"""按上游并发隔离：超限确定性拒绝、不计失败统计、不碰幂等键、在途计数一致。"""
import threading
import time

from app.bulkhead import BulkheadRegistry
from app.config import BulkheadConfig, CircuitBreakerConfig, RetryConfig
from app.errors import ErrorCategory
from app.gateway import Conclusion, Gateway
from app.upstream import (
    StubUpstreamClient,
    UpstreamOutcome as O,
    UpstreamRequest,
    UpstreamResponse as R,
)

from conftest import log_scenario


def make_blocking_gateway(*, limit, max_attempts=1):
    """首个上游调用会阻塞，直到测试放行；用于占住在途名额。"""
    entered = threading.Event()
    release = threading.Event()

    class BlockingClient(StubUpstreamClient):
        def call(self, request):
            entered.set()
            release.wait(timeout=5)
            return super().call(request)

    client = BlockingClient("stub")
    gw = Gateway(
        {"stub": client},
        breaker_config=CircuitBreakerConfig(failure_threshold=100),
        retry_config=RetryConfig(max_attempts=max_attempts, backoff_base_seconds=0.0),
        bulkhead_config=BulkheadConfig(default_max_in_flight=limit),
        sleeper=lambda _s: None,
        rng=lambda: 1.0,
    )
    return gw, client, entered, release


def test_excess_request_rejected_without_touching_upstream():
    gw, client, entered, release = make_blocking_gateway(limit=1)
    first_results: list = []
    holder = threading.Thread(
        target=lambda: first_results.append(
            gw.handle("stub", UpstreamRequest("GET", "/x"))
        )
    )
    holder.start()
    assert entered.wait(timeout=5)  # 第一个请求占住唯一在途名额

    rejected = gw.handle("stub", UpstreamRequest("GET", "/x"))
    assert rejected.conclusion is Conclusion.REJECTED
    assert rejected.reason == ErrorCategory.CONCURRENCY_LIMIT_EXCEEDED.value
    assert rejected.attempts == 0
    assert client.calls == 0  # 超限请求没有触达上游（首个调用仍阻塞在桩内）
    assert gw.bulkheads.in_flight("stub") == 1

    release.set()
    holder.join()
    assert first_results[0].conclusion is Conclusion.SUCCESS
    assert client.calls == 1  # 自始至终只有第一个请求触达上游
    assert gw.bulkheads.in_flight("stub") == 0  # 结束后在途计数归零
    log_scenario(
        "并发隔离-超限拒绝",
        f"reason={rejected.reason} upstream_calls={client.calls} in_flight=0",
    )


def test_rejected_requests_do_not_count_into_breaker_stats():
    gw, client, entered, release = make_blocking_gateway(limit=1)
    holder = threading.Thread(
        target=lambda: gw.handle("stub", UpstreamRequest("GET", "/x"))
    )
    holder.start()
    assert entered.wait(timeout=5)

    for _ in range(5):
        r = gw.handle("stub", UpstreamRequest("GET", "/x"))
        assert r.reason == ErrorCategory.CONCURRENCY_LIMIT_EXCEEDED.value

    breaker = gw.breakers.get("stub")
    assert breaker.failure_count_in_window() == 0  # 被挡请求不计入失败统计
    assert breaker.state.value == "closed"

    release.set()
    holder.join()
    log_scenario(
        "并发隔离-不计失败统计",
        f"window_failures={breaker.failure_count_in_window()} state=closed",
    )


def test_rejected_request_does_not_touch_in_progress_idempotency_key():
    gw, client, entered, release = make_blocking_gateway(limit=1)
    first_results: list = []
    holder = threading.Thread(
        target=lambda: first_results.append(
            gw.handle("stub", UpstreamRequest("POST", "/orders"), idempotency_key="K-iso")
        )
    )
    holder.start()
    assert entered.wait(timeout=5)  # 首个同键请求处理中且占住在途名额

    # 同键请求被并发隔离挡下：原因是并发超限，而非幂等冲突
    r = gw.handle("stub", UpstreamRequest("POST", "/orders"), idempotency_key="K-iso")
    assert r.conclusion is Conclusion.REJECTED
    assert r.reason == ErrorCategory.CONCURRENCY_LIMIT_EXCEEDED.value
    assert r.reason != ErrorCategory.IDEMPOTENCY_CONFLICT.value

    release.set()
    holder.join()
    assert first_results[0].conclusion is Conclusion.SUCCESS

    # 键未被干扰、正常完成：后续同键请求重放结果
    replay = gw.handle("stub", UpstreamRequest("POST", "/orders"), idempotency_key="K-iso")
    assert replay.conclusion is Conclusion.SUCCESS
    assert replay.reason == "idempotent_replay"
    assert client.calls == 1
    log_scenario(
        "并发隔离-不触碰处理中幂等键",
        f"rejected_reason={r.reason} replay={replay.reason} calls={client.calls}",
    )


def test_in_flight_never_exceeds_limit_and_returns_to_zero():
    limit = 3
    total = 20
    state = {"current": 0, "max_seen": 0}
    lock = threading.Lock()

    class SlowClient(StubUpstreamClient):
        def call(self, request):
            with lock:
                state["current"] += 1
                state["max_seen"] = max(state["max_seen"], state["current"])
            try:
                time.sleep(0.02)
                return super().call(request)
            finally:
                with lock:
                    state["current"] -= 1

    client = SlowClient("stub")
    gw = Gateway(
        {"stub": client},
        breaker_config=CircuitBreakerConfig(failure_threshold=100),
        retry_config=RetryConfig(max_attempts=1),
        bulkhead_config=BulkheadConfig(default_max_in_flight=limit),
        sleeper=lambda _s: None,
    )
    results: list = []
    barrier = threading.Barrier(total)

    def worker():
        barrier.wait()
        results.append(gw.handle("stub", UpstreamRequest("GET", "/x")))

    threads = [threading.Thread(target=worker) for _ in range(total)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    success = [r for r in results if r.conclusion is Conclusion.SUCCESS]
    rejected = [r for r in results if r.conclusion is Conclusion.REJECTED]
    assert len(success) + len(rejected) == total
    assert len(rejected) > 0
    assert all(
        r.reason == ErrorCategory.CONCURRENCY_LIMIT_EXCEEDED.value for r in rejected
    )
    assert 0 < state["max_seen"] <= limit  # 高并发下从不超额放行
    assert client.calls == len(success)
    assert gw.bulkheads.in_flight("stub") == 0  # 全部结束后计数归零，不漏减
    metrics = gw.metrics.snapshot()["stub"]
    assert metrics["rejected"] == len(rejected)
    log_scenario(
        "并发隔离-在途计数一致",
        f"total={total} success={len(success)} rejected={len(rejected)} "
        f"max_in_flight={state['max_seen']} final_in_flight=0",
    )


def test_per_upstream_limit_overrides_default():
    registry = BulkheadRegistry(
        BulkheadConfig(default_max_in_flight=1, per_upstream={"b": 3})
    )
    pa = registry.try_acquire("a")
    assert pa is not None
    assert registry.try_acquire("a") is None  # 默认上限 1

    permits_b = [registry.try_acquire("b") for _ in range(3)]
    assert all(p is not None for p in permits_b)  # 覆盖上限 3
    assert registry.try_acquire("b") is None

    for p in permits_b:
        p.release()
    pa.release()
    assert registry.in_flight("a") == 0
    assert registry.in_flight("b") == 0
    log_scenario("并发隔离-按上游覆盖上限", "a_limit=1 b_limit=3 均按配置生效")


def test_permit_release_is_idempotent():
    registry = BulkheadRegistry(BulkheadConfig(default_max_in_flight=1))
    permit = registry.try_acquire("a")
    assert permit is not None
    permit.release()
    permit.release()  # 重复释放安全
    assert registry.in_flight("a") == 0
    assert registry.try_acquire("a") is not None
    log_scenario("并发隔离-许可幂等释放", "in_flight=0 可再次获取")


def test_unlimited_by_default_keeps_existing_behavior():
    # 不配置上限时（默认 None）不限制，行为与既有版本一致。
    registry = BulkheadRegistry(BulkheadConfig())
    permits = [registry.try_acquire("a") for _ in range(100)]
    assert all(p is not None for p in permits)
    for p in permits:
        p.release()
    log_scenario("并发隔离-默认不限制", "100 个许可全部获取成功")
