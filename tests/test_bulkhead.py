"""按上游并发隔离：超限拒绝、不计失败统计、不动幂等键、在途计数准确。"""
import threading
import time

from app.config import BulkheadConfig, CircuitBreakerConfig, RetryConfig
from app.errors import ErrorCategory
from app.gateway import Conclusion, Gateway
from app.upstream import (
    StubUpstreamClient,
    UpstreamOutcome as O,
    UpstreamRequest,
    UpstreamResponse as R,
)


class BlockingClient(StubUpstreamClient):
    """进入后阻塞、并记录峰值在途数的桩上游。"""

    def __init__(self, name="stub"):
        super().__init__(name)
        self.entered = threading.Event()
        self.release = threading.Event()
        self.active = 0
        self.max_active = 0

    def call(self, request):
        with self._lock:
            self.calls += 1
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            if self.active >= 1:
                self.entered.set()
        self.release.wait(timeout=5.0)
        with self._lock:
            self.active -= 1
        return R(O.SUCCESS, 200, {"ok": True})


def make_gateway(client, *, default_limit=3, per_upstream=None):
    return Gateway(
        {client.name: client},
        breaker_config=CircuitBreakerConfig(failure_threshold=100),
        retry_config=RetryConfig(max_attempts=1),
        bulkhead_config=BulkheadConfig(
            default_max_concurrent=default_limit, per_upstream=per_upstream
        ),
        clock=lambda: 0.0,
        sleeper=lambda _s: None,
        rng=lambda: 1.0,
    )


def run_concurrent(gw, upstream, n, key_prefix=None):
    results: list[object] = []

    def worker(i):
        key = f"{key_prefix}-{i}" if key_prefix else None
        results.append(
            gw.handle(upstream, UpstreamRequest("GET", "/x"), idempotency_key=key)
        )

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    return threads, results


def test_over_limit_rejected_without_touching_upstream():
    print("[场景] 在途达到上限：超出请求直接拒绝，不进入上游")
    client = BlockingClient()
    gw = make_gateway(client, default_limit=3)
    threads, results = run_concurrent(gw, "stub", 10)

    assert client.entered.wait(timeout=2.0)
    deadline = time.monotonic() + 2.0
    while client.active < 3 and time.monotonic() < deadline:
        time.sleep(0.005)
    time.sleep(0.05)  # 让其余线程完成拒绝判定
    client.release.set()
    for t in threads:
        t.join()

    admitted = [r for r in results if r.conclusion is Conclusion.SUCCESS]
    rejected = [r for r in results if r.conclusion is Conclusion.REJECTED]
    assert len(admitted) == 3
    assert len(rejected) == 7
    assert all(
        r.reason == ErrorCategory.CONCURRENCY_LIMIT_EXCEEDED.value for r in rejected
    )
    assert all(r.attempts == 0 for r in rejected)
    assert client.calls == 3  # 被拒绝的请求从未触达上游
    print("[结果] 10 并发限 3：3 成功 / 7 拒绝(concurrency_limit_exceeded)，上游仅 3 次调用 ✅")


def test_in_flight_count_accurate_under_concurrency():
    print("[场景] 高并发下在途计数：不超上限、结束后归零")
    client = BlockingClient()
    gw = make_gateway(client, default_limit=3)
    threads, _ = run_concurrent(gw, "stub", 10)
    assert client.entered.wait(timeout=2.0)
    time.sleep(0.05)
    client.release.set()
    for t in threads:
        t.join()
    assert client.max_active == 3  # 从未超额放行
    assert client.active == 0  # 已结束的请求全部减回
    assert gw.bulkheads.in_flight("stub") == 0
    print("[结果] 峰值在途=3（=上限），结束后在途=0 ✅")


def test_rejected_does_not_affect_breaker_or_idempotency():
    print("[场景] 被拒请求不计熔断失败统计、不动幂等键")
    client = BlockingClient()
    gw = make_gateway(client, default_limit=1)
    threads, results = run_concurrent(gw, "stub", 4, key_prefix="k-bh")
    assert client.entered.wait(timeout=2.0)
    time.sleep(0.05)
    client.release.set()
    for t in threads:
        t.join()

    breaker = gw.breakers.get("stub")
    assert breaker.failure_count() == 0  # 拒绝不计入失败窗口
    assert breaker.state.value == "closed"

    # 被拒请求携带的幂等键未被占用：同键可立即正常使用
    rejected_keys = [
        f"k-bh-{i}"
        for i, r in enumerate(results)
        if r.conclusion is Conclusion.REJECTED
    ]
    assert rejected_keys
    client2 = StubUpstreamClient("stub")
    gw.clients["stub"] = client2
    retry = gw.handle(
        "stub", UpstreamRequest("GET", "/x"), idempotency_key=rejected_keys[0]
    )
    assert retry.conclusion is Conclusion.SUCCESS  # 不是 409 冲突
    print("[结果] 失败窗口=0、熔断 closed、被拒键可立即重用 ✅")


def test_per_upstream_limit_override():
    print("[场景] 按上游覆盖并发上限")
    client_a = BlockingClient("a")
    client_b = BlockingClient("b")
    gw = Gateway(
        {"a": client_a, "b": client_b},
        retry_config=RetryConfig(max_attempts=1),
        bulkhead_config=BulkheadConfig(
            default_max_concurrent=1, per_upstream={"b": 5}
        ),
        clock=lambda: 0.0,
        sleeper=lambda _s: None,
        rng=lambda: 1.0,
    )
    assert gw.bulkheads.get("a").max_concurrent == 1
    assert gw.bulkheads.get("b").max_concurrent == 5
    print("[结果] 默认 1、上游 b 覆盖为 5 ✅")
