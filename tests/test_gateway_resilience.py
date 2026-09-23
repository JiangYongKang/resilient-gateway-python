"""网关集成：熔断时间窗口恢复、半开试探、同键并发与重复提交。"""
import threading

from app.config import CircuitBreakerConfig, RetryConfig
from app.errors import ErrorCategory
from app.gateway import Conclusion, Gateway
from app.upstream import (
    StubUpstreamClient,
    UpstreamOutcome as O,
    UpstreamRequest,
    UpstreamResponse as R,
)


def make_gateway(script=None, *, threshold=3, open_seconds=10.0, half_open=1,
                 max_attempts=1, window=60.0):
    time_holder = {"t": 0.0}
    client = StubUpstreamClient("stub")
    if script is not None:
        client.set_script(script)
    gw = Gateway(
        {"stub": client},
        breaker_config=CircuitBreakerConfig(
            failure_threshold=threshold,
            open_state_seconds=open_seconds,
            half_open_max_calls=half_open,
            failure_window_seconds=window,
        ),
        retry_config=RetryConfig(max_attempts=max_attempts, backoff_base_seconds=0.0),
        clock=lambda: time_holder["t"],
        sleeper=lambda _s: None,
        rng=lambda: 1.0,
    )
    return gw, client, time_holder


def fail():
    return R(O.RETRYABLE_FAILURE, 503)


def ok():
    return R(O.SUCCESS, 200, {"ok": True})


def test_circuit_recovers_after_open_window_via_half_open():
    # 阈值 3、每请求只尝试 1 次：前 3 个请求各贡献 1 次失败后熔断。
    gw, client, t = make_gateway(
        [fail(), fail(), fail(), ok(), ok()], threshold=3, max_attempts=1
    )
    req = UpstreamRequest("GET", "/x")

    for _ in range(2):
        early = gw.handle("stub", req)
        assert early.conclusion is Conclusion.DEGRADED
        assert early.reason == ErrorCategory.RETRY_EXHAUSTED.value
    r1 = gw.handle("stub", req)  # 第 3 次失败触发熔断
    assert r1.reason == ErrorCategory.CIRCUIT_OPEN.value
    assert client.calls == 3

    t["t"] += 9.0
    r2 = gw.handle("stub", req)
    assert r2.conclusion is Conclusion.DEGRADED
    assert r2.reason == ErrorCategory.CIRCUIT_OPEN.value
    assert client.calls == 3  # 未到恢复时间，不触达

    t["t"] += 1.1
    r3 = gw.handle("stub", req)  # 半开试探且成功
    assert r3.conclusion is Conclusion.SUCCESS
    assert r3.payload == {"ok": True}
    assert client.calls == 4

    r4 = gw.handle("stub", req)  # 已恢复关闭，后续正常
    assert r4.conclusion is Conclusion.SUCCESS
    assert client.calls == 5


def test_half_open_failure_reopens_and_waits_again():
    gw, client, t = make_gateway(
        [fail(), fail(), fail(), fail(), ok()], threshold=3, max_attempts=1
    )
    req = UpstreamRequest("GET", "/x")
    for _ in range(3):
        gw.handle("stub", req)
    assert client.calls == 3
    t["t"] += 10.1
    probe = gw.handle("stub", req)  # 半开试探失败 -> 重新打开
    assert probe.reason == ErrorCategory.CIRCUIT_OPEN.value
    assert client.calls == 4

    t["t"] += 9.0
    assert gw.handle("stub", req).reason == ErrorCategory.CIRCUIT_OPEN.value
    assert client.calls == 4
    t["t"] += 1.1
    recovered = gw.handle("stub", req)
    assert recovered.conclusion is Conclusion.SUCCESS
    assert client.calls == 5


def test_concurrent_same_key_single_side_effect():
    # 自定义客户端：首个调用阻塞，直到所有竞争者都完成去重判定后再放行。
    release = threading.Event()

    class BlockingClient(StubUpstreamClient):
        def call(self, request):
            release.wait(timeout=5)
            return super().call(request)

    # 包装幂等存储：第 16 次 acquire（无论 acquired/in_progress/replay）
    # 完成后才允许首个上游调用返回，保证同步窗口覆盖全部竞争者。
    from app.idempotency import IdempotencyStore

    all_acquired = threading.Event()
    acquire_count = {"n": 0}
    counter_lock = threading.Lock()

    class CountingStore(IdempotencyStore):
        def acquire(self, key):
            result = super().acquire(key)
            with counter_lock:
                acquire_count["n"] += 1
                if acquire_count["n"] == 16:
                    all_acquired.set()
            return result

    client = BlockingClient("stub")
    gw = Gateway(
        {"stub": client},
        breaker_config=CircuitBreakerConfig(),
        retry_config=RetryConfig(max_attempts=1),
        idempotency_store=CountingStore(),
        sleeper=lambda _s: None,
    )
    results: list = []
    barrier = threading.Barrier(16)

    def worker():
        barrier.wait()
        results.append(
            gw.handle("stub", UpstreamRequest("POST", "/orders"), idempotency_key="K-123")
        )

    threads = [threading.Thread(target=worker) for _ in range(16)]
    for th in threads:
        th.start()
    assert all_acquired.wait(timeout=5)
    release.set()  # 此时 1 个 acquired 正阻塞在桩内，15 个 in_progress 已拒绝
    for th in threads:
        th.join()

    success = [r for r in results if r.conclusion is Conclusion.SUCCESS]
    rejected = [r for r in results if r.conclusion is Conclusion.REJECTED]
    assert len(success) == 1
    assert len(rejected) == 15
    assert all(r.reason == ErrorCategory.IDEMPOTENCY_CONFLICT.value for r in rejected)
    assert client.calls == 1  # 只产生一次上游副作用


def test_duplicate_submission_after_completion_replays_without_side_effect():
    gw, client, _ = make_gateway(max_attempts=1)
    req = UpstreamRequest("POST", "/orders")
    first = gw.handle("stub", req, idempotency_key="K-dup")
    assert first.conclusion is Conclusion.SUCCESS
    assert client.calls == 1

    second = gw.handle("stub", req, idempotency_key="K-dup")
    assert second.conclusion is Conclusion.SUCCESS
    assert second.reason == "idempotent_replay"
    assert second.payload == first.payload
    assert second.request_id != first.request_id  # 关联标识各自独立
    assert client.calls == 1  # 重放不再触达上游


def test_failed_first_attempt_releases_key_for_resubmission():
    gw, client, _ = make_gateway(
        [fail(), ok()], threshold=100, max_attempts=1
    )
    req = UpstreamRequest("POST", "/orders")
    first = gw.handle("stub", req, idempotency_key="K-retry")
    # 非幂等但带键：单次尝试失败 -> 降级（不重试，预算=1），键应释放
    assert first.conclusion is Conclusion.DEGRADED
    second = gw.handle("stub", req, idempotency_key="K-retry")
    assert second.conclusion is Conclusion.SUCCESS
    assert client.calls == 2
