"""单次转发总时间预算：耗尽即降级、原因可区分、并发各算各的。"""
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


class FakeClock:
    """假时钟：睡眠直接推进时间，预算用尽在测试中完全确定。"""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def make_gateway(script, *, max_attempts=3, backoff_base=0.5):
    clock = FakeClock()
    client = StubUpstreamClient("stub")
    client.set_script(script)
    gw = Gateway(
        {"stub": client},
        breaker_config=CircuitBreakerConfig(failure_threshold=100),
        retry_config=RetryConfig(
            max_attempts=max_attempts,
            backoff_base_seconds=backoff_base,
            backoff_max_seconds=10.0,
        ),
        clock=clock,
        sleeper=clock.sleep,
        rng=lambda: 1.0,  # 抖动置 1，退避时长确定
    )
    return gw, client, clock


def retryable():
    return R(O.RETRYABLE_FAILURE, 503, None)


def ok():
    return R(O.SUCCESS, 200, {"v": 1})


def test_zero_budget_degrades_without_calling_upstream():
    print("[场景] 预算为 0：一次都不打上游，直接降级")
    gw, client, _ = make_gateway([ok()])
    result = gw.handle("stub", UpstreamRequest("GET", "/x"), budget_seconds=0.0)
    assert result.conclusion is Conclusion.DEGRADED
    assert result.reason == ErrorCategory.BUDGET_EXHAUSTED.value
    assert result.attempts == 0
    assert client.calls == 0
    print("[结果] budget=0 -> degraded/budget_exhausted, upstream calls = 0 ✅")


def test_budget_consumed_by_backoff_stops_before_next_attempt():
    print("[场景] 重试等待计入预算：等待耗尽预算后不再发起下一次尝试")
    gw, client, clock = make_gateway([retryable(), retryable(), ok()])
    # 预算 0.6s：第 1 次失败后等 0.5s，第 2 次失败后只剩 0.1s 可等，
    # 等待结束即到达预算边界，第 3 次尝试不再发生。
    result = gw.handle("stub", UpstreamRequest("GET", "/x"), budget_seconds=0.6)
    assert result.conclusion is Conclusion.DEGRADED
    assert result.reason == ErrorCategory.BUDGET_EXHAUSTED.value
    assert result.attempts == 2
    assert client.calls == 2
    assert clock.now == 0.6  # 等待被截断到预算边界，不超睡
    print("[结果] 预算 0.6s -> 2 次尝试后 budget_exhausted, 未超睡 ✅")


def test_budget_reason_distinct_from_retry_exhausted_and_circuit_open():
    print("[场景] 预算耗尽原因与重试耗尽、熔断打开可区分")
    gw, client, _ = make_gateway([retryable(), retryable(), ok()])
    result = gw.handle("stub", UpstreamRequest("GET", "/x"), budget_seconds=0.6)
    assert result.reason == ErrorCategory.BUDGET_EXHAUSTED.value
    assert result.reason != ErrorCategory.RETRY_EXHAUSTED.value
    assert result.reason != ErrorCategory.CIRCUIT_OPEN.value
    # 对照：不带预算时同样的脚本会重试到成功
    gw2, client2, _ = make_gateway([retryable(), retryable(), ok()])
    result2 = gw2.handle("stub", UpstreamRequest("GET", "/x"))
    assert result2.conclusion is Conclusion.SUCCESS
    assert client2.calls == 3
    print("[结果] budget_exhausted 与 retry_exhausted/circuit_open 明确区分 ✅")


def test_budget_exhaustion_releases_idempotency_key():
    print("[场景] 预算耗尽不占用幂等键，同键可安全重提")
    gw, client, _ = make_gateway([retryable(), retryable(), retryable(), ok()])
    first = gw.handle(
        "stub",
        UpstreamRequest("POST", "/orders"),
        idempotency_key="k-budget-1",
        budget_seconds=0.6,
    )
    assert first.conclusion is Conclusion.DEGRADED
    assert first.reason == ErrorCategory.BUDGET_EXHAUSTED.value
    # 键已释放：同键重提不会被 409 拒绝，且能继续打上游直至成功
    second = gw.handle(
        "stub",
        UpstreamRequest("POST", "/orders"),
        idempotency_key="k-budget-1",
        budget_seconds=10.0,
    )
    assert second.conclusion is Conclusion.SUCCESS
    print("[结果] 预算耗尽后同键重提成功，无 409 ✅")


def test_concurrent_requests_have_independent_budgets():
    print("[场景] 并发请求各算各的预算，互不影响")
    # 上游 a 持续可重试失败；上游 stub 第二次成功。
    client_a = StubUpstreamClient("a")
    client_a.set_script([R(O.RETRYABLE_FAILURE, 503, None)] * 10)
    client_b = StubUpstreamClient("b")
    client_b.set_script([R(O.RETRYABLE_FAILURE, 503, None), ok()])
    gw = Gateway(
        {"a": client_a, "b": client_b},
        breaker_config=CircuitBreakerConfig(failure_threshold=100),
        retry_config=RetryConfig(
            max_attempts=3, backoff_base_seconds=0.02, backoff_max_seconds=0.02
        ),
        rng=lambda: 1.0,
    )
    results: dict[str, object] = {}

    def worker_a():  # 预算极短且上游持续失败 -> 预算耗尽降级
        results["a"] = gw.handle(
            "a", UpstreamRequest("GET", "/a"), budget_seconds=0.01
        )

    def worker_b():  # 预算充足 -> 重试后成功
        results["b"] = gw.handle(
            "b", UpstreamRequest("GET", "/b"), budget_seconds=30.0
        )

    t_a = threading.Thread(target=worker_a)
    t_b = threading.Thread(target=worker_b)
    t_a.start()
    t_b.start()
    t_a.join()
    t_b.join()

    a, b = results["a"], results["b"]
    assert a.conclusion is Conclusion.DEGRADED
    assert a.reason == ErrorCategory.BUDGET_EXHAUSTED.value
    assert b.conclusion is Conclusion.SUCCESS
    print("[结果] 并发下 A 预算耗尽降级、B 正常成功，互不影响 ✅")
