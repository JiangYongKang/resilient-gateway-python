"""网关：安全重试、降级与错误归一化（确定性，退避不真正睡眠）。"""
from app.config import CircuitBreakerConfig, RetryConfig
from app.errors import ErrorCategory
from app.gateway import Conclusion, Gateway
from app.upstream import (
    StubUpstreamClient,
    UpstreamOutcome as O,
    UpstreamRequest,
    UpstreamResponse as R,
)


def make_gateway(script=None, *, threshold=5, max_attempts=3, exc=None):
    client = StubUpstreamClient(
        "stub", exception_factory=(lambda: exc) if exc else None
    )
    if script is not None:
        client.set_script(script)
    gw = Gateway(
        {"stub": client},
        breaker_config=CircuitBreakerConfig(
            failure_threshold=threshold,
            open_state_seconds=10.0,
            failure_window_seconds=60.0,
        ),
        retry_config=RetryConfig(max_attempts=max_attempts, backoff_base_seconds=0.01),
        clock=lambda: 0.0,
        sleeper=lambda _s: None,
        rng=lambda: 1.0,
    )
    return gw, client


def retryable(detail=None):
    return R(O.RETRYABLE_FAILURE, 503, None, detail)


def fatal():
    return R(O.FATAL_FAILURE, 400, None, "bad request")


def ok():
    return R(O.SUCCESS, 200, {"v": 1})


def test_success_after_retries_counts_attempts():
    gw, client = make_gateway([retryable(), retryable(), ok()])
    result = gw.handle("stub", UpstreamRequest("GET", "/x"))
    assert result.conclusion is Conclusion.SUCCESS
    assert result.attempts == 3
    assert client.calls == 3
    assert result.payload == {"v": 1}
    assert result.request_id


def test_retry_exhausted_returns_degraded():
    gw, client = make_gateway([retryable(), retryable(), retryable()], max_attempts=3)
    result = gw.handle("stub", UpstreamRequest("GET", "/x"))
    assert result.conclusion is Conclusion.DEGRADED
    assert result.reason == ErrorCategory.RETRY_EXHAUSTED.value
    assert result.attempts == 3
    assert result.payload == {"degraded": True, "upstream": "stub", "data": None}
    assert client.calls == 3


def test_non_idempotent_post_without_key_not_retried():
    gw, client = make_gateway([retryable()], max_attempts=3)
    result = gw.handle("stub", UpstreamRequest("POST", "/orders", {"q": 1}))
    assert result.conclusion is Conclusion.REJECTED
    assert result.reason == ErrorCategory.NON_IDEMPOTENT_RETRY_FORBIDDEN.value
    assert result.attempts == 1
    assert client.calls == 1  # 关键：无重试、无重复副作用


def test_non_idempotent_post_with_key_is_retried_once():
    gw, client = make_gateway([retryable(), ok()], max_attempts=3)
    result = gw.handle(
        "stub", UpstreamRequest("POST", "/orders"), idempotency_key="k-post-1"
    )
    assert result.conclusion is Conclusion.SUCCESS
    assert result.attempts == 2
    assert client.calls == 2


def test_fatal_failure_is_not_retried_and_rejected():
    gw, client = make_gateway([fatal()], max_attempts=3)
    result = gw.handle("stub", UpstreamRequest("GET", "/x"))
    assert result.conclusion is Conclusion.REJECTED
    assert result.reason == ErrorCategory.UPSTREAM_REJECTED.value
    assert result.attempts == 1
    assert client.calls == 1


def test_upstream_exception_normalized_and_degraded():
    gw, client = make_gateway(exc=RuntimeError("db exploded with secret token"))
    result = gw.handle("stub", UpstreamRequest("GET", "/x"))
    assert result.conclusion is Conclusion.DEGRADED
    assert result.reason == ErrorCategory.RETRY_EXHAUSTED.value
    # 原始异常细节绝不出现在结论中
    assert "secret token" not in str(result)
    assert client.calls == 3  # 异常视为可重试失败


def test_circuit_open_short_circuits_to_degraded_without_calls():
    gw, client = make_gateway(
        [retryable(), retryable(), retryable()], threshold=3, max_attempts=3
    )
    first = gw.handle("stub", UpstreamRequest("GET", "/x"))
    assert first.conclusion is Conclusion.DEGRADED
    assert first.reason == ErrorCategory.CIRCUIT_OPEN.value
    calls_after_first = client.calls
    second = gw.handle("stub", UpstreamRequest("GET", "/x"))
    assert second.conclusion is Conclusion.DEGRADED
    assert second.reason == ErrorCategory.CIRCUIT_OPEN.value
    assert second.attempts == 0
    assert client.calls == calls_after_first  # 熔断打开期间不触达上游
