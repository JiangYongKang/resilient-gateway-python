"""重试决策测试：可重试性、非幂等需幂等键、原因可区分。"""

from __future__ import annotations

from app.config import ResilienceConfig
from app.retry import (
    RetryDecision,
    RetryRefuseReason,
    compute_backoff,
    decide_retry,
    is_idempotent_method,
)


def test_idempotent_methods():
    assert is_idempotent_method("GET")
    assert is_idempotent_method("put")
    assert not is_idempotent_method("POST")
    assert not is_idempotent_method("PATCH")


def test_non_idempotent_without_key_is_refused():
    v = decide_retry(
        attempt=1, max_attempts=3, failure_retryable=True,
        method="POST", idempotency_key=None,
    )
    assert v.decision is RetryDecision.GIVE_UP
    assert v.reason is RetryRefuseReason.NON_IDEMPOTENT_WITHOUT_KEY
    print("[retry] POST 无幂等键 ->", v.reason.value)


def test_non_idempotent_with_key_can_retry():
    v = decide_retry(
        attempt=1, max_attempts=3, failure_retryable=True,
        method="POST", idempotency_key="key-1",
    )
    assert v.decision is RetryDecision.RETRY
    print("[retry] POST 携带幂等键 -> RETRY")


def test_non_retryable_failure_distinct_reason():
    v = decide_retry(
        attempt=1, max_attempts=3, failure_retryable=False,
        method="GET", idempotency_key=None,
    )
    assert v.decision is RetryDecision.GIVE_UP
    assert v.reason is RetryRefuseReason.NON_RETRYABLE_FAILURE
    print("[retry] 不可重试失败 ->", v.reason.value)


def test_max_attempts_distinct_reason():
    v = decide_retry(
        attempt=3, max_attempts=3, failure_retryable=True,
        method="GET", idempotency_key=None,
    )
    assert v.reason is RetryRefuseReason.MAX_ATTEMPTS_REACHED
    print("[retry] 达到上限 ->", v.reason.value)


def test_backoff_is_bounded():
    cfg = ResilienceConfig(backoff_base_seconds=0.01, max_backoff_seconds=0.1)
    for attempt in range(1, 8):
        b = compute_backoff(attempt, cfg)
        assert 0 <= b <= 0.1
