"""重试决策。

规则：
1. 仅对可重试失败（failure_retryable=True）重试；
2. 非幂等方法（POST/PATCH 等）只有携带客户端幂等键时才允许重试，
   否则直接拒绝重试，并给出可区分原因；
3. 达到最大尝试次数后放弃。
"""

from __future__ import annotations

import enum
import random
from dataclasses import dataclass

from app.config import ResilienceConfig


class RetryDecision(str, enum.Enum):
    RETRY = "RETRY"
    GIVE_UP = "GIVE_UP"


class RetryRefuseReason(str, enum.Enum):
    NON_IDEMPOTENT_WITHOUT_KEY = "non_idempotent_without_idempotency_key"
    NON_RETRYABLE_FAILURE = "non_retryable_failure"
    MAX_ATTEMPTS_REACHED = "max_attempts_reached"
    CIRCUIT_OPEN = "circuit_open"


@dataclass
class RetryVerdict:
    decision: RetryDecision
    reason: RetryRefuseReason | None = None
    backoff_seconds: float = 0.0


IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "PUT", "DELETE", "OPTIONS"})


def is_idempotent_method(method: str) -> bool:
    return method.upper() in IDEMPOTENT_METHODS


def compute_backoff(attempt: int, config: ResilienceConfig) -> float:
    """指数退避：base * 2**(attempt-1)，截断到 max，并加少量抖动。

    attempt 表示“刚完成的第几次尝试”（从 1 开始）。
    """
    raw = config.backoff_base_seconds * (2 ** max(0, attempt - 1))
    capped = min(raw, config.max_backoff_seconds)
    jitter = random.uniform(0, capped * 0.1) if capped > 0 else 0.0
    return min(capped + jitter, config.max_backoff_seconds)


def decide_retry(
    *,
    attempt: int,
    max_attempts: int,
    failure_retryable: bool,
    method: str,
    idempotency_key: str | None,
) -> RetryVerdict:
    """决定第 attempt 次失败后是否重试。

    原因区分顺序（对外可解释）：
    1. 已达上限 -> MAX_ATTEMPTS_REACHED
    2. 失败本身不可重试 -> NON_RETRYABLE_FAILURE
    3. 非幂等且无幂等键 -> NON_IDEMPOTENT_WITHOUT_KEY
    """
    if attempt >= max_attempts:
        return RetryVerdict(RetryDecision.GIVE_UP, RetryRefuseReason.MAX_ATTEMPTS_REACHED)
    if not failure_retryable:
        return RetryVerdict(RetryDecision.GIVE_UP, RetryRefuseReason.NON_RETRYABLE_FAILURE)
    if not is_idempotent_method(method) and not idempotency_key:
        return RetryVerdict(
            RetryDecision.GIVE_UP, RetryRefuseReason.NON_IDEMPOTENT_WITHOUT_KEY
        )
    return RetryVerdict(RetryDecision.RETRY)
