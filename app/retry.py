"""安全重试策略。

* 仅对 ``RETRYABLE_FAILURE`` 重试；``FATAL_FAILURE`` 与成功不重试。
* 非幂等方法（POST/PATCH）默认禁止重试；只有客户端显式携带幂等键时才允许。
* 退避为指数退避 + 抖动（抖动可注入随机源，测试中置零以求确定）。
"""
from __future__ import annotations

import random
import time
from collections.abc import Callable

from app.config import RetryConfig
from app.upstream import UpstreamOutcome

IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "PUT", "DELETE", "OPTIONS", "TRACE"})


def is_idempotent_method(method: str) -> bool:
    return method.upper() in IDEMPOTENT_METHODS


def is_retryable_outcome(outcome: UpstreamOutcome) -> bool:
    return outcome is UpstreamOutcome.RETRYABLE_FAILURE


def retry_allowed(method: str, idempotency_key: str | None) -> bool:
    """非幂等请求只有携带客户端幂等键时才允许重试。"""
    if is_idempotent_method(method):
        return True
    return idempotency_key is not None


def compute_backoff_seconds(
    attempt: int,
    config: RetryConfig,
    rng: Callable[[], float] = random.random,
) -> float:
    """第 ``attempt`` 次失败后（attempt 从 1 起）的等待秒数。"""
    base = config.backoff_base_seconds * (2 ** (attempt - 1))
    cap = min(config.backoff_max_seconds, base)
    return cap * rng()


def sleep(seconds: float) -> None:  # pragma: no cover - 测试注入替换
    time.sleep(seconds)
