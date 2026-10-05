"""错误类别归一化。

所有上游异常与内部异常都必须映射为 :class:`ErrorCategory` 中的有限类别，
禁止把原始异常文本直接透出给调用方（仅记录在服务端日志）。
"""
from __future__ import annotations

from enum import Enum

from app.upstream import UpstreamOutcome


class ErrorCategory(str, Enum):
    UPSTREAM_UNAVAILABLE = "upstream_unavailable"
    UPSTREAM_TIMEOUT = "upstream_timeout"
    UPSTREAM_REJECTED = "upstream_rejected"
    UPSTREAM_BAD_RESPONSE = "upstream_bad_response"
    RETRY_EXHAUSTED = "retry_exhausted"
    BUDGET_EXHAUSTED = "budget_exhausted"
    NON_IDEMPOTENT_RETRY_FORBIDDEN = "non_idempotent_retry_forbidden"
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"
    CONCURRENCY_LIMIT_EXCEEDED = "concurrency_limit_exceeded"
    CIRCUIT_OPEN = "circuit_open"
    INTERNAL = "internal"


# 对外可展示的稳定文案：不含内部细节。
PUBLIC_MESSAGES: dict[ErrorCategory, str] = {
    ErrorCategory.UPSTREAM_UNAVAILABLE: "upstream service is unavailable",
    ErrorCategory.UPSTREAM_TIMEOUT: "upstream request timed out",
    ErrorCategory.UPSTREAM_REJECTED: "upstream rejected the request",
    ErrorCategory.UPSTREAM_BAD_RESPONSE: "upstream returned an invalid response",
    ErrorCategory.RETRY_EXHAUSTED: "upstream failed after retries",
    ErrorCategory.BUDGET_EXHAUSTED: "request time budget exhausted, serving degraded result",
    ErrorCategory.NON_IDEMPOTENT_RETRY_FORBIDDEN: "retry is forbidden for non-idempotent request without idempotency key",
    ErrorCategory.IDEMPOTENCY_CONFLICT: "a request with the same idempotency key is already being processed",
    ErrorCategory.CONCURRENCY_LIMIT_EXCEEDED: "upstream concurrency limit exceeded, request rejected",
    ErrorCategory.CIRCUIT_OPEN: "upstream circuit breaker is open, serving degraded result",
    ErrorCategory.INTERNAL: "internal server error",
}


class GatewayError(Exception):
    def __init__(
        self,
        category: ErrorCategory,
        message: str,
        upstream: str | None = None,
    ) -> None:
        super().__init__(message)
        self.category = category
        self.message = message
        self.upstream = upstream

    def __str__(self) -> str:  # pragma: no cover - 简单格式化
        return f"{self.category.value}: {self.message}"


def normalize_outcome_failure(
    outcome: UpstreamOutcome,
) -> ErrorCategory:
    """把上游结论中的失败映射为错误类别（成功不应调用本函数）。"""
    if outcome is UpstreamOutcome.FATAL_FAILURE:
        return ErrorCategory.UPSTREAM_REJECTED
    return ErrorCategory.UPSTREAM_UNAVAILABLE


def public_message(category: ErrorCategory) -> str:
    return PUBLIC_MESSAGES[category]
