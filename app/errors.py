"""错误分类与归一化。

对外只透出归一化后的错误类别（ErrorCategory），不直接抛出上游原始错误。
"""

from __future__ import annotations

import enum


class ErrorCategory(str, enum.Enum):
    # 上游不可用（连接失败/5xx），可重试
    UPSTREAM_UNAVAILABLE = "upstream_unavailable"
    # 上游超时，可重试
    UPSTREAM_TIMEOUT = "upstream_timeout"
    # 上游显式拒绝（4xx 中不可重试的一类，如 422/400）
    UPSTREAM_REJECTED = "upstream_rejected"
    # 上游返回了无法处理的响应
    UPSTREAM_BAD_RESPONSE = "upstream_bad_response"
    # 服务内部异常，兜底类别
    INTERNAL_ERROR = "internal_error"


# 可重试的 HTTP 状态码集合
RETRYABLE_STATUS = frozenset({408, 429, 500, 502, 503, 504})


def classify_status(status_code: int) -> "tuple[ErrorCategory, bool]":
    """把上游 HTTP 状态码归一化为 (错误类别, 是否可重试)。

    - 408/429 及 5xx -> 不可用类，可重试
    - 404 -> 不可用（服务缺失），可重试
    - 其余 4xx -> 显式拒绝，不可重试
    """
    if status_code in RETRYABLE_STATUS or status_code == 404:
        if status_code == 408:
            return ErrorCategory.UPSTREAM_TIMEOUT, True
        return ErrorCategory.UPSTREAM_UNAVAILABLE, True
    if 400 <= status_code < 500:
        return ErrorCategory.UPSTREAM_REJECTED, False
    return ErrorCategory.UPSTREAM_BAD_RESPONSE, False


class GatewayError(Exception):
    """服务内部异常基类（细节不对外透出）。"""

    category: ErrorCategory = ErrorCategory.INTERNAL_ERROR

    def __init__(self, message: str = "", *, category: ErrorCategory | None = None) -> None:
        super().__init__(message)
        if category is not None:
            self.category = category


class UpstreamCallError(GatewayError):
    """上游调用失败，携带类别与可重试判定。"""

    def __init__(
        self,
        message: str,
        *,
        category: ErrorCategory,
        retryable: bool,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message, category=category)
        self.retryable = retryable
        self.status_code = status_code
