"""领域模型：上游请求、响应、降级结果。"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class UpstreamRequest:
    upstream: str
    method: str = "POST"
    path: str = "/"
    payload: dict | None = None
    idempotency_key: str | None = None


@dataclass
class UpstreamResponse:
    status_code: int
    body: dict = field(default_factory=dict)


@dataclass
class GatewayResult:
    """对外统一结论。"""

    correlation_id: str
    upstream: str
    degraded: bool
    status: str
    data: dict = field(default_factory=dict)
    attempts: int = 0
    error_category: str | None = None
    reason: str | None = None
