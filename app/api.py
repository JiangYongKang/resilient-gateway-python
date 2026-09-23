"""HTTP 装配：保留既有 ``GET /`` 行为，新增韧性转发端点。

新增路由（不改变任何既有对外行为）：

* ``POST /upstreams/{upstream}/call`` —— 经熔断/幂等/重试治理后的转发调用；
* ``GET  /admin/circuits``            —— 查看各上游熔断状态（便于本地验证）。

结论到 HTTP 状态码的映射保持确定：

* ``SUCCESS``   -> 200
* ``DEGRADED``  -> 503（携带确定的降级 JSON，而非异常文本）
* ``REJECTED``  -> 409（同键处理中）或 422（上游致命拒绝 / 非幂等禁止重试）

所有响应都带 ``X-Request-ID`` 响应头，与服务端日志中的关联标识一致。
"""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.context import current_request_id, log_event
from app.errors import ErrorCategory, GatewayError, public_message
from app.gateway import Conclusion, Gateway
from app.upstream import StubUpstreamClient, UpstreamRequest

# 模块级单例：uvicorn 与测试复用同一份熔断/幂等状态。
stub = StubUpstreamClient("stub")
gateway = Gateway({"stub": stub})

_REJECT_STATUS = {
    ErrorCategory.IDEMPOTENCY_CONFLICT.value: 409,
    ErrorCategory.NON_IDEMPOTENT_RETRY_FORBIDDEN.value: 422,
    ErrorCategory.UPSTREAM_REJECTED.value: 502,
}


class CallBody(BaseModel):
    method: str = "GET"
    path: str = "/"
    body: Any | None = None


def create_app() -> FastAPI:
    app = FastAPI(title="resilient-gateway-python")

    @app.get("/")
    def read_root() -> dict[str, str]:
        # 既有对外行为，保持原样。
        return {"Hello": "World"}

    def _error_response(
        category: ErrorCategory, status_code: int, request_id: str | None
    ) -> JSONResponse:
        return JSONResponse(
            {
                "conclusion": "rejected",
                "upstream": None,
                "attempts": 0,
                "request_id": request_id,
                "error": {"category": category.value, "message": public_message(category)},
            },
            status_code=status_code,
            headers={"X-Request-ID": request_id or ""},
        )

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(_request: Request, exc: GatewayError) -> JSONResponse:
        # 归一化的网关错误：按类别映射状态码，不透出内部细节。
        log_event("gateway_error", category=exc.category.value, upstream=exc.upstream)
        status = {
            ErrorCategory.UPSTREAM_UNAVAILABLE: 503,
            ErrorCategory.UPSTREAM_TIMEOUT: 503,
            ErrorCategory.CIRCUIT_OPEN: 503,
            ErrorCategory.RETRY_EXHAUSTED: 503,
        }.get(exc.category, 422)
        return _error_response(exc.category, status, current_request_id())

    @app.exception_handler(Exception)
    async def unhandled_error_handler(_request: Request, exc: Exception) -> JSONResponse:
        # 兜底：任何意外内部异常只返回归一化 internal，绝不透出原始信息。
        log_event("unhandled_internal_error", error_type=type(exc).__name__)
        return _error_response(ErrorCategory.INTERNAL, 500, current_request_id())

    @app.post("/upstreams/{upstream}/call")
    async def call_upstream(upstream: str, call_body: CallBody, request: Request):
        idempotency_key = request.headers.get("Idempotency-Key")
        result = gateway.handle(
            upstream,
            UpstreamRequest(call_body.method.upper(), call_body.path, call_body.body),
            idempotency_key=idempotency_key,
        )
        body: dict[str, Any] = {
            "conclusion": result.conclusion.value,
            "upstream": result.upstream,
            "attempts": result.attempts,
            "request_id": result.request_id,
            "payload": result.payload,
        }
        headers = {"X-Request-ID": result.request_id or ""}

        if result.conclusion is Conclusion.SUCCESS:
            status_code = 200
        elif result.conclusion is Conclusion.DEGRADED:
            status_code = 503
            body["error"] = {
                "category": result.reason,
                "message": public_message(
                    ErrorCategory(result.reason)
                    if result.reason in ErrorCategory._value2member_map_
                    else ErrorCategory.INTERNAL
                ),
            }
        else:  # REJECTED
            status_code = _REJECT_STATUS.get(result.reason or "", 422)
            body["error"] = {
                "category": result.reason,
                "message": public_message(
                    ErrorCategory(result.reason)
                    if result.reason in ErrorCategory._value2member_map_
                    else ErrorCategory.INTERNAL
                ),
            }
        return JSONResponse(body, status_code=status_code, headers=headers)

    @app.get("/admin/circuits")
    def circuit_states() -> dict[str, str]:
        return {name: state.value for name, state in gateway.breakers.all_states().items()}

    return app
