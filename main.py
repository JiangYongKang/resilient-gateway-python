"""HTTP 服务入口。

保留原有 GET / 行为（{"Hello": "World"}），新增 POST /gateway/invoke 转发接口。
"""

from __future__ import annotations

from fastapi import FastAPI
from pydantic import BaseModel

from app.config import ResilienceConfig
from app.errors import ErrorCategory
from app.gateway import Gateway
from app.models import UpstreamRequest
from app.upstream import StubUpstreamClient

app = FastAPI(title="resilient-gateway-python")


@app.get("/")
def read_root():
    return {"Hello": "World"}


class InvokePayload(BaseModel):
    upstream: str
    method: str = "POST"
    path: str = "/"
    payload: dict | None = None
    idempotency_key: str | None = None


# 默认内置两个本地桩上游，便于直接起服务手工验证：
# upstream-a 恒成功，upstream-flaky 恒失败（可观察熔断/降级）。
from functools import lru_cache


@lru_cache(maxsize=1)
def _get_gateway() -> Gateway:
    return Gateway(
        {
            "upstream-a": StubUpstreamClient("upstream-a", behavior="success"),
            "upstream-flaky": StubUpstreamClient("upstream-flaky", behavior="error"),
        },
        ResilienceConfig(),
    )


@app.post("/gateway/invoke")
def invoke_gateway(body: InvokePayload):
    result = _get_gateway().invoke(
        UpstreamRequest(
            upstream=body.upstream,
            method=body.method.upper(),
            path=body.path,
            payload=body.payload,
            idempotency_key=body.idempotency_key,
        )
    )
    http_status = 200 if (not result.degraded and not result.error_category) else 503 if result.degraded else 502
    from fastapi.responses import JSONResponse

    return JSONResponse(status_code=http_status, content=result.__dict__)


@app.get("/gateway/circuits")
def circuit_snapshots():
    return _get_gateway().breakers.all_snapshots()
