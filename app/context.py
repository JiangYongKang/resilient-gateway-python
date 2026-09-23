"""请求关联标识与结构化日志。

每个请求在入口处生成 ``request_id``，通过 :class:`contextvars.ContextVar`
贯穿同一次处理链路中的所有日志行；并发请求之间互不干扰。
"""
from __future__ import annotations

import contextlib
import json
import logging
import uuid
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger("resilient_gateway")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)
logger.propagate = False

_request_id: ContextVar[str | None] = ContextVar("request_id", default=None)


@dataclass(frozen=True)
class RequestContext:
    request_id: str


def new_context() -> RequestContext:
    """生成一个新的请求上下文（关联标识为短 UUID）。"""
    return RequestContext(request_id=uuid.uuid4().hex[:12])


def current_request_id() -> str | None:
    return _request_id.get()


@contextlib.contextmanager
def bind_context(ctx: RequestContext):
    """在 ``with`` 作用域内绑定当前请求的关联标识。"""
    token = _request_id.set(ctx.request_id)
    try:
        yield ctx
    finally:
        _request_id.reset(token)


def log_event(event: str, **fields: Any) -> None:
    """打印一行结构化日志，自动附带当前关联标识。"""
    payload: dict[str, Any] = {"event": event, "request_id": _request_id.get()}
    payload.update(fields)
    logger.info(json.dumps(payload, ensure_ascii=False, default=str))
