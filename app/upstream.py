"""上游客户端抽象与测试用本地桩。

真实接入时可实现 :class:`UpstreamClient`（例如 httpx 调用外部服务）；
本仓库的可验证流程全部基于 :class:`StubUpstreamClient`：

* 按脚本顺序返回预设结果，脚本耗尽后返回默认结果（默认成功）；
* 线程安全地记录副作用调用次数（``calls``），用于断言"无重复副作用"；
* 支持注入异常以验证内部错误归一化。
"""
from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Callable


class UpstreamOutcome(str, Enum):
    SUCCESS = "success"
    RETRYABLE_FAILURE = "retryable_failure"
    FATAL_FAILURE = "fatal_failure"


@dataclass(frozen=True)
class UpstreamRequest:
    method: str
    path: str
    body: object | None = None


@dataclass(frozen=True)
class UpstreamResponse:
    outcome: UpstreamOutcome
    status_code: int = 200
    payload: object | None = None
    detail: str | None = None


class UpstreamClient:
    name = "base"

    def call(self, request: UpstreamRequest) -> UpstreamResponse:
        raise NotImplementedError


class StubUpstreamClient(UpstreamClient):
    """线程安全的脚本式本地桩上游。"""

    def __init__(
        self,
        name: str = "stub",
        default: UpstreamResponse | None = None,
        exception_factory: Callable[[], Exception] | None = None,
    ) -> None:
        self.name = name
        self._lock = threading.Lock()
        self.script: deque[UpstreamResponse] = deque()
        self.calls = 0
        self.requests: list[UpstreamRequest] = []
        self.default = default or UpstreamResponse(UpstreamOutcome.SUCCESS, 200, {"ok": True})
        self.exception_factory = exception_factory

    def set_script(self, responses: list[UpstreamResponse]) -> None:
        with self._lock:
            self.script = deque(responses)

    def call(self, request: UpstreamRequest) -> UpstreamResponse:
        with self._lock:
            self.calls += 1
            self.requests.append(request)
            if self.exception_factory is not None:
                raise self.exception_factory()
            if self.script:
                return self.script.popleft()
            return self.default
