"""上游客户端协议与本地桩实现。"""

from __future__ import annotations

import threading
from typing import Protocol

from app.errors import ErrorCategory, UpstreamCallError, classify_status
from app.models import UpstreamRequest, UpstreamResponse


class UpstreamClient(Protocol):
    def call(self, request: UpstreamRequest) -> UpstreamResponse:
        ...

    @property
    def call_count(self) -> int:
        ...


# 行为关键字 -> 抛出的归一化上游异常
_BEHAVIOR_ERRORS = {
    "error": (ErrorCategory.UPSTREAM_UNAVAILABLE, True, 503),
    "timeout": (ErrorCategory.UPSTREAM_TIMEOUT, True, 504),
    "reject": (ErrorCategory.UPSTREAM_REJECTED, False, 422),
}


class StubUpstreamClient:
    """测试用本地桩上游。

    behavior:
      - "success"：恒返回 200；
      - "error"/"timeout"/"reject"：恒抛对应归一化异常；
      - "flaky"：按 behaviors 序列依次执行（每个元素可以是状态码 int、
        行为关键字 str，或 ("delay", seconds)）；序列耗尽后恒成功。

    同时记录每次真正产生“副作用”的调用（side_effects），
    并可注入延迟以模拟在途并发。
    """

    def __init__(
        self,
        name: str,
        *,
        behavior: str = "success",
        behaviors: list | None = None,
        record_side_effects: bool = True,
        sleep=None,
    ) -> None:
        self.name = name
        self._behavior = behavior
        self._behaviors = list(behaviors) if behaviors is not None else None
        self._record_side_effects = record_side_effects
        self._sleep = sleep
        self._lock = threading.Lock()
        self._call_count = 0
        self._idx = 0
        self.side_effects: list[dict] = []

    @property
    def call_count(self) -> int:
        with self._lock:
            return self._call_count

    # ---- 内部 ------------------------------------------------------------

    def _next_step(self):
        if self._behaviors is not None:
            if self._idx < len(self._behaviors):
                step = self._behaviors[self._idx]
                self._idx += 1
                return step
            return 200
        return self._behavior

    def _raise_for_status(self, status_code: int) -> None:
        category, retryable = classify_status(status_code)
        raise UpstreamCallError(
            f"stub upstream {self.name} returned {status_code}",
            category=category,
            retryable=retryable,
            status_code=status_code,
        )

    # ---- 协议实现 --------------------------------------------------------

    def call(self, request: UpstreamRequest) -> UpstreamResponse:
        with self._lock:
            self._call_count += 1
            step = self._next_step()

        if isinstance(step, tuple) and step[0] == "delay":
            if self._sleep is not None:
                self._sleep(step[1])
            step = 200
        if isinstance(step, str) and step.startswith("delay:"):
            if self._sleep is not None:
                self._sleep(float(step.split(":", 1)[1]))
            step = 200

        if isinstance(step, str):
            if step == "success":
                status = 200
            elif step in _BEHAVIOR_ERRORS:
                category, retryable, code = _BEHAVIOR_ERRORS[step]
                raise UpstreamCallError(
                    f"stub upstream {self.name} behavior={step}",
                    category=category,
                    retryable=retryable,
                    status_code=code,
                )
            else:
                raise UpstreamCallError(
                    f"stub upstream {self.name} unknown behavior={step}",
                    category=ErrorCategory.UPSTREAM_BAD_RESPONSE,
                    retryable=False,
                )
        else:
            status = int(step)

        if self._record_side_effects:
            with self._lock:
                self.side_effects.append(
                    {"key": request.idempotency_key, "path": request.path, "status": status}
                )

        if status >= 400:
            self._raise_for_status(status)
        return UpstreamResponse(status_code=status, body={"upstream": self.name, "echo": request.payload})
