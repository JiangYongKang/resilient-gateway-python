"""单次转发的总时间预算。

调用方可随请求携带一个总预算秒数，从请求进入网关起算，
重试之间的退避等待也计入预算；预算耗尽即停止触达上游，
由编排层返回确定的降级结论（``budget_exhausted``）。

* 预算随请求独立计算，并发请求互不影响；
* 时钟可注入，测试中的耗时与耗尽时点完全确定；
* 不携带预算（None）时行为与既有版本完全一致。
"""
from __future__ import annotations

from collections.abc import Callable


class Deadline:
    """一次请求的预算视图；``budget_seconds`` 为 None 表示不限预算。"""

    def __init__(
        self,
        budget_seconds: float | None,
        clock: Callable[[], float],
    ) -> None:
        self._clock = clock
        self._budget = budget_seconds
        self._started_at = clock() if budget_seconds is not None else None

    def remaining(self) -> float | None:
        """剩余预算秒数（可能为负，表示已超支）；不限预算时返回 None。"""
        if self._budget is None:
            return None
        assert self._started_at is not None
        return self._budget - (self._clock() - self._started_at)

    def expired(self) -> bool:
        """预算是否已耗尽（不限预算时恒为 False）。"""
        rem = self.remaining()
        return rem is not None and rem <= 0

    def fits(self, wait_seconds: float) -> bool:
        """再等待 ``wait_seconds`` 后是否仍留有预算（不限预算恒为 True）。"""
        rem = self.remaining()
        return rem is None or wait_seconds < rem
