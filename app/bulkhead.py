"""按上游维度的并发隔离（舱壁）。

每个上游维护一个在途请求计数，上限来自 :class:`BulkheadConfig`：

* 在途数达到上限时，新请求直接被拒绝（``try_acquire`` 返回 False），
  不会进入上游调用、不计入熔断失败统计、也不触碰幂等键；
* 计数增减都在同一把锁内完成，``release`` 只由成功 ``try_acquire``
  的调用方配对调用，保证高并发下既不会超额放行、也不会漏减。
"""
from __future__ import annotations

import threading

from app.config import BulkheadConfig
from app.context import log_event


class Bulkhead:
    """单个上游的在途计数器。"""

    def __init__(self, upstream: str, max_concurrent: int) -> None:
        self.upstream = upstream
        self.max_concurrent = max_concurrent
        self._lock = threading.Lock()
        self._in_flight = 0

    def try_acquire(self) -> bool:
        """尝试占用一个在途名额；成功返回 True，超额返回 False。"""
        with self._lock:
            if self._in_flight >= self.max_concurrent:
                log_event(
                    "bulkhead_rejected",
                    upstream=self.upstream,
                    in_flight=self._in_flight,
                    max_concurrent=self.max_concurrent,
                )
                return False
            self._in_flight += 1
            return True

    def release(self) -> None:
        """释放一个名额；必须与成功的 ``try_acquire`` 严格配对。"""
        with self._lock:
            self._in_flight -= 1
            if self._in_flight < 0:  # 防御：配对错误不应让计数变负
                self._in_flight = 0

    @property
    def in_flight(self) -> int:
        with self._lock:
            return self._in_flight


class BulkheadRegistry:
    """按上游名惰性创建舱壁。"""

    def __init__(self, config: BulkheadConfig) -> None:
        self.config = config
        self._bulkheads: dict[str, Bulkhead] = {}
        self._lock = threading.Lock()

    def get(self, upstream: str) -> Bulkhead:
        with self._lock:
            bulkhead = self._bulkheads.get(upstream)
            if bulkhead is None:
                bulkhead = Bulkhead(upstream, self.config.limit_for(upstream))
                self._bulkheads[upstream] = bulkhead
            return bulkhead

    def in_flight(self, upstream: str) -> int:
        with self._lock:
            bulkhead = self._bulkheads.get(upstream)
        return bulkhead.in_flight if bulkhead is not None else 0
