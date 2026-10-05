"""按上游维度的并发隔离（bulkhead）。

每个上游有最大在途请求数；超限请求在进入幂等/熔断/上游调用之前
即被确定性拒绝，不计入失败统计，也不触碰幂等键。

* 在途计数全部在同一把锁内增减，高并发下既不会超额放行，
  也不会漏减已结束的请求；
* 许可（:class:`Permit``）的释放是幂等的，重复释放安全；
* 未配置上限（None）时不限制，行为与既有版本完全一致。
"""
from __future__ import annotations

import threading
from collections.abc import Callable

from app.config import BulkheadConfig
from app.context import log_event


class Permit:
    """在途许可：请求处理完毕必须释放；释放幂等，可重复调用。"""

    def __init__(self, on_release: Callable[[], None] | None = None) -> None:
        self._on_release = on_release
        self._released = False
        self._lock = threading.Lock()

    def __enter__(self) -> "Permit":
        return self

    def __exit__(self, *exc: object) -> None:
        self.release()

    def release(self) -> None:
        with self._lock:
            if self._released:
                return
            self._released = True
        if self._on_release is not None:
            self._on_release()


class BulkheadRegistry:
    def __init__(self, config: BulkheadConfig) -> None:
        self._config = config
        self._lock = threading.Lock()
        self._in_flight: dict[str, int] = {}

    def _limit_for(self, upstream: str) -> int | None:
        return self._config.per_upstream.get(
            upstream, self._config.default_max_in_flight
        )

    def try_acquire(self, upstream: str) -> Permit | None:
        """尝试占一个在途名额；超限返回 None（不阻塞、不等待）。"""
        limit = self._limit_for(upstream)
        if limit is None:
            return Permit()  # 不限制：占位许可，保持调用方路径一致
        with self._lock:
            current = self._in_flight.get(upstream, 0)
            if current >= limit:
                log_event(
                    "bulkhead_rejected",
                    upstream=upstream,
                    in_flight=current,
                    limit=limit,
                )
                return None
            self._in_flight[upstream] = current + 1
            log_event(
                "bulkhead_acquired",
                upstream=upstream,
                in_flight=current + 1,
                limit=limit,
            )
        return Permit(lambda: self._release(upstream))

    def _release(self, upstream: str) -> None:
        with self._lock:
            current = self._in_flight.get(upstream, 0)
            assert current > 0, f"bulkhead in-flight underflow: {upstream}"
            self._in_flight[upstream] = current - 1
            log_event(
                "bulkhead_released", upstream=upstream, in_flight=current - 1
            )

    def in_flight(self, upstream: str) -> int:
        """当前在途数（用于状态查看与测试断言）。"""
        with self._lock:
            return self._in_flight.get(upstream, 0)
