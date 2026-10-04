"""按上游维度的运行指标（只读查看入口的数据源）。

统计自进程启动以来每个上游的：

* ``retries``  —— 实际发生的重试等待次数（首次尝试不计）；
* ``degraded`` —— 返回降级结论的次数（含熔断打开、重试耗尽、预算耗尽）；
* ``rejected`` —— 返回拒绝结论的次数（含同键冲突、并发超限、上游致命拒绝等）。

所有计数在同一把锁内累加，快照读取也是原子的，便于并发下核对。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass


@dataclass
class _Counters:
    retries: int = 0
    degraded: int = 0
    rejected: int = 0


class MetricsRegistry:
    def __init__(self) -> None:
        self._counters: dict[str, _Counters] = {}
        self._lock = threading.Lock()

    def _get(self, upstream: str) -> _Counters:
        counters = self._counters.get(upstream)
        if counters is None:
            counters = _Counters()
            self._counters[upstream] = counters
        return counters

    def record_retry(self, upstream: str) -> None:
        with self._lock:
            self._get(upstream).retries += 1

    def record_degraded(self, upstream: str) -> None:
        with self._lock:
            self._get(upstream).degraded += 1

    def record_rejected(self, upstream: str) -> None:
        with self._lock:
            self._get(upstream).rejected += 1

    def snapshot(self, upstream: str) -> dict[str, int]:
        with self._lock:
            counters = self._counters.get(upstream)
            if counters is None:
                return {"retries": 0, "degraded": 0, "rejected": 0}
            return {
                "retries": counters.retries,
                "degraded": counters.degraded,
                "rejected": counters.rejected,
            }

    def all_upstreams(self) -> list[str]:
        with self._lock:
            return list(self._counters)

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()
