"""按上游维度的运行指标累计。

仅内存计数、进程内并发安全；供只读查看入口使用，
不参与任何转发决策。计数自服务启动以来累计，不随熔断窗口滑动。
"""
from __future__ import annotations

import threading


class MetricsRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, dict[str, int]] = {}

    def _bump(self, upstream: str, field: str) -> None:
        with self._lock:
            counters = self._counters.setdefault(
                upstream, {"retries": 0, "degraded": 0, "rejected": 0}
            )
            counters[field] += 1

    def record_retry(self, upstream: str) -> None:
        """记录一次实际执行的重试（首次尝试不计）。"""
        self._bump(upstream, "retries")

    def record_degraded(self, upstream: str) -> None:
        self._bump(upstream, "degraded")

    def record_rejected(self, upstream: str) -> None:
        self._bump(upstream, "rejected")

    def snapshot(self) -> dict[str, dict[str, int]]:
        """按上游返回 {retries, degraded, rejected} 累计值的快照。"""
        with self._lock:
            return {name: dict(c) for name, c in self._counters.items()}

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()
