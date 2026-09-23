"""基于幂等键的并发去重与结果复用。

状态（内存实现，进程内并发安全）：

    不存在 --acquire--> IN_PROGRESS（首个请求获得执行权，只产生一次副作用）
    IN_PROGRESS --acquire（并发同键）--> IN_PROGRESS，调用方拒绝为 409
    IN_PROGRESS --complete--> COMPLETED（缓存结果，后续同键直接复用）
    IN_PROGRESS --fail--> 删除记录（首个请求失败时允许后续重试）

执行失败（含重试耗尽）不会缓存成功结果，键被释放，客户端可凭同键安全重提；
而正在处理中的并发同键请求一定被拒绝，从而杜绝重复副作用。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from enum import Enum
from typing import Any

from app.context import log_event


class DedupStatus(str, Enum):
    ACQUIRED = "acquired"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"


@dataclass(frozen=True)
class DedupResult:
    status: DedupStatus
    payload: Any | None = None


@dataclass
class _Entry:
    done: bool = False
    payload: Any | None = None


class IdempotencyStore:
    def __init__(self) -> None:
        self._entries: dict[str, _Entry] = {}
        self._lock = threading.Lock()

    def acquire(self, key: str) -> DedupResult:
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                self._entries[key] = _Entry()
                log_event("idempotency_acquired", key=_mask(key))
                return DedupResult(DedupStatus.ACQUIRED)
            if entry.done:
                log_event("idempotency_replay", key=_mask(key))
                return DedupResult(DedupStatus.COMPLETED, entry.payload)
            log_event("idempotency_in_progress_reject", key=_mask(key))
            return DedupResult(DedupStatus.IN_PROGRESS)

    def complete(self, key: str, payload: Any) -> None:
        with self._lock:
            entry = self._entries.setdefault(key, _Entry())
            entry.done = True
            entry.payload = payload
            log_event("idempotency_completed", key=_mask(key))

    def fail(self, key: str) -> None:
        with self._lock:
            # 仅当尚未成功完成时释放，避免误删并发完成结果。
            entry = self._entries.get(key)
            if entry is not None and not entry.done:
                del self._entries[key]
                log_event("idempotency_failed_released", key=_mask(key))

    def reset(self) -> None:
        with self._lock:
            self._entries.clear()


def _mask(key: str) -> str:
    """日志中不打印完整幂等键，仅保留可辨认前缀。"""
    return key[:6] + "***" if len(key) > 6 else "***"
