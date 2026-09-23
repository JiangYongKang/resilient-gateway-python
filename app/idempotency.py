"""幂等键去重。

保证同一幂等键：
- 第一个请求获得 FIRST，负责真正调用上游（产生副作用）；
- 并发的同键请求获得 IN_FLIGHT，阻塞等待首个请求完成；
- 首个完成后，所有同键请求（含等待者与之后的重复提交）拿到同一份结论 DUPLICATE。

因此同键请求最多产生一次上游副作用。
"""

from __future__ import annotations

import enum
import threading
from dataclasses import dataclass

from app.logging_utils import get_logger
from app.models import GatewayResult

logger = get_logger("gateway.idempotency")


class DedupeStatus(str, enum.Enum):
    FIRST = "FIRST"
    DUPLICATE = "DUPLICATE"
    IN_FLIGHT = "IN_FLIGHT"


@dataclass
class DedupeOutcome:
    status: DedupeStatus
    result: GatewayResult | None = None


class IdempotencyStore:
    def __init__(self) -> None:
        # Condition 自带一把锁，finished/inflight 均在其保护下访问
        self._cond = threading.Condition()
        self._finished: dict[str, GatewayResult] = {}
        self._inflight: set[str] = set()

    def begin(self, key: str) -> DedupeOutcome:
        """登记幂等键。

        - 已有完成结论 -> 直接复用（DUPLICATE）；
        - 同键正在执行 -> 等待至完成后复用（DUPLICATE）；
        - 否则抢占执行权（FIRST）。
        """
        with self._cond:
            if key in self._finished:
                logger.info("dedupe hit-finished key=%s", key, extra={"correlation_id": "-"})
                return DedupeOutcome(DedupeStatus.DUPLICATE, self._finished[key])
            if key in self._inflight:
                logger.info("dedupe wait-inflight key=%s", key, extra={"correlation_id": "-"})
                self._cond.wait_for(lambda: key in self._finished)
                return DedupeOutcome(DedupeStatus.DUPLICATE, self._finished[key])
            self._inflight.add(key)
            return DedupeOutcome(DedupeStatus.FIRST)

    def finish(self, key: str, result: GatewayResult) -> None:
        """FIRST 执行者写入结论并唤醒所有等待者。"""
        with self._cond:
            self._finished[key] = result
            self._inflight.discard(key)
            self._cond.notify_all()
            logger.info("dedupe stored key=%s", key, extra={"correlation_id": "-"})
