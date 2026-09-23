"""按上游维度维护的熔断状态机：关闭 / 打开 / 半开。

并发一致性：所有状态判定与迁移都在同一把 RLock 内完成；
OPEN -> HALF_OPEN 的迁移由“抢到迁移权的那个请求”原子完成，
且半开期间只放一个试探请求在途（_half_open_in_flight）。

抗抖动：
- CLOSED 下只统计时间窗口内的失败，窗口外失败过期不计入；
- 短暂失败（未达阈值）不会打开；
- 试探成功达到阈值才真正关闭，试探失败立即重新打开。
"""

from __future__ import annotations

import enum
import threading
import time

from app.config import ResilienceConfig
from app.logging_utils import get_logger

logger = get_logger("gateway.circuit")


class CircuitState(str, enum.Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    def __init__(self, upstream: str, config: ResilienceConfig, *, clock=time.monotonic) -> None:
        self.upstream = upstream
        self.config = config
        self._clock = clock
        self._lock = threading.RLock()
        self._state = CircuitState.CLOSED
        # CLOSED 状态下窗口内失败时间戳
        self._failures: list[float] = []
        self._opened_at = 0.0
        self._half_open_successes = 0
        self._half_open_failures = 0
        self._half_open_in_flight = False

    # ---- 只读视图 -------------------------------------------------------

    @property
    def state(self) -> CircuitState:
        with self._lock:
            return self._state

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "upstream": self.upstream,
                "state": self._state.value,
                "window_failures": len(self._failures),
                "half_open_successes": self._half_open_successes,
                "half_open_failures": self._half_open_failures,
            }

    # ---- 内部工具（调用时持锁） -----------------------------------------

    def _prune_failures(self, now: float) -> None:
        cutoff = now - self.config.window_seconds
        if self._failures and self._failures[0] <= cutoff:
            self._failures = [t for t in self._failures if t > cutoff]

    def _transition(self, new_state: CircuitState, now: float, *, cid: str | None = None, **extra) -> None:
        old = self._state
        if old == new_state:
            return
        self._state = new_state
        if new_state == CircuitState.OPEN:
            self._opened_at = now
            self._half_open_in_flight = False
            self._half_open_successes = 0
            self._half_open_failures = 0
        elif new_state == CircuitState.HALF_OPEN:
            self._half_open_in_flight = True
            self._half_open_successes = 0
            self._half_open_failures = 0
        elif new_state == CircuitState.CLOSED:
            self._failures = []
            self._half_open_in_flight = False
        logger.info(
            "circuit state-change %s -> %s upstream=%s %s",
            old.value, new_state.value, self.upstream, extra,
            extra={"correlation_id": cid or "-"},
        )

    # ---- 对外 API --------------------------------------------------------

    def allow_request(self, *, cid: str | None = None) -> bool:
        """是否放行当前请求。

        - CLOSED：始终放行；
        - OPEN：恢复时间未到 -> 拒绝；到了 -> 原子迁移 HALF_OPEN，只放一个试探；
        - HALF_OPEN：已有试探在途 -> 拒绝，保证同一时刻只有一个试探。
        """
        with self._lock:
            now = self._clock()
            if self._state == CircuitState.CLOSED:
                return True
            if self._state == CircuitState.OPEN:
                if now - self._opened_at < self.config.open_recovery_seconds:
                    return False
                self._transition(CircuitState.HALF_OPEN, now, cid=cid)
                return True
            # HALF_OPEN
            if self._half_open_in_flight:
                return False
            # 上一轮试探已结束但尚未满足关闭阈值：继续允许下一个试探
            self._half_open_in_flight = True
            return True

    def record_success(self, *, cid: str | None = None) -> None:
        with self._lock:
            now = self._clock()
            if self._state == CircuitState.CLOSED:
                # 成功会冲刷窗口内失败，避免历史短暂失败长期累积造成误判
                self._failures = []
                return
            if self._state == CircuitState.HALF_OPEN:
                self._half_open_in_flight = False
                self._half_open_successes += 1
                logger.info(
                    "circuit probe success upstream=%s successes=%d/%d",
                    self.upstream,
                    self._half_open_successes,
                    self.config.half_open_success_threshold,
                    extra={"correlation_id": cid or "-"},
                )
                if self._half_open_successes >= self.config.half_open_success_threshold:
                    self._transition(CircuitState.CLOSED, now, cid=cid)

    def record_failure(self, *, cid: str | None = None) -> None:
        with self._lock:
            now = self._clock()
            if self._state == CircuitState.HALF_OPEN:
                self._half_open_in_flight = False
                self._half_open_failures += 1
                logger.info(
                    "circuit probe failure upstream=%s failures=%d/%d",
                    self.upstream,
                    self._half_open_failures,
                    self.config.half_open_failure_threshold,
                    extra={"correlation_id": cid or "-"},
                )
                if self._half_open_failures >= self.config.half_open_failure_threshold:
                    # 重新打开，恢复计时刷新
                    self._transition(CircuitState.OPEN, now, cid=cid)
                return
            if self._state == CircuitState.OPEN:
                return
            # CLOSED：窗口统计
            self._failures.append(now)
            self._prune_failures(now)
            logger.info(
                "circuit failure recorded upstream=%s window_failures=%d/%d",
                self.upstream, len(self._failures), self.config.failure_threshold,
                extra={"correlation_id": cid or "-"},
            )
            if len(self._failures) >= self.config.failure_threshold:
                self._transition(CircuitState.OPEN, now, cid=cid)


class CircuitBreakerRegistry:
    """按上游名持有独立熔断器（懒创建）。"""

    def __init__(self, config: ResilienceConfig, *, clock=time.monotonic) -> None:
        self.config = config
        self._clock = clock
        self._lock = threading.Lock()
        self._breakers: dict[str, CircuitBreaker] = {}

    def get(self, upstream: str) -> CircuitBreaker:
        with self._lock:
            breaker = self._breakers.get(upstream)
            if breaker is None:
                breaker = CircuitBreaker(upstream, self.config, clock=self._clock)
                self._breakers[upstream] = breaker
            return breaker

    def all_snapshots(self) -> dict[str, dict]:
        with self._lock:
            return {name: b.snapshot() for name, b in self._breakers.items()}
