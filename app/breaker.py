"""按上游维度维护的熔断状态机。

状态迁移（每个上游一把可重入锁，保证并发下迁移一致）：

    CLOSED -- 失败窗口内失败数 >= failure_threshold --> OPEN
    OPEN   -- 距打开时间 >= open_state_seconds -------> HALF_OPEN（发放限量试探许可）
    HALF_OPEN -- 试探请求成功 -------------------------> CLOSED
    HALF_OPEN -- 试探请求失败 -------------------------> OPEN（重新计时）

关键语义：

* CLOSED 下失败时间戳保存在滑动窗口（failure_window_seconds）中，
  短暂零星失败会随窗口滑出，不会累积触发熔断，避免抖动误伤健康上游。
* HALF_OPEN 同时只放行 half_open_max_calls 个试探请求，
  超出的请求在熔断器仍处开放语义下被拒绝。
* 时间来源可注入（clock），便于测试中确定性复现恢复过程。
"""
from __future__ import annotations

import threading
import time
from collections import deque
from enum import Enum
from typing import Callable, Deque

from app.config import CircuitBreakerConfig
from app.context import log_event


class BreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


_CLOSED, _OPEN, _HALF_OPEN = (
    BreakerState.CLOSED,
    BreakerState.OPEN,
    BreakerState.HALF_OPEN,
)


class CircuitBreaker:
    def __init__(
        self,
        upstream: str,
        config: CircuitBreakerConfig,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.upstream = upstream
        self.config = config
        self._clock = clock
        self._lock = threading.RLock()
        self._state: BreakerState = _CLOSED
        self._failure_times: Deque[float] = deque()
        self._opened_at: float | None = None
        self._half_open_permits = 0

    @property
    def state(self) -> BreakerState:
        with self._lock:
            self._maybe_advance()
            return self._state

    def failure_count(self) -> int:
        """当前失败窗口内累计的失败数（先滑出过期记录再计数）。"""
        with self._lock:
            self._prune_failures()
            return len(self._failure_times)

    def allow_request(self) -> bool:
        """是否允许一个请求通过熔断器。

        CLOSED 恒允许；OPEN 到期转 HALF_OPEN 后允许前 N 个试探请求；
        HALF_OPEN 许可发完即拒绝。
        """
        with self._lock:
            self._maybe_advance()
            if self._state == _CLOSED:
                return True
            if self._state == _HALF_OPEN:
                if self._half_open_permits > 0:
                    self._half_open_permits -= 1
                    log_event(
                        "breaker_half_open_permit",
                        upstream=self.upstream,
                        permits_left=self._half_open_permits,
                    )
                    return True
                return False
            return False

    def record_success(self) -> None:
        with self._lock:
            if self._state == _HALF_OPEN:
                self._transition(_CLOSED)
                self._failure_times.clear()
                self._opened_at = None
                self._half_open_permits = 0
            elif self._state == _CLOSED:
                # 健康流量下清空窗口，防止陈旧失败长期滞留。
                self._prune_failures()
            log_event("breaker_record_success", upstream=self.upstream, state=self._state)

    def record_failure(self) -> None:
        with self._lock:
            now = self._clock()
            if self._state == _HALF_OPEN:
                # 任何一个试探失败，立即重新打开并重新计时。
                self._to_open(now)
            elif self._state == _OPEN:
                self._opened_at = now
            else:
                self._failure_times.append(now)
                self._prune_failures(now)
                if len(self._failure_times) >= self.config.failure_threshold:
                    self._to_open(now)
            log_event(
                "breaker_record_failure",
                upstream=self.upstream,
                state=self._state,
                window_failures=len(self._failure_times),
            )

    # ---- 内部方法（调用方须持锁）----

    def _maybe_advance(self) -> None:
        if self._state != _OPEN:
            return
        assert self._opened_at is not None
        if self._clock() - self._opened_at >= self.config.open_state_seconds:
            self._transition(_HALF_OPEN)
            self._half_open_permits = self.config.half_open_max_calls
            self._failure_times.clear()

    def _to_open(self, now: float) -> None:
        self._transition(_OPEN)
        self._opened_at = now
        self._half_open_permits = 0

    def _prune_failures(self, now: float | None = None) -> None:
        now = now if now is not None else self._clock()
        horizon = now - self.config.failure_window_seconds
        while self._failure_times and self._failure_times[0] < horizon:
            self._failure_times.popleft()

    def _transition(self, new_state: str) -> None:
        if new_state == self._state:
            return
        old = self._state
        self._state = new_state
        log_event(
            "breaker_state_transition",
            upstream=self.upstream,
            old_state=old,
            new_state=new_state,
        )


class BreakerRegistry:
    def __init__(
        self,
        config: CircuitBreakerConfig,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config
        self._clock = clock
        self._breakers: dict[str, CircuitBreaker] = {}
        self._lock = threading.Lock()

    def get(self, upstream: str) -> CircuitBreaker:
        with self._lock:
            breaker = self._breakers.get(upstream)
            if breaker is None:
                breaker = CircuitBreaker(upstream, self.config, clock=self._clock)
                self._breakers[upstream] = breaker
            return breaker

    def all_states(self) -> dict[str, BreakerState]:
        with self._lock:
            return {name: b.state for name, b in self._breakers.items()}

    def all_breakers(self) -> dict[str, CircuitBreaker]:
        """返回全部已创建熔断器的快照（只读用途）。"""
        with self._lock:
            return dict(self._breakers)

    def reset(self) -> None:
        with self._lock:
            self._breakers.clear()
