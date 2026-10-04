"""韧性策略可配置项（仅数据定义）。"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CircuitBreakerConfig:
    # 失败窗口内达到该失败数即打开熔断。
    failure_threshold: int = 5
    # 熔断打开后等待多少秒进入半开试探。
    open_state_seconds: float = 30.0
    # 半开状态同时放行的试探请求数。
    half_open_max_calls: int = 1
    # 失败计数滑动窗口长度；窗口外的零星失败自动滑出。
    failure_window_seconds: float = 60.0


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 3
    backoff_base_seconds: float = 0.01
    backoff_max_seconds: float = 1.0


@dataclass(frozen=True)
class BulkheadConfig:
    """按上游维度的并发隔离（舱壁）配置。

    * ``default_max_concurrent``：未单独配置的上游使用的默认最大在途请求数；
    * ``per_upstream``：按上游名覆盖默认值。
    """

    default_max_concurrent: int = 32
    per_upstream: dict[str, int] | None = None

    def limit_for(self, upstream: str) -> int:
        if self.per_upstream and upstream in self.per_upstream:
            return self.per_upstream[upstream]
        return self.default_max_concurrent
