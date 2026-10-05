"""韧性策略可配置项（仅数据定义）。"""
from __future__ import annotations

from dataclasses import dataclass, field


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
class BudgetConfig:
    # 调用方未随请求携带预算时使用的默认总预算秒数；
    # None 表示不限预算（默认，保持既有行为不变）。
    default_seconds: float | None = None


@dataclass(frozen=True)
class BulkheadConfig:
    # 每个上游默认允许的最大在途请求数；
    # None 表示不限制（默认，保持既有行为不变）。
    default_max_in_flight: int | None = None
    # 按上游名覆盖默认值的配置。
    per_upstream: dict[str, int] = field(default_factory=dict)
