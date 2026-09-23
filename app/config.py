"""韧性策略可配置项。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ResilienceConfig:
    # 熔断：连续失败达到阈值后打开
    failure_threshold: int = 5
    # 熔断：统计窗口（秒）
    window_seconds: float = 10.0
    # 熔断：打开后等待多久允许一次试探（秒）
    open_recovery_seconds: float = 1.0
    # 半开：试探成功达到该值则关闭
    half_open_success_threshold: int = 2
    # 半开：试探失败达到该值则重新打开
    half_open_failure_threshold: int = 1
    # 重试：最多尝试次数（含首次）
    max_attempts: int = 3
    # 重试：最大退避（秒）
    max_backoff_seconds: float = 0.5
    # 重试基础退避（秒）
    backoff_base_seconds: float = 0.01
