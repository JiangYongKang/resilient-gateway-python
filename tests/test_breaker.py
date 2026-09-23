"""熔断器状态机单测：滑窗、自动恢复、半开许可与并发一致性。"""
import threading

from app.breaker import BreakerRegistry, BreakerState
from app.config import CircuitBreakerConfig


def make_registry(**kw):
    clock = kw.pop("clock", None)
    t = {"v": 0.0}

    def fake_clock():
        return t["v"]

    cfg = CircuitBreakerConfig(
        failure_threshold=kw.pop("failure_threshold", 3),
        open_state_seconds=kw.pop("open_state_seconds", 10.0),
        failure_window_seconds=kw.pop("failure_window_seconds", 5.0),
        half_open_max_calls=kw.pop("half_open_max_calls", 1),
    )
    return BreakerRegistry(cfg, clock=clock or fake_clock), t, fake_clock


def test_sliding_window_failures_expire_and_do_not_trip():
    reg, t, clock = make_registry()
    cb = reg.get("u")
    cb.record_failure()
    t["v"] += 3.0
    cb.record_failure()  # 窗口内 2 次，未达阈值 3
    assert cb.state is BreakerState.CLOSED
    t["v"] += 3.0      # 最早失败（t=0）已滑出 5s 窗口
    cb.record_failure()
    assert cb.state is BreakerState.CLOSED
    assert cb.allow_request() is True


def test_trip_open_then_timeout_half_open_then_success_close():
    reg, t, _ = make_registry()
    cb = reg.get("u")
    for _ in range(3):
        cb.record_failure()
    assert cb.state is BreakerState.OPEN
    assert cb.allow_request() is False
    t["v"] += 9.0
    assert cb.allow_request() is False  # 未到恢复时间
    t["v"] += 1.1
    assert cb.allow_request() is True   # 进入 HALF_OPEN 并拿到唯一许可
    assert cb.state is BreakerState.HALF_OPEN
    assert cb.allow_request() is False  # 试探许可已发完
    cb.record_success()
    assert cb.state is BreakerState.CLOSED


def test_half_open_failure_reopens_with_fresh_timer():
    reg, t, _ = make_registry()
    cb = reg.get("u")
    for _ in range(3):
        cb.record_failure()
    t["v"] += 10.1
    assert cb.allow_request() is True
    cb.record_failure()
    assert cb.state is BreakerState.OPEN
    assert cb.allow_request() is False  # 重新计时，必须再等一个开放窗口
    t["v"] += 10.1
    assert cb.allow_request() is True


def test_concurrent_half_open_permits_only_one_passes():
    reg, t, _ = make_registry(half_open_max_calls=1)
    cb = reg.get("u")
    for _ in range(3):
        cb.record_failure()
    t["v"] += 10.1

    results = []

    def worker():
        results.append(cb.allow_request())

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert sum(1 for r in results if r) == 1
    assert sum(1 for r in results if not r) == 19


def test_registry_isolates_upstreams():
    reg, t, _ = make_registry()
    a = reg.get("a")
    b = reg.get("b")
    for _ in range(3):
        a.record_failure()
    assert a.state is BreakerState.OPEN
    assert b.state is BreakerState.CLOSED
    assert reg.all_states() == {"a": BreakerState.OPEN, "b": BreakerState.CLOSED}
