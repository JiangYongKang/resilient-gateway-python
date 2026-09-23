"""熔断状态机测试：阈值、窗口抗抖动、时间恢复、并发状态迁移一致性。"""

from __future__ import annotations

import threading

import pytest

from app.circuit_breaker import CircuitBreaker, CircuitState, CircuitBreakerRegistry
from app.config import ResilienceConfig


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def advance(self, d: float) -> None:
        self.t += d


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def config():
    return ResilienceConfig(
        failure_threshold=3,
        window_seconds=10,
        open_recovery_seconds=5,
        half_open_success_threshold=2,
        half_open_failure_threshold=1,
    )


def test_brief_failures_do_not_open(clock, config):
    cb = CircuitBreaker("u", config, clock=clock)
    cb.record_failure(cid="c1")
    cb.record_failure(cid="c1")
    assert cb.state is CircuitState.CLOSED
    print("[state] 两次短暂失败后状态:", cb.state.value)


def test_failures_outside_window_expire(clock, config):
    cb = CircuitBreaker("u", config, clock=clock)
    cb.record_failure(cid="c2")
    cb.record_failure(cid="c2")
    clock.advance(11)  # 窗口外失败过期
    cb.record_failure(cid="c2")
    cb.record_failure(cid="c2")
    assert cb.state is CircuitState.CLOSED, cb.snapshot()
    print("[state] 窗口过期后状态:", cb.state.value, "窗口失败数:", cb.snapshot()["window_failures"])


def test_threshold_opens_and_blocks(clock, config):
    cb = CircuitBreaker("u", config, clock=clock)
    for _ in range(3):
        cb.record_failure(cid="c3")
    assert cb.state is CircuitState.OPEN
    assert cb.allow_request(cid="c3") is False
    print("[state] 达阈值后状态:", cb.state.value)


def test_half_open_single_probe_then_close(clock, config):
    cb = CircuitBreaker("u", config, clock=clock)
    for _ in range(3):
        cb.record_failure(cid="c4")
    clock.advance(5)
    # 只有一个试探请求能被放行
    assert cb.allow_request(cid="c4") is True
    assert cb.state is CircuitState.HALF_OPEN
    assert cb.allow_request(cid="c4") is False  # 在途试探被拒
    cb.record_success(cid="c4")
    assert cb.allow_request(cid="c4") is True  # 第二个试探
    cb.record_success(cid="c4")
    assert cb.state is CircuitState.CLOSED
    print("[state] 试探成功后状态:", cb.state.value)


def test_probe_failure_reopens(clock, config):
    cb = CircuitBreaker("u", config, clock=clock)
    for _ in range(3):
        cb.record_failure(cid="c5")
    opened_at_first = cb._opened_at
    clock.advance(5)
    assert cb.allow_request(cid="c5") is True
    cb.record_failure(cid="c5")
    assert cb.state is CircuitState.OPEN
    assert cb._opened_at > opened_at_first  # 恢复计时刷新
    print("[state] 试探失败重新打开, opened_at 刷新:", cb._opened_at)


def test_concurrent_recovery_allows_single_probe(clock, config):
    """恢复时刻并发请求：必须只有一个请求迁移到 HALF_OPEN 并被放行。"""
    cb = CircuitBreaker("u", config, clock=clock)
    for _ in range(3):
        cb.record_failure()
    assert cb.state is CircuitState.OPEN
    clock.advance(5)

    decisions: list[bool] = []
    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()
        decisions.append(cb.allow_request())

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sum(decisions) == 1, decisions
    assert cb.state is CircuitState.HALF_OPEN
    print("[state] 8 并发恢复时刻放行数:", sum(decisions), "状态:", cb.state.value)


def test_concurrent_failures_open_exactly_once(clock, config):
    """并发失败上报：达到阈值时 CLOSED->OPEN 只迁移一次，无抖动。"""
    cb = CircuitBreaker("u", config, clock=clock)
    transitions: list[tuple] = []
    original = cb._transition

    def spy(new_state, now, **kw):
        original(new_state, now, **kw)
        transitions.append(new_state)

    cb._transition = spy
    barrier = threading.Barrier(6)

    def worker():
        barrier.wait()
        cb.record_failure()

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert cb.state is CircuitState.OPEN
    assert transitions == [CircuitState.OPEN], transitions
    print("[state] 6 并发失败迁移序列:", [s.value for s in transitions])


def test_registry_isolates_upstreams(clock, config):
    reg = CircuitBreakerRegistry(config, clock=clock)
    a = reg.get("a")
    b = reg.get("b")
    assert reg.get("a") is a and reg.get("b") is b
    for _ in range(3):
        a.record_failure()
    assert a.state is CircuitState.OPEN
    assert b.state is CircuitState.CLOSED  # 不互相误伤
    print("[state] 上游隔离:", reg.all_snapshots())
