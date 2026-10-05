"""单次转发总时间预算：进入即耗尽、退避计入预算、键释放、并发预算独立。"""
import threading

from app.config import BudgetConfig, CircuitBreakerConfig, RetryConfig
from app.errors import ErrorCategory
from app.gateway import Conclusion, Gateway
from app.upstream import (
    StubUpstreamClient,
    UpstreamOutcome as O,
    UpstreamRequest,
    UpstreamResponse as R,
)

from conftest import log_scenario


def make_gateway(script=None, *, max_attempts=3, backoff_base=0.5, default_budget=None):
    """时钟可推进的网关：sleeper 会把等待时间计入时钟，模拟真实耗时。"""
    time_holder = {"t": 0.0}
    client = StubUpstreamClient("stub")
    if script is not None:
        client.set_script(script)

    def sleeper(seconds: float) -> None:
        time_holder["t"] += seconds

    gw = Gateway(
        {"stub": client},
        breaker_config=CircuitBreakerConfig(failure_threshold=100),
        retry_config=RetryConfig(
            max_attempts=max_attempts, backoff_base_seconds=backoff_base
        ),
        budget_config=BudgetConfig(default_seconds=default_budget),
        clock=lambda: time_holder["t"],
        sleeper=sleeper,
        rng=lambda: 1.0,
    )
    return gw, client, time_holder


def fail():
    return R(O.RETRYABLE_FAILURE, 503)


def ok():
    return R(O.SUCCESS, 200, {"ok": True})


def test_budget_exhausted_on_entry_degrades_without_calling_upstream():
    gw, client, _ = make_gateway([ok()])
    r = gw.handle("stub", UpstreamRequest("GET", "/x"), budget_seconds=0.0)
    assert r.conclusion is Conclusion.DEGRADED
    assert r.reason == ErrorCategory.BUDGET_EXHAUSTED.value
    # 与"重试次数用尽""熔断打开"可区分
    assert r.reason != ErrorCategory.RETRY_EXHAUSTED.value
    assert r.reason != ErrorCategory.CIRCUIT_OPEN.value
    assert r.attempts == 0
    assert client.calls == 0  # 预算耗尽不再打上游
    assert r.payload == {"degraded": True, "upstream": "stub", "data": None}
    log_scenario(
        "预算耗尽-进入即耗尽",
        f"conclusion={r.conclusion.value} reason={r.reason} upstream_calls={client.calls}",
    )


def test_backoff_exceeding_budget_stops_without_extra_attempt():
    # 第一次失败后退避 1s 会超出 0.5s 预算：不再等待也不再打上游。
    gw, client, t = make_gateway([fail(), ok()], backoff_base=1.0)
    r = gw.handle("stub", UpstreamRequest("GET", "/x"), budget_seconds=0.5)
    assert r.conclusion is Conclusion.DEGRADED
    assert r.reason == ErrorCategory.BUDGET_EXHAUSTED.value
    assert r.attempts == 1
    assert client.calls == 1
    assert t["t"] == 0.0  # 没有发生任何退避等待
    log_scenario(
        "预算耗尽-退避超预算",
        f"reason={r.reason} attempts={r.attempts} elapsed={t['t']}",
    )


def test_retry_within_budget_succeeds():
    gw, client, t = make_gateway([fail(), ok()], backoff_base=0.3)
    r = gw.handle("stub", UpstreamRequest("GET", "/x"), budget_seconds=1.0)
    assert r.conclusion is Conclusion.SUCCESS
    assert r.attempts == 2
    assert client.calls == 2
    assert t["t"] == 0.3  # 退避等待计入了预算时钟
    log_scenario(
        "预算内重试成功",
        f"conclusion={r.conclusion.value} attempts={r.attempts} elapsed={t['t']}",
    )


def test_budget_consumption_accumulates_across_attempts():
    # 第一次退避 0.4s 在预算内；第二次退避 0.8s 超出剩余 0.1s。
    gw, client, t = make_gateway([fail(), fail(), ok()], backoff_base=0.4)
    r = gw.handle("stub", UpstreamRequest("GET", "/x"), budget_seconds=0.5)
    assert r.conclusion is Conclusion.DEGRADED
    assert r.reason == ErrorCategory.BUDGET_EXHAUSTED.value
    assert r.attempts == 2
    assert client.calls == 2
    assert t["t"] == 0.4
    log_scenario(
        "预算耗尽-跨尝试累计",
        f"reason={r.reason} attempts={r.attempts} elapsed={t['t']}",
    )


def test_budget_exhaustion_releases_idempotency_key():
    gw, client, _ = make_gateway([fail(), ok()], backoff_base=1.0)
    req = UpstreamRequest("POST", "/orders")
    first = gw.handle("stub", req, idempotency_key="K-budget", budget_seconds=0.5)
    assert first.conclusion is Conclusion.DEGRADED
    assert first.reason == ErrorCategory.BUDGET_EXHAUSTED.value
    # 预算耗尽视为失败：键被释放，客户端可凭同键安全重提
    second = gw.handle("stub", req, idempotency_key="K-budget", budget_seconds=10.0)
    assert second.conclusion is Conclusion.SUCCESS
    assert client.calls == 2
    log_scenario(
        "预算耗尽-幂等键释放",
        f"first={first.reason} second={second.conclusion.value} calls={client.calls}",
    )


def test_default_budget_from_config_applies_and_request_overrides():
    gw, client, _ = make_gateway([ok()], default_budget=0.0)
    r1 = gw.handle("stub", UpstreamRequest("GET", "/x"))
    assert r1.reason == ErrorCategory.BUDGET_EXHAUSTED.value
    assert client.calls == 0
    # 调用方随请求显式携带预算时覆盖默认配置
    r2 = gw.handle("stub", UpstreamRequest("GET", "/x"), budget_seconds=5.0)
    assert r2.conclusion is Conclusion.SUCCESS
    assert client.calls == 1
    log_scenario(
        "默认预算与请求级覆盖",
        f"default={r1.reason} override={r2.conclusion.value}",
    )


def test_no_budget_keeps_existing_behavior():
    gw, client, _ = make_gateway([fail(), fail(), ok()], backoff_base=0.5)
    r = gw.handle("stub", UpstreamRequest("GET", "/x"))
    assert r.conclusion is Conclusion.SUCCESS
    assert r.attempts == 3
    assert client.calls == 3
    log_scenario("不携带预算-行为不变", f"conclusion={r.conclusion.value} attempts={r.attempts}")


def test_concurrent_requests_have_independent_budgets():
    gw, client, _ = make_gateway([ok(), ok()])
    barrier = threading.Barrier(2)
    results: dict[str, object] = {}

    def worker(name: str, budget):
        barrier.wait()
        results[name] = gw.handle(
            "stub", UpstreamRequest("GET", "/x"), budget_seconds=budget
        )

    threads = [
        threading.Thread(target=worker, args=("no-budget", None)),
        threading.Thread(target=worker, args=("zero-budget", 0.0)),
    ]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    zero = results["zero-budget"]
    free = results["no-budget"]
    assert zero.conclusion is Conclusion.DEGRADED
    assert zero.reason == ErrorCategory.BUDGET_EXHAUSTED.value
    assert free.conclusion is Conclusion.SUCCESS
    assert client.calls == 1  # 只有无预算限制的请求触达上游
    log_scenario(
        "并发请求预算各自独立",
        f"zero_budget={zero.reason} no_budget={free.conclusion.value}",
    )
