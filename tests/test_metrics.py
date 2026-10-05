"""运行指标累计与只读状态视图：重试/降级/拒绝计数、熔断状态与窗口失败数。"""
from app.config import CircuitBreakerConfig, RetryConfig
from app.errors import ErrorCategory
from app.gateway import Conclusion, Gateway
from app.upstream import (
    StubUpstreamClient,
    UpstreamOutcome as O,
    UpstreamRequest,
    UpstreamResponse as R,
)

from conftest import log_scenario


def fail():
    return R(O.RETRYABLE_FAILURE, 503)


def ok():
    return R(O.SUCCESS, 200, {"ok": True})


def make_gateway(script, *, max_attempts=2):
    client = StubUpstreamClient("stub")
    client.set_script(script)
    gw = Gateway(
        {"stub": client},
        breaker_config=CircuitBreakerConfig(failure_threshold=100),
        retry_config=RetryConfig(max_attempts=max_attempts, backoff_base_seconds=0.0),
        sleeper=lambda _s: None,
        rng=lambda: 1.0,
    )
    return gw, client


def test_metrics_accumulate_retries_degraded_rejected():
    gw, client = make_gateway([fail(), ok(), fail(), fail(), fail()])
    # 1) GET 失败一次后重试成功 -> retries=1
    r1 = gw.handle("stub", UpstreamRequest("GET", "/x"))
    assert r1.conclusion is Conclusion.SUCCESS
    # 2) GET 两次失败 -> 重试耗尽降级 -> degraded=1, retries 再 +1
    r2 = gw.handle("stub", UpstreamRequest("GET", "/x"))
    assert r2.conclusion is Conclusion.DEGRADED
    assert r2.reason == ErrorCategory.RETRY_EXHAUSTED.value
    # 3) POST 无幂等键失败 -> 确定性拒绝 -> rejected=1
    r3 = gw.handle("stub", UpstreamRequest("POST", "/orders"))
    assert r3.conclusion is Conclusion.REJECTED
    assert r3.reason == ErrorCategory.NON_IDEMPOTENT_RETRY_FORBIDDEN.value

    snap = gw.status_snapshot()["stub"]
    assert snap["retries"] == 2
    assert snap["degraded"] == 1
    assert snap["rejected"] == 1
    assert snap["breaker_state"] == "closed"
    assert snap["window_failures"] == 4  # 1 + 2 + 1 次失败全部计入窗口
    assert snap["in_flight"] == 0
    log_scenario(
        "指标累计-重试/降级/拒绝",
        f"retries={snap['retries']} degraded={snap['degraded']} "
        f"rejected={snap['rejected']} window_failures={snap['window_failures']}",
    )


def test_metrics_count_budget_exhaustion_as_degraded():
    gw, client = make_gateway([ok()])
    r = gw.handle("stub", UpstreamRequest("GET", "/x"), budget_seconds=0.0)
    assert r.reason == ErrorCategory.BUDGET_EXHAUSTED.value
    snap = gw.status_snapshot()["stub"]
    assert snap["degraded"] == 1
    assert snap["rejected"] == 0
    assert snap["retries"] == 0
    assert snap["window_failures"] == 0  # 预算耗尽不算上游失败
    log_scenario(
        "指标累计-预算耗尽计为降级",
        f"degraded={snap['degraded']} window_failures={snap['window_failures']}",
    )


def test_status_snapshot_is_read_only():
    gw, client = make_gateway([fail(), ok(), ok()])
    gw.handle("stub", UpstreamRequest("GET", "/x"))  # 1 次失败 + 重试成功
    first = gw.status_snapshot()
    second = gw.status_snapshot()
    assert first == second  # 重复读取不改变任何状态
    # 读取后转发行为不受影响
    r = gw.handle("stub", UpstreamRequest("GET", "/x"))
    assert r.conclusion is Conclusion.SUCCESS
    assert gw.status_snapshot()["stub"]["breaker_state"] == "closed"
    log_scenario("只读视图-读取无副作用", "两次快照一致且后续请求正常")


def test_window_failures_follow_breaker_window():
    time_holder = {"t": 0.0}
    client = StubUpstreamClient("stub")
    client.set_script([fail(), ok()])
    gw = Gateway(
        {"stub": client},
        breaker_config=CircuitBreakerConfig(
            failure_threshold=100, failure_window_seconds=10.0
        ),
        retry_config=RetryConfig(max_attempts=1, backoff_base_seconds=0.0),
        clock=lambda: time_holder["t"],
        sleeper=lambda _s: None,
        rng=lambda: 1.0,
    )
    gw.handle("stub", UpstreamRequest("GET", "/x"))  # 1 次失败
    assert gw.status_snapshot()["stub"]["window_failures"] == 1
    time_holder["t"] += 11.0  # 滑出窗口
    assert gw.status_snapshot()["stub"]["window_failures"] == 0
    log_scenario("窗口失败数-随窗口滑动", "11s 后 window_failures 1->0")
