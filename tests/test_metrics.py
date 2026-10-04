"""运行指标累计与只读查看入口：重试/降级/拒绝计数、熔断状态与窗口失败数。"""
import threading

import pytest
from fastapi.testclient import TestClient

from app.api import create_app, gateway, stub
from app.config import CircuitBreakerConfig, RetryConfig
from app.gateway import Conclusion, Gateway
from app.upstream import (
    StubUpstreamClient,
    UpstreamOutcome as O,
    UpstreamRequest,
    UpstreamResponse as R,
)


def make_gateway(script):
    client = StubUpstreamClient("stub")
    client.set_script(script)
    gw = Gateway(
        {"stub": client},
        breaker_config=CircuitBreakerConfig(failure_threshold=100),
        retry_config=RetryConfig(max_attempts=3, backoff_base_seconds=0.01),
        clock=lambda: 0.0,
        sleeper=lambda _s: None,
        rng=lambda: 1.0,
    )
    return gw, client


def retryable():
    return R(O.RETRYABLE_FAILURE, 503, None)


def ok():
    return R(O.SUCCESS, 200, {"v": 1})


def test_retry_and_degraded_counters_accumulate():
    print("[场景] 指标累计：重试等待与降级结论按上游计数")
    gw, _ = make_gateway([retryable(), retryable(), ok()] + [retryable()] * 3)
    first = gw.handle("stub", UpstreamRequest("GET", "/x"))
    assert first.conclusion is Conclusion.SUCCESS
    snap = gw.metrics.snapshot("stub")
    assert snap == {"retries": 2, "degraded": 0, "rejected": 0}

    second = gw.handle("stub", UpstreamRequest("GET", "/y"))
    assert second.conclusion is Conclusion.DEGRADED
    snap = gw.metrics.snapshot("stub")
    assert snap == {"retries": 4, "degraded": 1, "rejected": 0}
    print("[结果] 两次请求后 retries=4, degraded=1, rejected=0 ✅")


def test_rejected_counter_accumulates():
    print("[场景] 指标累计：拒绝结论计数（非幂等禁止重试）")
    gw, _ = make_gateway([retryable()])
    result = gw.handle("stub", UpstreamRequest("POST", "/orders"))
    assert result.conclusion is Conclusion.REJECTED
    snap = gw.metrics.snapshot("stub")
    assert snap["rejected"] == 1
    assert snap["degraded"] == 0
    print("[结果] rejected=1，其余为 0 ✅")


def test_counters_consistent_under_concurrency():
    print("[场景] 并发下指标计数一致：20 线程各重试 2 次后成功")
    # 每个线程独立上游（独立桩与脚本），避免共享脚本竞争。
    clients = {}
    for i in range(20):
        c = StubUpstreamClient(f"u{i}")
        c.set_script([retryable(), retryable(), ok()])
        clients[f"u{i}"] = c
    gw = Gateway(
        clients,
        breaker_config=CircuitBreakerConfig(failure_threshold=100),
        retry_config=RetryConfig(max_attempts=3, backoff_base_seconds=0.01),
        clock=lambda: 0.0,
        sleeper=lambda _s: None,
        rng=lambda: 1.0,
    )
    barrier = threading.Barrier(20)

    def worker(i):
        barrier.wait(timeout=5.0)
        result = gw.handle(f"u{i}", UpstreamRequest("GET", "/c"))
        assert result.conclusion is Conclusion.SUCCESS

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    total_retries = sum(
        gw.metrics.snapshot(f"u{i}")["retries"] for i in range(20)
    )
    assert total_retries == 40  # 20 请求 × 2 次重试等待，无丢失
    for i in range(20):
        snap = gw.metrics.snapshot(f"u{i}")
        assert snap == {"retries": 2, "degraded": 0, "rejected": 0}
    print("[结果] 20 并发后总 retries=40，每上游恰好 2，计数无丢失 ✅")


# ---- 只读查看入口（HTTP）----


@pytest.fixture(autouse=True)
def reset_singletons():
    gateway.breakers.reset()
    gateway.idempotency.reset()
    gateway.metrics.reset()
    stub.calls = 0
    stub.requests = []
    stub.set_script([])
    stub.exception_factory = None
    yield


@pytest.fixture()
def client():
    return TestClient(create_app())


def test_admin_upstreams_reports_state_failures_and_counters(client):
    print("[场景] 只读入口：熔断状态 + 窗口失败数 + 累计指标")
    # 请求 1：失败 1 次后成功（1 次重试等待，窗口内 1 次失败）
    stub.set_script([retryable(), ok()])
    resp = client.post("/upstreams/stub/call", json={"method": "GET", "path": "/x"})
    assert resp.status_code == 200

    # 请求 2：3 次可重试失败耗尽（2 次重试等待，窗口内再 +3 次失败）
    stub.set_script([retryable()] * 3)
    resp = client.post("/upstreams/stub/call", json={"method": "GET", "path": "/y"})
    assert resp.status_code == 503

    stats = client.get("/admin/upstreams").json()["stub"]
    assert stats["breaker_state"] == "closed"  # 4 < 默认阈值 5，未熔断
    assert stats["window_failures"] == 4  # 两次请求的失败都在窗口内
    assert stats["retries"] == 3  # 1 + 2 次重试等待
    assert stats["degraded"] == 1
    assert stats["rejected"] == 0
    assert stats["in_flight"] == 0
    print(f"[结果] /admin/upstreams -> {stats} ✅")


def test_admin_circuits_behavior_unchanged(client):
    print("[场景] 兼容：既有 /admin/circuits 行为不变")
    client.post("/upstreams/stub/call", json={"method": "GET", "path": "/x"})
    state = client.get("/admin/circuits").json()
    assert state == {"stub": "closed"}  # 仍是 名称->状态 的扁平结构
    print("[结果] /admin/circuits 响应结构保持原样 ✅")


def test_budget_header_maps_to_budget_exhausted_503(client):
    print("[场景] HTTP 预算请求头：预算为 0 直接降级且不打上游")
    resp = client.post(
        "/upstreams/stub/call",
        json={"method": "GET", "path": "/x"},
        headers={"X-Request-Budget-Ms": "0"},
    )
    assert resp.status_code == 503
    data = resp.json()
    assert data["conclusion"] == "degraded"
    assert data["error"]["category"] == "budget_exhausted"
    assert stub.calls == 0
    print("[结果] X-Request-Budget-Ms: 0 -> 503 budget_exhausted, upstream calls=0 ✅")


def test_without_budget_header_behavior_unchanged(client):
    print("[场景] 兼容：不带预算请求头时行为与原来一致")
    stub.set_script([retryable(), ok()])
    resp = client.post("/upstreams/stub/call", json={"method": "GET", "path": "/x"})
    assert resp.status_code == 200
    assert resp.json()["conclusion"] == "success"
    assert stub.calls == 2
    print("[结果] 无预算头 -> 正常重试成功 ✅")
