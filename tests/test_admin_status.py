"""HTTP 层新能力：/admin/status 只读入口、X-Budget-Seconds 请求头、429 映射。"""
import pytest
from fastapi.testclient import TestClient

from app.api import create_app, gateway, stub
from app.bulkhead import BulkheadRegistry
from app.config import BulkheadConfig
from app.upstream import UpstreamOutcome as O, UpstreamResponse as R

from conftest import log_scenario


@pytest.fixture(autouse=True)
def reset_singletons():
    original_breakers = gateway.breakers
    original_store = gateway.idempotency
    original_metrics = gateway.metrics
    original_bulkheads = gateway.bulkheads
    gateway.breakers.reset()
    gateway.idempotency.reset()
    gateway.metrics.reset()
    stub.calls = 0
    stub.requests = []
    stub.set_script([])
    stub.exception_factory = None
    yield
    gateway.breakers = original_breakers
    gateway.idempotency = original_store
    gateway.metrics = original_metrics
    gateway.bulkheads = original_bulkheads


@pytest.fixture()
def client():
    return TestClient(create_app())


def test_admin_status_reports_breaker_and_counters(client):
    # 默认重试 3 次全部失败 -> 重试耗尽降级，retries=2、degraded=1、窗口失败 3。
    stub.set_script([R(O.RETRYABLE_FAILURE, 503)] * 3)
    resp = client.post("/upstreams/stub/call", json={"method": "GET", "path": "/x"})
    assert resp.status_code == 503
    assert resp.json()["error"]["category"] == "retry_exhausted"

    status = client.get("/admin/status").json()
    assert "stub" in status
    entry = status["stub"]
    assert entry["breaker_state"] == "closed"
    assert entry["window_failures"] == 3
    assert entry["retries"] == 2
    assert entry["degraded"] == 1
    assert entry["rejected"] == 0
    assert entry["in_flight"] == 0
    log_scenario(
        "只读入口-熔断状态与累计指标",
        f"state={entry['breaker_state']} window_failures={entry['window_failures']} "
        f"retries={entry['retries']} degraded={entry['degraded']}",
    )


def test_admin_status_counts_rejections(client):
    stub.set_script([R(O.RETRYABLE_FAILURE, 503)])
    resp = client.post("/upstreams/stub/call", json={"method": "POST", "path": "/orders"})
    assert resp.status_code == 422
    entry = client.get("/admin/status").json()["stub"]
    assert entry["rejected"] == 1
    assert entry["degraded"] == 0
    log_scenario("只读入口-拒绝计数", f"rejected={entry['rejected']}")


def test_budget_header_drives_budget_exhaustion(client):
    resp = client.post(
        "/upstreams/stub/call",
        json={"method": "GET", "path": "/x"},
        headers={"X-Budget-Seconds": "0"},
    )
    assert resp.status_code == 503
    data = resp.json()
    assert data["conclusion"] == "degraded"
    assert data["error"]["category"] == "budget_exhausted"
    assert data["attempts"] == 0
    assert stub.calls == 0
    log_scenario("预算请求头-耗尽降级", f"category={data['error']['category']} calls={stub.calls}")


def test_budget_header_within_budget_passes(client):
    resp = client.post(
        "/upstreams/stub/call",
        json={"method": "GET", "path": "/x"},
        headers={"X-Budget-Seconds": "30"},
    )
    assert resp.status_code == 200
    assert resp.json()["conclusion"] == "success"
    log_scenario("预算请求头-预算充足", "conclusion=success")


def test_invalid_budget_header_rejected_422(client):
    resp = client.post(
        "/upstreams/stub/call",
        json={"method": "GET", "path": "/x"},
        headers={"X-Budget-Seconds": "not-a-number"},
    )
    assert resp.status_code == 422
    assert resp.json()["error"]["category"] == "invalid_budget_header"
    assert stub.calls == 0
    log_scenario("预算请求头-非法值拒绝", "status=422 calls=0")


def test_concurrency_limit_maps_to_429(client):
    gateway.bulkheads = BulkheadRegistry(BulkheadConfig(default_max_in_flight=0))
    resp = client.post("/upstreams/stub/call", json={"method": "GET", "path": "/x"})
    assert resp.status_code == 429
    data = resp.json()
    assert data["conclusion"] == "rejected"
    assert data["error"]["category"] == "concurrency_limit_exceeded"
    assert stub.calls == 0
    entry = client.get("/admin/status").json()["stub"]
    assert entry["rejected"] == 1
    assert entry["window_failures"] == 0  # 被挡请求不计入失败统计
    log_scenario("并发隔离-HTTP 429 映射", f"category={data['error']['category']} calls=0")


def test_new_endpoint_does_not_change_existing_behavior(client):
    assert client.get("/").json() == {"Hello": "World"}
    circuits = client.get("/admin/circuits").json()
    assert circuits == {} or "stub" in circuits  # 结构不变：上游名 -> 状态字符串
    resp = client.post("/upstreams/stub/call", json={"method": "GET", "path": "/x"})
    assert resp.status_code == 200
    assert set(resp.json()) == {"conclusion", "upstream", "attempts", "request_id", "payload"}
    log_scenario("兼容性-既有接口行为不变", "GET / 与 /admin/circuits 与转发响应结构不变")
