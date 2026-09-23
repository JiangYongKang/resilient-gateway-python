"""HTTP 层测试：对外结论映射、关联标识、既有根路由行为。"""
import pytest
from fastapi.testclient import TestClient

from app.api import create_app, gateway, stub
from app.upstream import UpstreamOutcome as O, UpstreamResponse as R


@pytest.fixture(autouse=True)
def reset_singletons():
    original_registry = gateway.breakers
    original_store = gateway.idempotency
    gateway.breakers.reset()
    gateway.idempotency.reset()
    stub.calls = 0
    stub.requests = []
    stub.set_script([])
    stub.exception_factory = None
    yield
    gateway.breakers = original_registry
    gateway.idempotency = original_store


@pytest.fixture()
def client():
    return TestClient(create_app())


def test_root_behavior_unchanged(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert resp.json() == {"Hello": "World"}


def test_success_response_carries_request_id(client):
    resp = client.post("/upstreams/stub/call", json={"method": "GET", "path": "/x"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["conclusion"] == "success"
    assert data["request_id"]
    assert resp.headers["X-Request-ID"] == data["request_id"]


def test_degraded_response_is_normalized_503(client):
    # 默认熔断阈值 5，3 次重试失败不会触发熔断，结论为重试耗尽降级。
    stub.set_script(
        [R(O.RETRYABLE_FAILURE, 503)] * gateway.retry_config.max_attempts
    )
    resp = client.post("/upstreams/stub/call", json={"method": "GET", "path": "/x"})
    assert resp.status_code == 503
    data = resp.json()
    assert data["conclusion"] == "degraded"
    assert data["error"]["category"] == "retry_exhausted"
    assert data["payload"]["degraded"] is True


def test_post_without_idempotency_key_rejected_after_failure(client):
    stub.set_script([R(O.RETRYABLE_FAILURE, 503)])
    resp = client.post(
        "/upstreams/stub/call", json={"method": "POST", "path": "/orders"}
    )
    assert resp.status_code == 422
    data = resp.json()
    assert data["conclusion"] == "rejected"
    assert data["error"]["category"] == "non_idempotent_retry_forbidden"
    assert stub.calls == 1


def test_unknown_upstream_is_normalized_503(client):
    resp = client.post(
        "/upstreams/no-such-upstream/call", json={"method": "GET", "path": "/x"}
    )
    assert resp.status_code == 503
    data = resp.json()
    assert data["conclusion"] == "rejected"
    assert data["error"]["category"] == "upstream_unavailable"
    assert "no-such-upstream" not in data["error"]["message"]


def test_fatal_failure_maps_to_502(client):
    stub.set_script([R(O.FATAL_FAILURE, 400, None, "bad")])
    resp = client.post("/upstreams/stub/call", json={"method": "GET", "path": "/x"})
    assert resp.status_code == 502
    assert resp.json()["error"]["category"] == "upstream_rejected"


def test_upstream_exception_never_leaks(client):
    def boom():
        raise RuntimeError("internal secret: AKIA1234567890")

    stub.exception_factory = boom
    resp = client.post("/upstreams/stub/call", json={"method": "GET", "path": "/x"})
    assert resp.status_code == 503
    body = resp.text
    assert "AKIA1234567890" not in body
    assert "internal secret" not in body


def test_circuit_open_returns_503_and_admin_reports_open(client):
    import time

    from app.breaker import BreakerRegistry
    from app.config import CircuitBreakerConfig

    # 将阈值调低到 2，制造两次失败触发熔断
    gateway.breakers = BreakerRegistry(
        CircuitBreakerConfig(failure_threshold=2, open_state_seconds=30.0),
        clock=time.monotonic,
    )
    stub.set_script([R(O.RETRYABLE_FAILURE, 503)] * 4)
    for _ in range(2):
        client.post("/upstreams/stub/call", json={"method": "GET", "path": "/x"})
    state = client.get("/admin/circuits").json()
    assert state["stub"] == "open"
    resp = client.post("/upstreams/stub/call", json={"method": "GET", "path": "/x"})
    assert resp.status_code == 503
    assert resp.json()["error"]["category"] == "circuit_open"
