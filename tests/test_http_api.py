"""HTTP 层测试：原有 GET / 行为保持不变，新增转发接口行为确定。"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import main
from app.config import ResilienceConfig
from app.gateway import Gateway
from app.upstream import StubUpstreamClient


@pytest.fixture
def client():
    # 每个测试注入全新网关，避免熔断/幂等状态跨用例污染
    gw = Gateway(
        {
            "upstream-a": StubUpstreamClient("upstream-a", behavior="success"),
            "upstream-flaky": StubUpstreamClient("upstream-flaky", behavior="error"),
        },
        ResilienceConfig(max_attempts=2, backoff_base_seconds=0.001),
    )
    main._get_gateway.cache_clear()
    main._get_gateway()  # 先建立缓存项再替换
    # 直接让缓存持有测试网关
    main._get_gateway.cache_clear()
    # monkeypatch 风格：把缓存函数替换为返回 gw
    original = main._get_gateway
    main._get_gateway = lambda: gw
    try:
        yield TestClient(main.app)
    finally:
        main._get_gateway = original
        main._get_gateway.cache_clear()


def test_legacy_root_behavior_unchanged(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert resp.json() == {"Hello": "World"}  # 原对外行为不破坏


def test_invoke_success_returns_200(client):
    resp = client.post(
        "/gateway/invoke",
        json={"upstream": "upstream-a", "method": "POST", "idempotency_key": "k1"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "SUCCESS" and body["correlation_id"]


def test_invoke_degraded_returns_503(client):
    resp = client.post("/gateway/invoke", json={"upstream": "upstream-flaky", "method": "POST"})
    assert resp.status_code == 503
    body = resp.json()
    assert body["degraded"] is True
    assert body["error_category"] == "upstream_unavailable"
    assert body["reason"] == "non_idempotent_without_idempotency_key"


def test_invoke_unknown_upstream_returns_503(client):
    resp = client.post("/gateway/invoke", json={"upstream": "ghost"})
    assert resp.status_code == 503
    assert resp.json()["reason"] == "upstream_not_configured"


def test_circuits_snapshot_endpoint(client):
    resp = client.get("/gateway/circuits")
    assert resp.status_code == 200
    assert isinstance(resp.json(), dict)
