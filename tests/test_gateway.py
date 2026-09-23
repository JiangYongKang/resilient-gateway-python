"""网关端到端编排测试（本地桩上游）。

覆盖：成功、重试恢复、非幂等拒绝重试、业务拒绝归一化、
熔断打开降级与时间恢复、同键并发单次副作用、同键重复复用、
关联标识贯穿、未知上游确定性降级。
"""

from __future__ import annotations

import threading
import time

import pytest

from app.config import ResilienceConfig
from app.errors import UpstreamCallError
from app.gateway import Gateway
from app.models import UpstreamRequest
from app.upstream import StubUpstreamClient


@pytest.fixture
def config():
    return ResilienceConfig(
        failure_threshold=3,
        window_seconds=10,
        open_recovery_seconds=0.2,
        half_open_success_threshold=1,
        half_open_failure_threshold=1,
        max_attempts=3,
        backoff_base_seconds=0.001,
        max_backoff_seconds=0.01,
    )


def test_success_passthrough(config):
    g = Gateway({"a": StubUpstreamClient("a", behavior="success")}, config)
    r = g.invoke(UpstreamRequest(upstream="a", method="POST", idempotency_key="k1"))
    assert r.status == "SUCCESS" and r.attempts == 1 and not r.degraded
    assert r.correlation_id and r.error_category is None
    print(f"[flow] cid={r.correlation_id} status={r.status} attempts={r.attempts}")


def test_retry_then_recovery_with_idempotency_key(config):
    stub = StubUpstreamClient("a", behaviors=[503, 503, 200])
    g = Gateway({"a": stub}, config)
    r = g.invoke(UpstreamRequest(upstream="a", method="POST", idempotency_key="k2"))
    assert r.status == "SUCCESS" and r.attempts == 3
    assert stub.call_count == 3
    print(f"[retry] cid={r.correlation_id} 第3次尝试成功, 上游调用={stub.call_count}")


def test_non_idempotent_post_without_key_not_retried(config):
    stub = StubUpstreamClient("a", behavior="error")
    g = Gateway({"a": stub}, config)
    r = g.invoke(UpstreamRequest(upstream="a", method="POST"))
    assert r.degraded and r.attempts == 1
    assert r.reason == "non_idempotent_without_idempotency_key"
    assert r.error_category == "upstream_unavailable"
    assert stub.call_count == 1  # 拒绝重试 -> 只有一次副作用
    print(f"[retry] cid={r.correlation_id} 拒绝重试原因={r.reason}")


def test_get_retries_until_max_and_degrades(config):
    stub = StubUpstreamClient("a", behavior="timeout")
    g = Gateway({"a": stub}, config)
    r = g.invoke(UpstreamRequest(upstream="a", method="GET"))
    assert r.degraded and r.attempts == 3
    assert r.reason == "max_attempts_reached"
    assert r.error_category == "upstream_timeout"
    print(f"[flow] cid={r.correlation_id} 耗尽重试降级 category={r.error_category}")


def test_upstream_rejection_is_distinct_non_degraded(config):
    g = Gateway({"a": StubUpstreamClient("a", behavior="reject")}, config)
    r = g.invoke(UpstreamRequest(upstream="a", method="POST", idempotency_key="k4"))
    assert not r.degraded and r.status == "ERROR"
    assert r.error_category == "upstream_rejected"
    assert r.reason == "non_retryable_failure"
    print(f"[normalize] cid={r.correlation_id} 业务拒绝 category={r.error_category}")


def test_unknown_upstream_deterministic_fallback(config):
    g = Gateway({}, config)
    r1 = g.invoke(UpstreamRequest(upstream="ghost"), correlation_id="cid-x")
    r2 = g.invoke(UpstreamRequest(upstream="ghost"), correlation_id="cid-y")
    assert r1.degraded and r1.reason == "upstream_not_configured"
    assert r1.data == r2.data  # 确定性降级负载
    print(f"[fallback] cid={r1.correlation_id} 确定性降级 data={r1.data}")


def test_circuit_opens_then_blocks_then_recovers(config):
    g = Gateway({"a": StubUpstreamClient("a", behavior="error")}, config)
    for i in range(3):
        g.invoke(UpstreamRequest(upstream="a", method="GET"))
    assert g.breakers.get("a").state.value == "OPEN"

    blocked = g.invoke(UpstreamRequest(upstream="a", method="GET"), correlation_id="cid-block")
    assert blocked.degraded and blocked.reason == "circuit_open" and blocked.attempts == 0
    print(f"[circuit] cid={blocked.correlation_id} 熔断打开直接降级 reason={blocked.reason}")

    # 恢复时间后上游变健康 -> 半开试探成功 -> 关闭
    g.clients["a"] = StubUpstreamClient("a", behavior="success")
    time.sleep(0.25)
    recovered = g.invoke(UpstreamRequest(upstream="a", method="GET"), correlation_id="cid-rec")
    assert recovered.status == "SUCCESS"
    assert g.breakers.get("a").state.value == "CLOSED"
    print(f"[circuit] cid={recovered.correlation_id} 试探成功恢复 CLOSED")


def test_circuit_probe_failure_reopens(config):
    g = Gateway({"a": StubUpstreamClient("a", behavior="error")}, config)
    for _ in range(3):
        g.invoke(UpstreamRequest(upstream="a", method="GET"))
    time.sleep(0.25)
    r = g.invoke(UpstreamRequest(upstream="a", method="GET"))
    assert g.breakers.get("a").state.value == "OPEN"
    assert r.degraded
    print(f"[circuit] cid={r.correlation_id} 半开试探失败重新 OPEN")


def test_concurrent_same_key_single_side_effect(config):
    stub = StubUpstreamClient("a", behavior="success", sleep=time.sleep)
    g = Gateway({"a": stub}, config)
    results: list = []

    def worker():
        results.append(
            g.invoke(
                UpstreamRequest(upstream="a", method="POST", idempotency_key="same"),
                correlation_id=None,
            )
        )

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert stub.call_count == 1
    assert len(stub.side_effects) == 1
    assert all(r.status == "SUCCESS" for r in results)
    cids = {r.correlation_id for r in results}
    assert len(cids) == 8  # 关联标识各自独立且贯穿
    print(f"[idempotency] 8 并发同键上游调用={stub.call_count} cids={sorted(cids)[:3]}...")


def test_duplicate_key_after_failure_reuses_and_no_extra_call(config):
    stub = StubUpstreamClient("a", behavior="error")
    g = Gateway({"a": stub}, config)
    r1 = g.invoke(UpstreamRequest(upstream="a", method="POST", idempotency_key="dup"),
                  correlation_id="cid-1")
    assert r1.degraded and stub.call_count == 3
    r2 = g.invoke(UpstreamRequest(upstream="a", method="POST", idempotency_key="dup"),
                  correlation_id="cid-2")
    assert r2.degraded
    assert r2.reason == r1.reason and r2.attempts == r1.attempts and r2.data == r1.data
    assert r2.correlation_id == "cid-2"  # 复用结论但 cid 为当前请求
    assert stub.call_count == 3  # 无新增副作用
    print(f"[idempotency] cid={r2.correlation_id} 复用降级结论 上游调用仍为={stub.call_count}")


def test_internal_exception_normalized_not_leaked(config):
    class ExplodingClient:
        call_count = 0

        def call(self, request):
            self.call_count += 1
            raise RuntimeError("secret internal detail /tmp/path")

    g = Gateway({"a": ExplodingClient()}, config)
    r = g.invoke(UpstreamRequest(upstream="a", method="GET"), correlation_id="cid-secret")
    assert r.degraded and r.error_category == "internal_error"
    assert "secret" not in str(r.data) and "/tmp/path" not in str(r.data)
    print(f"[normalize] cid={r.correlation_id} 内部异常归一化 data={r.data}")
