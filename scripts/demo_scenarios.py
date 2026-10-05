"""本地可验证的韧性治理场景演示。

运行：
    uv run python scripts/demo_scenarios.py

脚本使用脚本式桩上游与可控时钟，确定性复现：
1. 可重试失败后的安全重试成功；
2. 非幂等 POST 无幂等键时拒绝重试；携带幂等键才重试；
3. 熔断 关闭 -> 打开 -> 半开 -> 关闭 的时间窗口恢复；
4. 并发同键请求只产生一次副作用，重复提交命中结果重放；
5. 单次请求总预算耗尽即确定性降级，不再触达上游；
6. 按上游并发隔离：超限请求确定性拒绝，不计失败统计；
7. 只读状态视图：熔断状态、窗口失败数与启动以来累计指标。

运行时会打印状态迁移、重试决策与贯穿链路的 request_id 结构化日志。
"""
from __future__ import annotations

import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import BulkheadConfig, CircuitBreakerConfig, RetryConfig
from app.errors import ErrorCategory
from app.gateway import Conclusion, Gateway
from app.upstream import (
    StubUpstreamClient,
    UpstreamOutcome as O,
    UpstreamRequest,
    UpstreamResponse as R,
)


def fail() -> R:
    return R(O.RETRYABLE_FAILURE, 503)


def fatal() -> R:
    return R(O.FATAL_FAILURE, 400, None, "bad request")


def ok() -> R:
    return R(O.SUCCESS, 200, {"order_id": "A-1001"})


def banner(title: str) -> None:
    print("\n" + "=" * 12 + f" {title} " + "=" * 12)


def build_gateway(script: list[R], time_holder: dict, **cb_kw) -> tuple[Gateway, StubUpstreamClient]:
    client = StubUpstreamClient("orders")
    client.set_script(script)
    gw = Gateway(
        {"orders": client},
        breaker_config=CircuitBreakerConfig(
            failure_threshold=cb_kw.get("failure_threshold", 3),
            open_state_seconds=cb_kw.get("open_state_seconds", 10.0),
            failure_window_seconds=60.0,
        ),
        retry_config=RetryConfig(max_attempts=3, backoff_base_seconds=0.0),
        clock=lambda: time_holder["t"],
        sleeper=lambda _s: None,
        rng=lambda: 1.0,
    )
    return gw, client


def scenario_retry_then_success() -> None:
    banner("场景1：前两次可重试失败，第三次成功")
    t = {"t": 0.0}
    gw, client = build_gateway([fail(), fail(), ok()], t)
    result = gw.handle("orders", UpstreamRequest("GET", "/orders/1"))
    print(f"结论={result.conclusion.value} attempts={result.attempts} "
          f"request_id={result.request_id} payload={result.payload}")
    assert result.conclusion is Conclusion.SUCCESS and client.calls == 3


def scenario_non_idempotent_rules() -> None:
    banner("场景2：非幂等 POST 的重试规则")
    t = {"t": 0.0}
    gw, client = build_gateway([fail(), fail(), ok()], t)

    no_key = gw.handle("orders", UpstreamRequest("POST", "/orders"))
    print(f"[无幂等键] 结论={no_key.conclusion.value} 原因={no_key.reason} 调用次数={client.calls}")
    assert no_key.conclusion is Conclusion.REJECTED
    assert no_key.reason == ErrorCategory.NON_IDEMPOTENT_RETRY_FORBIDDEN.value
    assert client.calls == 1

    with_key = gw.handle("orders", UpstreamRequest("POST", "/orders"), idempotency_key="demo-key")
    print(f"[有幂等键] 结论={with_key.conclusion.value} attempts={with_key.attempts} 调用次数={client.calls}")
    assert with_key.conclusion is Conclusion.SUCCESS and client.calls == 3


def scenario_circuit_recovery() -> None:
    banner("场景3：熔断时间窗口自动恢复")
    t = {"t": 0.0}
    gw, client = build_gateway([fail(), fail(), fail(), ok(), ok()], t)
    req = UpstreamRequest("GET", "/orders/1")

    gw.handle("orders", req)
    gw.handle("orders", req)
    tripped = gw.handle("orders", req)
    print(f"第3次失败后: 结论={tripped.conclusion.value} 原因={tripped.reason} 熔断器={gw.breakers.get('orders').state.value}")

    t["t"] += 5.0
    during_open = gw.handle("orders", req)
    print(f"打开后 5s: 结论={during_open.conclusion.value} 上游调用次数={client.calls}（应仍为3）")

    t["t"] += 5.1
    probe = gw.handle("orders", req)
    print(f"打开后 10.1s 半开试探: 结论={probe.conclusion.value} 熔断器={gw.breakers.get('orders').state.value}")

    healed = gw.handle("orders", req)
    print(f"试探成功后: 结论={healed.conclusion.value} 熔断器={gw.breakers.get('orders').state.value}")
    assert gw.breakers.get("orders").state.value == "closed"


def scenario_concurrent_same_key() -> None:
    banner("场景4：16 个并发同键 POST，只有一次副作用")
    release = threading.Event()

    class BlockingClient(StubUpstreamClient):
        def call(self, request):
            release.wait(timeout=5)
            return super().call(request)

    # 计数式幂等存储：等全部 16 个请求都完成去重判定后，再放行首个上游调用，
    # 保证 1 个执行 + 15 个"处理中拒绝"的结论可复现。
    from app.idempotency import IdempotencyStore

    all_decided = threading.Event()
    counter = {"n": 0}
    counter_lock = threading.Lock()

    class CountingStore(IdempotencyStore):
        def acquire(self, key):
            result = super().acquire(key)
            with counter_lock:
                counter["n"] += 1
                if counter["n"] == 16:
                    all_decided.set()
            return result

    client = BlockingClient("orders")
    gw = Gateway(
        {"orders": client},
        breaker_config=CircuitBreakerConfig(failure_threshold=100),
        retry_config=RetryConfig(max_attempts=1),
        idempotency_store=CountingStore(),
        sleeper=lambda _s: None,
    )
    results: list = []
    barrier = threading.Barrier(16)

    def worker() -> None:
        barrier.wait()
        results.append(
            gw.handle("orders", UpstreamRequest("POST", "/orders"), idempotency_key="same-key-001")
        )

    threads = [threading.Thread(target=worker) for _ in range(16)]
    for th in threads:
        th.start()
    assert all_decided.wait(timeout=5)
    release.set()
    for th in threads:
        th.join()

    success = [r for r in results if r.conclusion is Conclusion.SUCCESS]
    rejected = [r for r in results if r.conclusion is Conclusion.REJECTED]
    print(f"成功={len(success)} 拒绝={len(rejected)} 上游副作用次数={client.calls}")
    print(f"拒绝原因={rejected[0].reason}（与重试原因可明确区分）")
    assert len(success) == 1 and len(rejected) == 15 and client.calls == 1

    replay = gw.handle("orders", UpstreamRequest("POST", "/orders"), idempotency_key="same-key-001")
    print(f"随后重复提交: 结论={replay.conclusion.value} 原因={replay.reason} "
          f"上游副作用次数={client.calls}（仍为1，命中结果重放）")


def scenario_budget_exhausted() -> None:
    banner("场景5：单次请求总预算耗尽即确定性降级")
    t = {"t": 0.0}
    # 退避基数 1s：第一次失败后需等 1s 再重试，但预算只有 0.5s。
    client = StubUpstreamClient("orders")
    client.set_script([fail(), ok()])

    def sleeper(seconds: float) -> None:
        t["t"] += seconds

    gw = Gateway(
        {"orders": client},
        breaker_config=CircuitBreakerConfig(failure_threshold=100),
        retry_config=RetryConfig(max_attempts=3, backoff_base_seconds=1.0),
        clock=lambda: t["t"],
        sleeper=sleeper,
        rng=lambda: 1.0,
    )
    req = UpstreamRequest("GET", "/orders/1")
    exhausted = gw.handle("orders", req, budget_seconds=0.5)
    print(f"预算 0.5s（退避需 1s）: 结论={exhausted.conclusion.value} 原因={exhausted.reason} "
          f"attempts={exhausted.attempts} 上游调用次数={client.calls}")
    assert exhausted.conclusion is Conclusion.DEGRADED
    assert exhausted.reason == ErrorCategory.BUDGET_EXHAUSTED.value
    assert client.calls == 1  # 预算耗尽，不再打上游

    enough = gw.handle("orders", req, budget_seconds=10.0)
    print(f"预算 10s: 结论={enough.conclusion.value} attempts={enough.attempts} "
          f"上游调用次数={client.calls}")
    assert enough.conclusion is Conclusion.SUCCESS


def scenario_concurrency_isolation() -> None:
    banner("场景6：按上游并发隔离，超限确定性拒绝")
    release = threading.Event()
    entered = threading.Event()

    class BlockingClient(StubUpstreamClient):
        def call(self, request):
            entered.set()
            release.wait(timeout=5)
            return super().call(request)

    client = BlockingClient("orders")
    gw = Gateway(
        {"orders": client},
        breaker_config=CircuitBreakerConfig(failure_threshold=100),
        retry_config=RetryConfig(max_attempts=1),
        bulkhead_config=BulkheadConfig(default_max_in_flight=1),
        sleeper=lambda _s: None,
    )
    first: list = []
    holder = threading.Thread(
        target=lambda: first.append(gw.handle("orders", UpstreamRequest("GET", "/orders/1")))
    )
    holder.start()
    assert entered.wait(timeout=5)

    rejected = gw.handle("orders", UpstreamRequest("GET", "/orders/1"))
    breaker = gw.breakers.get("orders")
    print(f"在途 1/1 时再发请求: 结论={rejected.conclusion.value} 原因={rejected.reason} "
          f"熔断窗口失败数={breaker.failure_count_in_window()}（不计入失败统计）")
    assert rejected.conclusion is Conclusion.REJECTED
    assert rejected.reason == ErrorCategory.CONCURRENCY_LIMIT_EXCEEDED.value
    assert breaker.failure_count_in_window() == 0

    release.set()
    holder.join()
    print(f"首个请求结束后: 结论={first[0].conclusion.value} 在途数={gw.bulkheads.in_flight('orders')}")
    assert first[0].conclusion is Conclusion.SUCCESS
    assert gw.bulkheads.in_flight("orders") == 0


def scenario_status_snapshot() -> None:
    banner("场景7：只读状态视图（熔断状态/窗口失败数/累计指标）")
    t = {"t": 0.0}
    gw, client = build_gateway(
        [fail(), ok(), fail(), fail(), fail(), fail()], t, failure_threshold=100
    )
    req = UpstreamRequest("GET", "/orders/1")
    gw.handle("orders", req)                                   # 重试 1 次后成功
    degraded = gw.handle("orders", req)                        # 重试耗尽降级
    rejected = gw.handle("orders", UpstreamRequest("POST", "/orders"))  # 无键拒绝
    assert degraded.conclusion is Conclusion.DEGRADED
    assert rejected.conclusion is Conclusion.REJECTED

    snapshot = gw.status_snapshot()["orders"]
    print(f"status_snapshot: {snapshot}")
    assert snapshot["breaker_state"] == "closed"
    assert snapshot["window_failures"] == 5  # 1 + 3 + 1 次失败
    assert snapshot["retries"] == 3          # 1 + 2 次实际重试
    assert snapshot["degraded"] == 1
    assert snapshot["rejected"] == 1
    print("对应 HTTP 只读入口：GET /admin/status")


def main() -> None:
    scenario_retry_then_success()
    scenario_non_idempotent_rules()
    scenario_circuit_recovery()
    scenario_concurrent_same_key()
    scenario_budget_exhausted()
    scenario_concurrency_isolation()
    scenario_status_snapshot()
    print("\n全部场景断言通过。")


if __name__ == "__main__":
    main()
