"""本地可验证的韧性治理场景演示。

运行：
    uv run python scripts/demo_scenarios.py

脚本使用脚本式桩上游与可控时钟，确定性复现：
1. 可重试失败后的安全重试成功；
2. 非幂等 POST 无幂等键时拒绝重试；携带幂等键才重试；
3. 熔断 关闭 -> 打开 -> 半开 -> 关闭 的时间窗口恢复；
4. 并发同键请求只产生一次副作用，重复提交命中结果重放；
5. 单次转发总时间预算：重试等待计入预算，耗尽即降级且原因可区分；
6. 按上游并发隔离：超限请求直接拒绝，不打上游、不计失败、不动幂等键；
7. 只读运行指标：重试/降级/拒绝累计计数。

运行时会打印状态迁移、重试决策与贯穿链路的 request_id 结构化日志。
"""
from __future__ import annotations

import sys
import threading
from pathlib import Path

# 允许从仓库根目录直接运行：uv run python scripts/demo_scenarios.py
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

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
    banner("场景5：总时间预算耗尽，重试等待也计入预算")
    t = {"t": 0.0}

    def sleeper(seconds: float) -> None:
        t["t"] += seconds  # 假时钟：睡眠直接推进时间

    client = StubUpstreamClient("orders")
    client.set_script([fail(), fail(), ok()])
    gw = Gateway(
        {"orders": client},
        breaker_config=CircuitBreakerConfig(failure_threshold=100),
        retry_config=RetryConfig(
            max_attempts=3, backoff_base_seconds=0.5, backoff_max_seconds=10.0
        ),
        clock=lambda: t["t"],
        sleeper=sleeper,
        rng=lambda: 1.0,
    )
    result = gw.handle(
        "orders", UpstreamRequest("GET", "/orders/1"), budget_seconds=0.6
    )
    print(f"预算 0.6s: 结论={result.conclusion.value} 原因={result.reason} "
          f"attempts={result.attempts} 上游调用={client.calls} 已耗时={t['t']}s")
    assert result.conclusion is Conclusion.DEGRADED
    assert result.reason == ErrorCategory.BUDGET_EXHAUSTED.value
    assert client.calls == 2 and t["t"] == 0.6  # 第 3 次尝试未发生，且未超睡


def scenario_bulkhead_rejection() -> None:
    banner("场景6：按上游并发隔离，超限直接拒绝")
    release = threading.Event()

    class BlockingClient(StubUpstreamClient):
        def call(self, request):
            release.wait(timeout=5)
            return super().call(request)

    client = BlockingClient("orders")
    gw = Gateway(
        {"orders": client},
        retry_config=RetryConfig(max_attempts=1),
        bulkhead_config=BulkheadConfig(default_max_concurrent=2),
        sleeper=lambda _s: None,
    )
    results: list = []
    barrier = threading.Barrier(6)

    def worker() -> None:
        barrier.wait()
        results.append(gw.handle("orders", UpstreamRequest("GET", "/orders/1")))

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for th in threads:
        th.start()
    import time

    time.sleep(0.05)  # 等 2 个在途请求进入上游
    release.set()
    for th in threads:
        th.join()

    ok_n = len([r for r in results if r.conclusion is Conclusion.SUCCESS])
    rej = [r for r in results if r.conclusion is Conclusion.REJECTED]
    print(f"限 2 并发 6 请求: 成功={ok_n} 拒绝={len(rej)} 原因={rej[0].reason} "
          f"上游调用={client.calls} 结束后在途={gw.bulkheads.in_flight('orders')}")
    assert ok_n == 2 and len(rej) == 4 and client.calls == 2
    assert rej[0].reason == ErrorCategory.CONCURRENCY_LIMIT_EXCEEDED.value
    assert gw.bulkheads.in_flight("orders") == 0


def scenario_metrics_snapshot() -> None:
    banner("场景7：只读运行指标累计")
    t = {"t": 0.0}
    gw, client = build_gateway(
        [fail(), fail(), ok(), fail(), fail(), fail(), fail()],
        t,
        failure_threshold=100,
    )
    gw.handle("orders", UpstreamRequest("GET", "/orders/1"))  # 2 次重试后成功
    gw.handle("orders", UpstreamRequest("GET", "/orders/2"))  # 重试耗尽降级
    gw.handle("orders", UpstreamRequest("POST", "/orders"))   # 非幂等无键拒绝
    snap = gw.metrics.snapshot("orders")
    breaker = gw.breakers.get("orders")
    print(f"指标: retries={snap['retries']} degraded={snap['degraded']} "
          f"rejected={snap['rejected']} 熔断器={breaker.state.value} "
          f"窗口失败数={breaker.failure_count()}")
    assert snap == {"retries": 4, "degraded": 1, "rejected": 1}


def main() -> None:
    scenario_retry_then_success()
    scenario_non_idempotent_rules()
    scenario_circuit_recovery()
    scenario_concurrent_same_key()
    scenario_budget_exhausted()
    scenario_bulkhead_rejection()
    scenario_metrics_snapshot()
    print("\n全部场景断言通过。")


if __name__ == "__main__":
    main()
