"""幂等去重测试：并发同键只执行一次，重复提交复用结论。"""

from __future__ import annotations

import threading

from app.idempotency import DedupeStatus, IdempotencyStore
from app.models import GatewayResult


def _ok(cid: str) -> GatewayResult:
    return GatewayResult(
        correlation_id=cid, upstream="u", degraded=False, status="SUCCESS", data={"v": 1}
    )


def test_concurrent_same_key_single_execution():
    store = IdempotencyStore()
    outcomes: list[tuple[str, object]] = []
    barrier = threading.Barrier(6)

    def worker(i):
        barrier.wait()
        out = store.begin(f"key-{i % 2}")  # 6 线程竞争 2 个键
        if out.status is DedupeStatus.FIRST:
            store.finish(f"key-{i % 2}", _ok(f"cid-{i}"))
            outcomes.append(("FIRST", i))
        else:
            outcomes.append(("DUP", out.result))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    firsts = [o for o in outcomes if o[0] == "FIRST"]
    assert len(firsts) == 2, outcomes  # 每个键恰好一个 FIRST
    print("[idempotency] 6 线程 / 2 键, FIRST 数量:", len(firsts))


def test_duplicate_after_completion_reuses_result():
    store = IdempotencyStore()
    out1 = store.begin("k")
    assert out1.status is DedupeStatus.FIRST
    result = _ok("cid-original")
    store.finish("k", result)

    out2 = store.begin("k")
    assert out2.status is DedupeStatus.DUPLICATE
    assert out2.result is result
    print("[idempotency] 重复提交复用同一结论, cid:", result.correlation_id)


def test_in_flight_waiter_gets_finished_result():
    store = IdempotencyStore()
    holder = threading.Event()
    done = threading.Event()
    seen: list[object] = []

    def first():
        out = store.begin("k")
        assert out.status is DedupeStatus.FIRST
        holder.wait(1.0)
        store.finish("k", _ok("cid-first"))
        done.set()

    def waiter():
        out = store.begin("k")
        seen.append((out.status, out.result))

    t1 = threading.Thread(target=first)
    t1.start()
    # 确保 first 已登记
    while "k" not in store._inflight:
        pass
    t2 = threading.Thread(target=waiter)
    t2.start()
    holder.set()
    t1.join(); t2.join()
    assert len(seen) == 1
    assert seen[0][0] is DedupeStatus.DUPLICATE
    assert seen[0][1].correlation_id == "cid-first"
    print("[idempotency] 在途等待者最终状态:", seen[0][0].value)
