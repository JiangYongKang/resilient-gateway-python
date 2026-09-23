"""幂等存储单测：并发同键去重、结果复用、失败释放。"""
import threading

import pytest

from app.idempotency import DedupStatus, IdempotencyStore


def test_acquire_complete_replay():
    store = IdempotencyStore()
    first = store.acquire("key-1234")
    assert first.status is DedupStatus.ACQUIRED
    assert store.acquire("key-1234").status is DedupStatus.IN_PROGRESS
    store.complete("key-1234", {"id": 1})
    replay = store.acquire("key-1234")
    assert replay.status is DedupStatus.COMPLETED
    assert replay.payload == {"id": 1}


def test_failure_releases_key_for_resubmission():
    store = IdempotencyStore()
    assert store.acquire("k").status is DedupStatus.ACQUIRED
    store.fail("k")
    assert store.acquire("k").status is DedupStatus.ACQUIRED


def test_fail_after_complete_does_not_drop_result():
    store = IdempotencyStore()
    store.acquire("k")
    store.complete("k", "ok")
    store.fail("k")
    assert store.acquire("k").status is DedupStatus.COMPLETED


def test_concurrent_same_key_only_one_acquired():
    store = IdempotencyStore()
    statuses = []
    barrier = threading.Barrier(32)

    def worker():
        barrier.wait()
        statuses.append(store.acquire("hot-key").status)

    threads = [threading.Thread(target=worker) for _ in range(32)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    acquired = [s for s in statuses if s is DedupStatus.ACQUIRED]
    rejected = [s for s in statuses if s is DedupStatus.IN_PROGRESS]
    assert len(acquired) == 1
    assert len(rejected) == 31


@pytest.mark.parametrize("n", [2, 8])
def test_distinct_keys_independent(n):
    store = IdempotencyStore()
    for i in range(n):
        assert store.acquire(f"key-{i}").status is DedupStatus.ACQUIRED
