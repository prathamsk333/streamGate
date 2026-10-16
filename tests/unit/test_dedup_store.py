from streamgate.gateway.dedup import MemoryDedupStore


def test_window_expiry() -> None:
    store = MemoryDedupStore()
    assert not store.seen_or_add(0, "k", 0, 100)
    assert store.seen_or_add(0, "k", 99, 100)
    assert not store.seen_or_add(0, "k", 100, 100)
    assert len(store) == 1


def test_expired_keys_are_evicted() -> None:
    store = MemoryDedupStore()
    for i in range(10):
        store.seen_or_add(0, f"k{i}", 0, 10)
    store.seen_or_add(0, "fresh", 50, 10)
    assert len(store) == 1


def test_capacity_evicts_soonest_expiring() -> None:
    store = MemoryDedupStore(max_keys_per_partition=2)
    store.seen_or_add(0, "a", 0, 100)
    store.seen_or_add(0, "b", 0, 200)
    store.seen_or_add(0, "c", 0, 300)
    assert len(store) == 2
    assert not store.seen_or_add(0, "a", 1, 100)


def test_drop_partitions() -> None:
    store = MemoryDedupStore()
    store.seen_or_add(0, "k", 0, 100)
    store.seen_or_add(1, "k", 0, 100)
    store.drop([0, 7])
    assert not store.seen_or_add(0, "k", 1, 100)
    assert store.seen_or_add(1, "k", 1, 100)
