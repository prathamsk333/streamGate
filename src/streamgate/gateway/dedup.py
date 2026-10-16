from __future__ import annotations

import heapq
from collections.abc import Iterable


class _Window:
    __slots__ = ("expiry", "heap")

    def __init__(self) -> None:
        self.expiry: dict[str, int] = {}
        self.heap: list[tuple[int, str]] = []

    def evict(self, now_ms: int, max_keys: int) -> None:
        while self.heap and (self.heap[0][0] <= now_ms or len(self.expiry) > max_keys):
            expires, key = heapq.heappop(self.heap)
            if self.expiry.get(key) == expires:
                del self.expiry[key]


class MemoryDedupStore:
    """Per-partition windows of recently seen keys.

    State is local to this instance and lost on restart; partitions are dropped when they
    are revoked, since another instance owns them from then on.
    """

    def __init__(self, max_keys_per_partition: int = 1_000_000) -> None:
        self.max_keys = max_keys_per_partition
        self._windows: dict[int, _Window] = {}

    def seen_or_add(self, partition: int, key: str, now_ms: int, window_ms: int) -> bool:
        window = self._windows.get(partition)
        if window is None:
            window = self._windows[partition] = _Window()
        window.evict(now_ms, self.max_keys)
        expires = window.expiry.get(key)
        if expires is not None and expires > now_ms:
            return True
        expires = now_ms + window_ms
        window.expiry[key] = expires
        heapq.heappush(window.heap, (expires, key))
        window.evict(now_ms, self.max_keys)
        return False

    def drop(self, partitions: Iterable[int]) -> None:
        for partition in partitions:
            self._windows.pop(partition, None)

    def __len__(self) -> int:
        return sum(len(w.expiry) for w in self._windows.values())
