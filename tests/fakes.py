from __future__ import annotations

from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from streamgate.gateway.ports import (
    DeliveryCallback,
    RebalanceCallback,
    TopicOffset,
    TopicPartition,
)
from streamgate.record import Headers, Record

Step = list[Record] | Callable[[], object]


class FakeConsumer:
    """Hands out scripted batches. A callable in the script runs instead of returning records."""

    def __init__(self, script: Sequence[Step], partitions: Sequence[TopicPartition]) -> None:
        self.script: deque[Step] = deque(script)
        self.partitions = list(partitions)
        self.commits: list[list[TopicOffset]] = []
        self.high: dict[TopicPartition, int] = {}
        self.closed = False
        self.on_drained: Callable[[], None] | None = None
        self._assigned = False
        self._on_assign: RebalanceCallback = lambda _: None
        self._on_revoke: RebalanceCallback = lambda _: None

    def subscribe(
        self, topics: Sequence[str], on_assign: RebalanceCallback, on_revoke: RebalanceCallback
    ) -> None:
        self.topics = list(topics)
        self._on_assign, self._on_revoke = on_assign, on_revoke

    def consume(self, max_messages: int, timeout_s: float) -> list[Record]:
        if not self._assigned:
            self._assigned = True
            self._on_assign(self.partitions)
        if self.script:
            step = self.script.popleft()
            if callable(step):
                step()
                return []
            return step
        if self.on_drained is not None:
            self.on_drained()
        return []

    def revoke(self, partitions: list[TopicPartition]) -> None:
        self._on_revoke(partitions)

    def commit(self, offsets: Sequence[TopicOffset]) -> None:
        self.commits.append(list(offsets))

    def watermarks(self, tp: TopicPartition, *, cached: bool = True) -> tuple[int, int]:
        return 0, self.high.get(tp, -1)

    def close(self) -> None:
        self.closed = True
        self._on_revoke(self.partitions)


@dataclass(frozen=True)
class Sent:
    topic: str
    key: bytes | None
    value: bytes | None
    headers: Headers
    timestamp_ms: int | None


class FakeProducer:
    def __init__(self) -> None:
        self.sent: list[Sent] = []
        self.failing: set[str] = set()
        self.stuck = 0
        self._waiting: list[tuple[str, DeliveryCallback]] = []

    def produce(
        self,
        topic: str,
        *,
        key: bytes | None,
        value: bytes | None,
        headers: Headers,
        timestamp_ms: int | None,
        on_delivery: DeliveryCallback,
    ) -> None:
        self.sent.append(Sent(topic, key, value, headers, timestamp_ms))
        self._waiting.append((topic, on_delivery))

    def flush(self, timeout_s: float) -> int:
        for topic, callback in self._waiting:
            callback("broker said no" if topic in self.failing else None)
        self._waiting.clear()
        return self.stuck

    def to(self, topic: str) -> list[Sent]:
        return [s for s in self.sent if s.topic == topic]


class FakeQuarantine:
    """A partitioned topic that supports the reads replay needs."""

    def __init__(self, topic: str, partitions: dict[int, list[Record]]) -> None:
        self.topic = topic
        self.data = partitions
        self.positions: dict[int, int] = {}
        self.closed = False

    def partitions(self, topic: str) -> list[int]:
        return sorted(self.data)

    def offsets_for_time(self, topic: str, partitions: Sequence[int], ts_ms: int) -> dict[int, int]:
        found = {}
        for p in partitions:
            hits = [r.offset for r in self.data[p] if (r.timestamp_ms or 0) >= ts_ms]
            found[p] = hits[0] if hits else -1
        return found

    def watermarks(self, tp: TopicPartition, *, cached: bool = True) -> tuple[int, int]:
        records = self.data[tp.partition]
        return (records[0].offset, records[-1].offset + 1) if records else (0, 0)

    def assign(self, offsets: Sequence[TopicOffset]) -> None:
        self.positions = {o.partition: o.offset for o in offsets}

    def consume(self, max_messages: int, timeout_s: float) -> list[Record]:
        batch: list[Record] = []
        for p, position in list(self.positions.items()):
            for record in self.data[p]:
                if record.offset >= position and len(batch) < max_messages:
                    batch.append(record)
                    self.positions[p] = record.offset + 1
        return batch

    def close(self) -> None:
        self.closed = True
