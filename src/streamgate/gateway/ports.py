"""The narrow slice of Kafka the pipeline depends on.

Real adapters live in `gateway.kafka`; in-memory fakes in `tests/fakes.py`.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol

from streamgate.record import Headers, Record


@dataclass(frozen=True, slots=True, order=True)
class TopicPartition:
    topic: str
    partition: int


@dataclass(frozen=True, slots=True)
class TopicOffset:
    topic: str
    partition: int
    offset: int


RebalanceCallback = Callable[[list[TopicPartition]], None]
DeliveryCallback = Callable[[str | None], None]


class ConsumerPort(Protocol):
    def subscribe(
        self,
        topics: Sequence[str],
        on_assign: RebalanceCallback,
        on_revoke: RebalanceCallback,
    ) -> None: ...

    def consume(self, max_messages: int, timeout_s: float) -> list[Record]: ...

    def commit(self, offsets: Sequence[TopicOffset]) -> None: ...

    def watermarks(self, tp: TopicPartition, *, cached: bool = True) -> tuple[int, int]: ...

    def close(self) -> None: ...


class ProducerPort(Protocol):
    def produce(
        self,
        topic: str,
        *,
        key: bytes | None,
        value: bytes | None,
        headers: Headers,
        timestamp_ms: int | None,
        on_delivery: DeliveryCallback,
    ) -> None: ...

    def flush(self, timeout_s: float) -> int:
        """Block until queued messages are delivered; return how many are still pending."""
        ...
