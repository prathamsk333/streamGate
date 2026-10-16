from __future__ import annotations

from dataclasses import dataclass

Headers = tuple[tuple[str, bytes | None], ...]


@dataclass(frozen=True, slots=True)
class Record:
    """A message as consumed from Kafka. Bytes are kept untouched for pass-through."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    headers: Headers = ()
    timestamp_ms: int | None = None

    @property
    def coordinates(self) -> str:
        return f"{self.topic}/{self.partition}/{self.offset}"
