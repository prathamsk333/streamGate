"""confluent-kafka implementations of the gateway ports. No business logic lives here."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from confluent_kafka import (
    TIMESTAMP_NOT_AVAILABLE,
    Consumer,
    KafkaError,
    KafkaException,
    Message,
    Producer,
)
from confluent_kafka import TopicPartition as KafkaTopicPartition
from confluent_kafka.admin import AdminClient

from streamgate.gateway.ports import (
    DeliveryCallback,
    RebalanceCallback,
    TopicOffset,
    TopicPartition,
)
from streamgate.record import Headers, Record

log = logging.getLogger(__name__)
client_log = logging.getLogger("librdkafka")


def _on_error(err: KafkaError) -> None:
    if err.fatal():
        log.critical("fatal Kafka error: %s", err)
    else:
        log.warning("Kafka error: %s", err)


def _with_logging(config: Mapping[str, Any]) -> dict[str, Any]:
    return {"logger": client_log, "error_cb": _on_error, **config}


def _to_bytes(value: str | bytes | None) -> bytes | None:
    return value.encode() if isinstance(value, str) else value


def to_record(msg: Message) -> Record:
    ts_type, ts = msg.timestamp()
    raw = msg.headers() or ()
    pairs = raw.items() if isinstance(raw, dict) else raw
    headers = tuple((k, _to_bytes(v)) for k, v in pairs)
    return Record(
        topic=msg.topic() or "",
        partition=msg.partition() or 0,
        offset=msg.offset() or 0,
        key=_to_bytes(msg.key()),
        value=_to_bytes(msg.value()),
        headers=headers,
        timestamp_ms=None if ts_type == TIMESTAMP_NOT_AVAILABLE else ts,
    )


class KafkaConsumer:
    def __init__(self, config: Mapping[str, Any]) -> None:
        self._consumer = Consumer(_with_logging(config))

    def subscribe(
        self,
        topics: Sequence[str],
        on_assign: RebalanceCallback,
        on_revoke: RebalanceCallback,
    ) -> None:
        def assigned(_: Consumer, partitions: list[KafkaTopicPartition]) -> None:
            on_assign([TopicPartition(p.topic, p.partition) for p in partitions])

        def revoked(_: Consumer, partitions: list[KafkaTopicPartition]) -> None:
            on_revoke([TopicPartition(p.topic, p.partition) for p in partitions])

        self._consumer.subscribe(
            list(topics), on_assign=assigned, on_revoke=revoked, on_lost=revoked
        )

    def consume(self, max_messages: int, timeout_s: float) -> list[Record]:
        records = []
        for msg in self._consumer.consume(num_messages=max_messages, timeout=timeout_s):
            err = msg.error()
            if err is None:
                records.append(to_record(msg))
            elif err.code() != KafkaError._PARTITION_EOF:
                raise KafkaException(err)
        return records

    def commit(self, offsets: Sequence[TopicOffset]) -> None:
        if not offsets:
            return
        partitions = [KafkaTopicPartition(o.topic, o.partition, o.offset) for o in offsets]
        self._consumer.commit(offsets=partitions, asynchronous=False)

    def watermarks(self, tp: TopicPartition, *, cached: bool = True) -> tuple[int, int]:
        result = self._consumer.get_watermark_offsets(
            KafkaTopicPartition(tp.topic, tp.partition), timeout=10, cached=cached
        )
        if result is None:
            raise KafkaException(f"could not fetch watermarks for {tp.topic}/{tp.partition}")
        low, high = result
        return int(low), int(high)

    def partitions(self, topic: str) -> list[int]:
        metadata = self._consumer.list_topics(topic, timeout=15).topics.get(topic)
        if metadata is None or metadata.error is not None:
            raise TopicCheckError(f"topic {topic} does not exist")
        return sorted(metadata.partitions)

    def offsets_for_time(self, topic: str, partitions: Sequence[int], ts_ms: int) -> dict[int, int]:
        """First offset per partition whose timestamp is >= ts_ms, or -1 if there is none."""
        query = [KafkaTopicPartition(topic, p, ts_ms) for p in partitions]
        found = self._consumer.offsets_for_times(query, timeout=15)
        return {tp.partition: tp.offset for tp in found}

    def assign(self, offsets: Sequence[TopicOffset]) -> None:
        self._consumer.assign(
            [KafkaTopicPartition(o.topic, o.partition, o.offset) for o in offsets]
        )

    def close(self) -> None:
        self._consumer.close()


class KafkaProducer:
    def __init__(self, config: Mapping[str, Any]) -> None:
        self._producer = Producer(_with_logging(config))

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
        def report(err: KafkaError | None, _: Message) -> None:
            on_delivery(None if err is None else str(err))

        while True:
            try:
                self._producer.produce(
                    topic,
                    value=value,
                    key=key,
                    headers=list(headers),
                    timestamp=timestamp_ms or 0,
                    on_delivery=report,
                )
                break
            except BufferError:
                # Local queue is full; let librdkafka drain it and try again.
                self._producer.poll(0.1)
        self._producer.poll(0)

    def flush(self, timeout_s: float) -> int:
        return int(self._producer.flush(timeout_s))


class TopicCheckError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class TopicLayout:
    name: str
    input: str
    clean: str
    quarantine: str


def check_topics(admin_config: Mapping[str, Any], streams: Sequence[TopicLayout]) -> list[str]:
    """Fail if any topic is missing; return warnings about layouts that weaken guarantees."""
    metadata = AdminClient(_with_logging(admin_config)).list_topics(timeout=15)
    topics = metadata.topics
    wanted = [t for s in streams for t in (s.input, s.clean, s.quarantine)]
    missing = [t for t in wanted if t not in topics or topics[t].error is not None]
    if missing:
        raise TopicCheckError(
            f"missing topics: {', '.join(missing)}. StreamGate does not create topics; "
            "create them with the partition count and retention you want."
        )
    warnings = []
    for s in streams:
        source, clean = len(topics[s.input].partitions), len(topics[s.clean].partitions)
        if source != clean:
            warnings.append(
                f"stream {s.name}: {s.input} has {source} partitions but {s.clean} has {clean}; "
                "per-key ordering is kept, but keys will not map to the same partition numbers"
            )
    return warnings
