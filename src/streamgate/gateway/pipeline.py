from __future__ import annotations

import logging
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from streamgate import __version__
from streamgate.codec import Codec
from streamgate.config import PipelineSettings
from streamgate.envelope import EnvelopeMeta
from streamgate.evaluation import evaluate
from streamgate.gateway.ports import (
    ConsumerPort,
    DeliveryCallback,
    ProducerPort,
    TopicOffset,
    TopicPartition,
)
from streamgate.gateway.router import Stream, route
from streamgate.health import Health
from streamgate.metrics import Metrics
from streamgate.record import Record
from streamgate.rules.loader import RuleWatcher
from streamgate.timeutil import now_ms

log = logging.getLogger(__name__)


class ProduceError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Failure:
    topic: str
    error: str


class Pipeline:
    """consume → evaluate → produce → flush → commit, one batch at a time.

    Offsets are committed only after every record of the batch has been acknowledged by the
    brokers, so a crash at any point re-delivers records instead of losing them.
    """

    def __init__(
        self,
        *,
        consumer: ConsumerPort,
        producer: ProducerPort,
        streams: Sequence[Stream],
        codec: Codec,
        settings: PipelineSettings,
        metrics: Metrics,
        health: Health,
        instance_id: str,
        watcher: RuleWatcher | None = None,
        reload_interval_s: float = 0,
        commit: bool = True,
        clock: Callable[[], int] = now_ms,
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._streams = {s.input: s for s in streams}
        self._codec = codec
        self._settings = settings
        self._metrics = metrics
        self._health = health
        self._instance = instance_id
        self._watcher = watcher
        self._reload_interval = reload_interval_s
        self._commit_enabled = commit
        self._clock = clock
        self._failures: list[Failure] = []
        self._assigned: set[TopicPartition] = set()
        self._last_reload = time.monotonic()
        self._stopping = False

    def stop(self) -> None:
        self._stopping = True

    @property
    def assigned(self) -> frozenset[TopicPartition]:
        return frozenset(self._assigned)

    def run(self) -> None:
        self._consumer.subscribe(list(self._streams), self._on_assign, self._on_revoke)
        log.info("started", extra={"instance": self._instance, "topics": sorted(self._streams)})
        try:
            while not self._stopping:
                records = self._consumer.consume(
                    self._settings.batch_size, self._settings.poll_timeout_s
                )
                if records:
                    self.process(records)
                self._maybe_reload()
                self._health.beat()
        finally:
            self._health.ready = False
            log.info("shutting down")
            self._consumer.close()
            self._producer.flush(self._settings.flush_timeout_s)

    def process(self, records: Sequence[Record]) -> None:
        started = time.perf_counter()
        now = self._clock()
        positions: dict[TopicPartition, int] = {}
        self._failures.clear()

        for record in records:
            stream = self._streams[record.topic]
            ruleset = stream.rules()
            ev = evaluate(
                record,
                ruleset,
                self._codec,
                stream.dedup,
                now,
                reject_tombstones=self._settings.reject_tombstones,
            )
            meta = EnvelopeMeta(self._instance, ruleset.id, __version__, now)
            out = route(stream, ev, self._codec, meta, warn_header=self._settings.warn_header)
            self._producer.produce(
                out.topic,
                key=out.key,
                value=out.value,
                headers=out.headers,
                timestamp_ms=out.timestamp_ms,
                on_delivery=self._on_delivery(out.topic),
            )
            self._metrics.observe(stream.name, ev.verdict)
            positions[TopicPartition(record.topic, record.partition)] = record.offset + 1

        pending = self._producer.flush(self._settings.flush_timeout_s)
        if pending or self._failures:
            self._delivery_failed(pending)

        if self._commit_enabled:
            self._consumer.commit(
                [TopicOffset(tp.topic, tp.partition, o) for tp, o in positions.items()]
            )
        self._metrics.batch(len(records), time.perf_counter() - started)
        self._update_lag(positions)

    def _on_delivery(self, topic: str) -> DeliveryCallback:
        def report(error: str | None) -> None:
            if error is not None:
                self._failures.append(Failure(topic, error))
                self._metrics.produce_errors.labels(topic).inc()

        return report

    def _delivery_failed(self, pending: int) -> None:
        first = self._failures[0] if self._failures else None
        summary = f"{len(self._failures)} deliveries failed, {pending} still pending" + (
            f"; first error on {first.topic}: {first.error}" if first else ""
        )
        if self._settings.on_produce_error == "exit":
            # Not committing means the batch is redelivered after restart: nothing is lost.
            raise ProduceError(summary)
        log.error("%s; committing anyway (on_produce_error=log)", summary)

    def _update_lag(self, positions: dict[TopicPartition, int]) -> None:
        for tp, position in positions.items():
            _, high = self._consumer.watermarks(tp)
            if high >= 0:
                self._metrics.set_lag(self._streams[tp.topic].name, tp.partition, high - position)

    def _on_assign(self, partitions: list[TopicPartition]) -> None:
        self._assigned.update(partitions)
        self._report_assignment()
        self._health.ready = True
        log.info("partitions assigned", extra={"partitions": _names(partitions)})

    def _on_revoke(self, partitions: list[TopicPartition]) -> None:
        self._assigned.difference_update(partitions)
        for tp in partitions:
            stream = self._streams.get(tp.topic)
            if stream is not None:
                stream.dedup.drop([tp.partition])
                self._metrics.forget_partition(stream.name, tp.partition)
        self._report_assignment()
        log.info("partitions revoked", extra={"partitions": _names(partitions)})

    def _report_assignment(self) -> None:
        for topic, stream in self._streams.items():
            owned = sum(1 for tp in self._assigned if tp.topic == topic)
            self._metrics.assigned.labels(stream.name).set(owned)

    def _maybe_reload(self) -> None:
        if self._watcher is None or self._reload_interval <= 0:
            return
        if time.monotonic() - self._last_reload < self._reload_interval:
            return
        self._last_reload = time.monotonic()
        for result in self._watcher.poll():
            self._metrics.reloads.labels(result.stream, "ok" if result.ok else "error").inc()
            if result.ruleset is not None:
                self._metrics.active_ruleset(result.ruleset)
                log.info(
                    "rules reloaded", extra={"stream": result.stream, "ruleset": result.ruleset.id}
                )
            else:
                log.error(
                    "rules not reloaded, keeping previous: %s",
                    result.error,
                    extra={"stream": result.stream},
                )


def _names(partitions: list[TopicPartition]) -> list[str]:
    return [f"{tp.topic}/{tp.partition}" for tp in sorted(partitions)]
