"""Re-run quarantined records through the current rules and release the ones that now pass.

Replay reads a bounded slice of the quarantine topic (from a point in time up to its end
when the replay started), never commits offsets, and can be re-run safely; re-running
releases the same records again, so expect duplicates in the clean topic if you do.
"""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Literal, Protocol

from streamgate import __version__
from streamgate.codec import Codec
from streamgate.envelope import (
    H_REPLAYED,
    H_SOURCE,
    Envelope,
    EnvelopeError,
    EnvelopeMeta,
    decode_envelope,
    strip_reserved,
)
from streamgate.evaluation import Evaluation, evaluate
from streamgate.gateway.dedup import MemoryDedupStore
from streamgate.gateway.ports import ProducerPort, TopicOffset, TopicPartition
from streamgate.gateway.router import Stream, route
from streamgate.record import Record
from streamgate.rules.base import Outcome, Verdict
from streamgate.rules.engine import RuleSet
from streamgate.timeutil import format_iso, now_ms

log = logging.getLogger(__name__)

AsOf = Literal["original", "now"]


class ReplaySource(Protocol):
    def partitions(self, topic: str) -> list[int]: ...

    def offsets_for_time(
        self, topic: str, partitions: Sequence[int], ts_ms: int
    ) -> dict[int, int]: ...

    def watermarks(self, tp: TopicPartition, *, cached: bool = True) -> tuple[int, int]: ...

    def assign(self, offsets: Sequence[TopicOffset]) -> None: ...

    def consume(self, max_messages: int, timeout_s: float) -> list[Record]: ...

    def close(self) -> None: ...


class ReplayError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ReplayOptions:
    since_ms: int | None = None
    until_ms: int | None = None
    as_of: AsOf = "original"
    dry_run: bool = False
    limit: int | None = None
    requarantine: bool = False
    release_duplicates: bool = False
    idle_timeout_s: float = 30.0
    flush_timeout_s: float = 30.0


@dataclass(frozen=True, slots=True)
class Replayed:
    envelope: Envelope
    evaluation: Evaluation

    @property
    def outcome(self) -> Outcome:
        return self.evaluation.verdict.outcome


@dataclass
class ReplaySummary:
    scanned: int = 0
    released: int = 0
    still_rejected: int = 0
    malformed: int = 0
    by_rule: Counter[str] = field(default_factory=Counter)


class Replayer:
    def __init__(
        self,
        *,
        source: ReplaySource,
        producer: ProducerPort,
        stream: Stream,
        codec: Codec,
        instance_id: str,
        clock: Callable[[], int] = now_ms,
    ) -> None:
        self._source = source
        self._producer = producer
        self._stream = stream
        self._codec = codec
        self._instance = instance_id
        self._clock = clock
        self._failures: list[str] = []

    def run(
        self, options: ReplayOptions, on_record: Callable[[Replayed], None] | None = None
    ) -> ReplaySummary:
        summary = ReplaySummary()
        try:
            self._consume(options, summary, on_record)
        finally:
            self._source.close()

        if not options.dry_run:
            pending = self._producer.flush(options.flush_timeout_s)
            if pending or self._failures:
                first = self._failures[0] if self._failures else "timed out"
                raise ReplayError(
                    f"{len(self._failures)} deliveries failed, {pending} pending: {first}"
                )
        return summary

    def _consume(
        self,
        options: ReplayOptions,
        summary: ReplaySummary,
        on_record: Callable[[Replayed], None] | None,
    ) -> None:
        remaining = self._plan(options)
        if not remaining:
            return
        topic = self._stream.quarantine
        self._source.assign([TopicOffset(topic, p, start) for p, (start, _) in remaining.items()])
        dedup = MemoryDedupStore()
        started = self._clock()
        idle_since = self._clock()

        while remaining and not _reached(summary, options.limit):
            batch = self._source.consume(500, 1.0)
            if not batch:
                if (self._clock() - idle_since) / 1_000 > options.idle_timeout_s:
                    log.warning("no records for %.0fs, stopping early", options.idle_timeout_s)
                    return
                continue
            idle_since = self._clock()
            for message in batch:
                window = remaining.get(message.partition)
                if window is None or message.offset >= window[1]:
                    continue
                if message.offset + 1 >= window[1]:
                    del remaining[message.partition]
                if _after(message, options.until_ms):
                    remaining.pop(message.partition, None)
                    continue
                self._replay_one(
                    message,
                    options=options,
                    dedup=dedup,
                    started=started,
                    summary=summary,
                    on_record=on_record,
                )
                if _reached(summary, options.limit):
                    return

    def _plan(self, options: ReplayOptions) -> dict[int, tuple[int, int]]:
        topic = self._stream.quarantine
        partitions = self._source.partitions(topic)
        ends = {
            p: self._source.watermarks(TopicPartition(topic, p), cached=False) for p in partitions
        }
        if options.since_ms is None:
            starts = {p: low for p, (low, _) in ends.items()}
        else:
            found = self._source.offsets_for_time(topic, partitions, options.since_ms)
            starts = {p: found.get(p, -1) for p in partitions}
        plan = {}
        for p in partitions:
            start, (_, end) = starts[p], ends[p]
            if 0 <= start < end:
                plan[p] = (start, end)
        return plan

    def _replay_one(
        self,
        message: Record,
        *,
        options: ReplayOptions,
        dedup: MemoryDedupStore,
        started: int,
        summary: ReplaySummary,
        on_record: Callable[[Replayed], None] | None,
    ) -> None:
        summary.scanned += 1
        try:
            envelope = decode_envelope(message.value or b"", self._codec)
        except EnvelopeError as exc:
            summary.malformed += 1
            log.warning("skipping %s: %s", message.coordinates, exc)
            return

        original = Record(
            envelope.topic,
            envelope.partition,
            envelope.offset,
            envelope.key,
            envelope.value,
            strip_reserved(message.headers),
            envelope.timestamp_ms,
        )
        now = envelope.rejected_at_ms if options.as_of == "original" else started
        ruleset = self._stream.rules()
        ev = evaluate(original, ruleset, self._codec, dedup, now)
        if not options.release_duplicates:
            ev = _keep_stateful_rejections(ev, envelope, ruleset)
        result = Replayed(envelope, ev)
        if on_record is not None:
            on_record(result)

        if result.outcome is Outcome.REJECT:
            summary.still_rejected += 1
            summary.by_rule.update(v.rule_id for v in ev.verdict.violations)
            if not options.requarantine:
                return
        else:
            summary.released += 1

        if options.dry_run:
            return
        meta = EnvelopeMeta(self._instance, ruleset.id, __version__, started)
        out = route(self._stream, ev, self._codec, meta)
        headers = (*out.headers, (H_REPLAYED, format_iso(started).encode()))
        if result.outcome is not Outcome.REJECT:
            headers += ((H_SOURCE, original.coordinates.encode()),)
        self._producer.produce(
            out.topic,
            key=out.key,
            value=out.value,
            headers=headers,
            timestamp_ms=out.timestamp_ms,
            on_delivery=self._on_delivery,
        )

    def _on_delivery(self, error: str | None) -> None:
        if error is not None:
            self._failures.append(error)


def _keep_stateful_rejections(ev: Evaluation, envelope: Envelope, ruleset: RuleSet) -> Evaluation:
    # A record rejected as a duplicate is still a duplicate; replay has a fresh dedup window
    # and would otherwise release it.
    current = {v.rule_id for v in ev.verdict.violations}
    sticky = [
        v
        for v in envelope.violations
        if v.rule_id in ruleset.stateful_ids and v.rule_id not in current
    ]
    if not sticky:
        return ev
    verdict = Verdict(
        [*ev.verdict.violations, *sticky], ev.verdict.rewritten, ev.verdict.annotations
    )
    return Evaluation(ev.record, ev.data, verdict, ev.decode_error)


def _after(message: Record, until_ms: int | None) -> bool:
    return until_ms is not None and (message.timestamp_ms or 0) > until_ms


def _reached(summary: ReplaySummary, limit: int | None) -> bool:
    return limit is not None and summary.scanned >= limit
