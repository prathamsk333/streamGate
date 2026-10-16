from __future__ import annotations

import json
from typing import Any

import pytest

from streamgate.codec import JsonCodec
from streamgate.envelope import EnvelopeMeta, encode_envelope, quarantine_headers
from streamgate.evaluation import evaluate
from streamgate.gateway.dedup import MemoryDedupStore
from streamgate.gateway.router import Stream
from streamgate.record import Record
from streamgate.replay import Replayed, Replayer, ReplayError, ReplayOptions
from streamgate.rules.base import Outcome
from streamgate.rules.engine import RuleSet
from tests.fakes import FakeProducer, FakeQuarantine
from tests.helpers import NOW, ruleset

codec = JsonCodec()
STRICT = ruleset("- {id: amount.range, type: range, field: amount, max: 100}")
RELAXED = ruleset("- {id: amount.range, type: range, field: amount, max: 1000}")
MINUTE = 60_000


def quarantined(offset: int, amount: int, *, rejected_at: int = NOW, partition: int = 0) -> Record:
    original = Record(
        "raw.orders",
        partition,
        100 + offset,
        f"ord-{offset}".encode(),
        json.dumps({"amount": amount, "at": "2025-10-14T09:59:00Z"}).encode(),
        (("trace", b"t"),),
        NOW - MINUTE,
    )
    ev = evaluate(original, STRICT, codec, MemoryDedupStore(), rejected_at)
    meta = EnvelopeMeta("sg-1", STRICT.id, "0.1.0", rejected_at)
    return Record(
        "quarantine.orders",
        partition,
        offset,
        original.key,
        encode_envelope(ev, codec, meta),
        original.headers + quarantine_headers(ev),
        rejected_at,
    )


def make(
    rules: RuleSet, partitions: dict[int, list[Record]]
) -> tuple[Replayer, FakeProducer, FakeQuarantine]:
    source = FakeQuarantine("quarantine.orders", partitions)
    producer = FakeProducer()
    stream = Stream(
        "orders",
        "raw.orders",
        "clean.orders",
        "quarantine.orders",
        rules=lambda: rules,
        dedup=MemoryDedupStore(),
    )
    replayer = Replayer(
        source=source,
        producer=producer,
        stream=stream,
        codec=codec,
        instance_id="replay-1",
        clock=lambda: NOW + 10 * MINUTE,
    )
    return replayer, producer, source


def test_releases_records_that_now_pass() -> None:
    replayer, producer, source = make(RELAXED, {0: [quarantined(0, 500), quarantined(1, 5000)]})
    summary = replayer.run(ReplayOptions())
    assert (summary.scanned, summary.released, summary.still_rejected) == (2, 1, 1)
    assert summary.by_rule == {"amount.range": 1}
    (out,) = producer.sent
    assert out.topic == "clean.orders"
    assert out.key == b"ord-0"
    assert json.loads(out.value or b"") == {"amount": 500, "at": "2025-10-14T09:59:00Z"}
    assert out.timestamp_ms == NOW - MINUTE
    headers = dict(out.headers)
    assert headers["trace"] == b"t"
    assert headers["sg-source"] == b"raw.orders/0/100"
    assert headers["sg-replayed"] == b"2025-10-14T10:10:00.000Z"
    assert "sg-violations" not in headers
    assert source.closed


def test_requarantine() -> None:
    replayer, producer, _ = make(STRICT, {0: [quarantined(0, 500)]})
    replayer.run(ReplayOptions(requarantine=True))
    (out,) = producer.sent
    assert out.topic == "quarantine.orders"
    assert json.loads(out.value or b"")["streamgate"]["instance"] == "replay-1"
    assert dict(out.headers)["sg-replayed"]


def test_dry_run_produces_nothing() -> None:
    replayer, producer, _ = make(RELAXED, {0: [quarantined(0, 500)]})
    seen: list[Replayed] = []
    summary = replayer.run(ReplayOptions(dry_run=True), on_record=seen.append)
    assert producer.sent == []
    assert summary.released == 1
    assert [r.outcome for r in seen] == [Outcome.PASS]


def test_time_window_and_limit() -> None:
    records = [quarantined(i, 500, rejected_at=NOW + i * MINUTE) for i in range(6)]
    replayer, producer, _ = make(RELAXED, {0: records, 1: []})
    summary = replayer.run(ReplayOptions(since_ms=NOW + 2 * MINUTE, until_ms=NOW + 4 * MINUTE))
    assert summary.scanned == 3
    assert [s.key for s in producer.sent] == [b"ord-2", b"ord-3", b"ord-4"]

    replayer, producer, _ = make(RELAXED, {0: records})
    assert replayer.run(ReplayOptions(limit=2)).scanned == 2


def test_nothing_after_since() -> None:
    replayer, _, source = make(RELAXED, {0: [quarantined(0, 500)]})
    summary = replayer.run(ReplayOptions(since_ms=NOW + MINUTE))
    assert summary.scanned == 0
    assert source.closed


@pytest.mark.parametrize(
    ("as_of", "outcome"), [("original", Outcome.PASS), ("now", Outcome.REJECT)]
)
def test_time_based_rules_use_the_original_clock(as_of: Any, outcome: Outcome) -> None:
    rules = ruleset("- {id: fresh, type: freshness, field: at, max_age: 5m}")
    replayer, _, _ = make(rules, {0: [quarantined(0, 500)]})
    seen: list[Replayed] = []
    replayer.run(ReplayOptions(as_of=as_of, dry_run=True), on_record=seen.append)
    assert seen[0].outcome is outcome


def test_malformed_envelopes_are_skipped() -> None:
    bad = Record("quarantine.orders", 0, 1, None, b"not an envelope", timestamp_ms=NOW)
    replayer, _, _ = make(RELAXED, {0: [quarantined(0, 500), bad]})
    summary = replayer.run(ReplayOptions())
    assert (summary.scanned, summary.malformed, summary.released) == (2, 1, 1)


def test_delivery_failure() -> None:
    replayer, producer, _ = make(RELAXED, {0: [quarantined(0, 500)]})
    producer.failing.add("clean.orders")
    with pytest.raises(ReplayError, match="1 deliveries failed"):
        replayer.run(ReplayOptions())


def test_duplicates_stay_rejected() -> None:
    dedup_rules = ruleset("- {id: once, type: dedup, key: id, window: 10m}")
    first = Record("raw.orders", 0, 1, b"k", b'{"id": 1}', timestamp_ms=NOW)
    store = MemoryDedupStore()
    evaluate(first, dedup_rules, codec, store, NOW)
    ev = evaluate(first, dedup_rules, codec, store, NOW)
    meta = EnvelopeMeta("sg-1", dedup_rules.id, "0.1.0", NOW)
    envelope = Record("quarantine.orders", 0, 0, b"k", encode_envelope(ev, codec, meta), (), NOW)

    replayer, producer, _ = make(dedup_rules, {0: [envelope]})
    summary = replayer.run(ReplayOptions())
    assert (summary.released, summary.still_rejected) == (0, 1)
    assert summary.by_rule == {"once": 1}

    replayer, producer, _ = make(dedup_rules, {0: [envelope]})
    assert replayer.run(ReplayOptions(release_duplicates=True)).released == 1
    assert len(producer.sent) == 1
