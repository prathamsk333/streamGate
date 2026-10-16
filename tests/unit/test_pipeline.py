from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from streamgate.codec import JsonCodec
from streamgate.config import PipelineSettings
from streamgate.gateway.dedup import MemoryDedupStore
from streamgate.gateway.pipeline import Pipeline, ProduceError
from streamgate.gateway.ports import TopicOffset, TopicPartition
from streamgate.gateway.router import Stream
from streamgate.health import Health
from streamgate.metrics import Metrics
from streamgate.record import Record
from streamgate.rules.engine import RuleSet
from streamgate.rules.loader import RuleWatcher
from tests.fakes import FakeConsumer, FakeProducer, Step
from tests.helpers import NOW, ruleset

RULES = """
- {id: amount.range, type: range, field: amount, min: 1, max: 100}
- {id: note.required, type: required, field: note, severity: warn}
- {id: id.dedup, type: dedup, key: id, window: 1m}
- {id: email.pii, type: pii, fields: [note], patterns: [email], action: mask, severity: warn}
"""
P0 = TopicPartition("raw.orders", 0)
P1 = TopicPartition("raw.orders", 1)


def rec(offset: int, value: bytes | None, partition: int = 0, **kw: Any) -> Record:
    return Record("raw.orders", partition, offset, kw.pop("key", b"k"), value, **kw)


def js(**fields: Any) -> bytes:
    return json.dumps(fields).encode()


class Harness:
    def __init__(
        self,
        script: list[Step],
        rules: RuleSet | None = None,
        watcher: RuleWatcher | None = None,
        commit: bool = True,
        **settings: Any,
    ) -> None:
        rs = rules or ruleset(RULES)
        self.dedup = MemoryDedupStore()
        self.stream = Stream(
            "orders",
            "raw.orders",
            "clean.orders",
            "quarantine.orders",
            rules=(lambda: watcher.get("orders")) if watcher else (lambda: rs),
            dedup=self.dedup,
        )
        self.consumer = FakeConsumer(script, [P0, P1])
        self.producer = FakeProducer()
        self.metrics = Metrics()
        self.health = Health(60)
        self.pipeline = Pipeline(
            consumer=self.consumer,
            producer=self.producer,
            streams=[self.stream],
            codec=JsonCodec(),
            settings=PipelineSettings(**settings),
            metrics=self.metrics,
            health=self.health,
            instance_id="test-1",
            watcher=watcher,
            reload_interval_s=1e-9 if watcher else 0,
            commit=commit,
            clock=lambda: NOW,
        )
        self.consumer.on_drained = self.pipeline.stop

    def run(self) -> Harness:
        self.pipeline.run()
        return self

    def sample(self, name: str, **labels: str) -> float | None:
        return self.metrics.registry.get_sample_value(name, labels)


def test_routes_by_outcome() -> None:
    h = Harness(
        [
            [
                rec(0, js(id=1, amount=50, note="hi")),
                rec(1, js(id=2, amount=500, note="hi")),
                rec(2, js(id=3, amount=5)),
            ]
        ]
    ).run()
    assert [s.value for s in h.producer.to("clean.orders")] == [
        js(id=1, amount=50, note="hi"),
        js(id=3, amount=5),
    ]
    (rejected,) = h.producer.to("quarantine.orders")
    body = json.loads(rejected.value or b"")
    assert body["source"] == {
        "topic": "raw.orders",
        "partition": 0,
        "offset": 1,
        "timestamp_ms": None,
        "key": "k",
    }
    assert [v["rule"] for v in body["violations"]] == ["amount.range"]
    assert body["streamgate"]["instance"] == "test-1"
    assert dict(rejected.headers)["sg-violations"] == b"amount.range"
    assert rejected.timestamp_ms == NOW
    assert h.sample("streamgate_records_total", stream="orders", outcome="pass") == 1
    assert h.sample("streamgate_records_total", stream="orders", outcome="warn") == 1
    assert h.sample("streamgate_records_total", stream="orders", outcome="reject") == 1
    assert (
        h.sample(
            "streamgate_violations_total", stream="orders", rule="amount.range", severity="reject"
        )
        == 1
    )


def test_clean_records_are_byte_exact() -> None:
    raw = b'{ "id": 1,   "amount": 5, "note": "x" }'
    original = rec(
        0, raw, key=b"\x00bin", headers=(("trace", b"abc"), ("empty", None)), timestamp_ms=NOW - 5
    )
    h = Harness([[original]]).run()
    (out,) = h.producer.sent
    assert (out.topic, out.key, out.value, out.headers, out.timestamp_ms) == (
        "clean.orders",
        b"\x00bin",
        raw,
        (("trace", b"abc"), ("empty", None)),
        NOW - 5,
    )


def test_masked_records_are_rewritten_everywhere() -> None:
    h = Harness(
        [
            [
                rec(0, js(id=1, amount=5, note="mail a@example.com")),
                rec(1, js(id=2, amount=0, note="mail a@example.com")),
            ]
        ]
    ).run()
    (clean,) = h.producer.to("clean.orders")
    assert json.loads(clean.value or b"")["note"] == "mail ***"
    assert dict(clean.headers)["sg-masked"] == b"email.pii"
    (quarantined,) = h.producer.to("quarantine.orders")
    assert b"a@example.com" not in (quarantined.value or b"")


def test_warn_header() -> None:
    h = Harness([[rec(0, js(id=1, amount=5))]], warn_header=True).run()
    assert dict(h.producer.sent[0].headers)["sg-warnings"] == b"note.required"


def test_poison_and_tombstones() -> None:
    h = Harness([[rec(0, b"\xff not json"), rec(1, None)]]).run()
    (poison,) = h.producer.to("quarantine.orders")
    body = json.loads(poison.value or b"")
    assert body["record"]["encoding"] == "base64"
    assert body["violations"][0]["rule"] == "decode.error"
    (tombstone,) = h.producer.to("clean.orders")
    assert tombstone.value is None


def test_tombstones_can_be_rejected() -> None:
    h = Harness([[rec(0, None)]], reject_tombstones=True).run()
    (out,) = h.producer.sent
    assert out.topic == "quarantine.orders"
    assert json.loads(out.value or b"")["record"] == {"encoding": "none", "value": None}


def test_commits_next_offset_per_partition_after_each_batch() -> None:
    h = Harness(
        [
            [
                rec(0, js(id=1, amount=5)),
                rec(5, js(id=2, amount=5), partition=1),
                rec(1, js(id=3, amount=5)),
            ],
            [rec(6, js(id=4, amount=5), partition=1)],
        ]
    ).run()
    assert h.consumer.commits == [
        [TopicOffset("raw.orders", 0, 2), TopicOffset("raw.orders", 1, 6)],
        [TopicOffset("raw.orders", 1, 7)],
    ]


@pytest.mark.parametrize("failure", ["delivery", "timeout"])
def test_no_commit_when_delivery_fails(failure: str) -> None:
    h = Harness([[rec(0, js(id=1, amount=5))]])
    if failure == "delivery":
        h.producer.failing.add("clean.orders")
    else:
        h.producer.stuck = 1
    with pytest.raises(ProduceError):
        h.run()
    assert h.consumer.commits == []
    assert h.consumer.closed
    assert not h.health.ready


def test_log_mode_commits_despite_failures() -> None:
    h = Harness([[rec(0, js(id=1, amount=5))]], on_produce_error="log")
    h.producer.failing.add("clean.orders")
    h.run()
    assert h.consumer.commits == [[TopicOffset("raw.orders", 0, 1)]]
    assert h.sample("streamgate_produce_errors_total", topic="clean.orders") == 1


def test_dry_run_never_commits() -> None:
    h = Harness([[rec(0, js(id=1, amount=5))]], commit=False).run()
    assert h.consumer.commits == []
    assert len(h.producer.sent) == 1


def test_revoke_drops_dedup_state_for_that_partition() -> None:
    h = Harness([])
    h.consumer.script.extend(
        [
            [rec(0, js(id=1, amount=5)), rec(0, js(id=1, amount=5), partition=1)],
            lambda: h.consumer.revoke([P0]),
            [rec(1, js(id=1, amount=5)), rec(1, js(id=1, amount=5), partition=1)],
        ]
    )
    h.run()
    outcomes = [s.topic for s in h.producer.sent]
    assert outcomes == ["clean.orders", "clean.orders", "clean.orders", "quarantine.orders"]


def test_assignment_and_readiness() -> None:
    h = Harness([])
    seen: list[Any] = []
    h.consumer.script.append(
        lambda: seen.append(
            (
                h.health.ready,
                h.pipeline.assigned,
                h.sample("streamgate_assigned_partitions", stream="orders"),
            )
        )
    )
    h.run()
    assert seen == [(True, frozenset({P0, P1}), 2)]
    assert not h.health.ready
    assert h.pipeline.assigned == frozenset()


def test_lag_gauge() -> None:
    h = Harness([[rec(3, js(id=1, amount=5))]])
    h.consumer.high[P0] = 10
    seen: list[float | None] = []
    h.consumer.script.append(
        lambda: seen.append(h.sample("streamgate_consumer_lag", stream="orders", partition="0"))
    )
    h.run()
    assert seen == [6]
    assert h.sample("streamgate_consumer_lag", stream="orders", partition="0") is None


def test_hot_reload_between_batches(tmp_path: Path) -> None:
    path = tmp_path / "orders.yaml"
    path.write_text("stream: orders\nrules: []\n")
    watcher = RuleWatcher({"orders": path})
    h = Harness([], watcher=watcher)

    def tighten() -> None:
        path.write_text(
            "stream: orders\nversion: 2\nrules:\n  - {id: a, type: required, field: a}\n"
        )

    h.consumer.script.extend([[rec(0, js(b=1))], tighten, [rec(1, js(b=1))]])
    h.run()
    assert [s.topic for s in h.producer.sent] == ["clean.orders", "quarantine.orders"]
    assert h.sample("streamgate_ruleset_reloads_total", stream="orders", result="ok") == 1
    quarantined = json.loads(h.producer.sent[1].value or b"")
    assert quarantined["streamgate"]["ruleset"].startswith("orders@2:")


def test_broken_reload_keeps_old_rules(tmp_path: Path) -> None:
    path = tmp_path / "orders.yaml"
    path.write_text("stream: orders\nrules: []\n")
    watcher = RuleWatcher({"orders": path})
    h = Harness([], watcher=watcher)
    h.consumer.script.extend(
        [
            lambda: path.write_text("stream: orders\nrules:\n  - {id: a, type: nope}\n"),
            [rec(0, js(b=1))],
        ]
    )
    h.run()
    assert [s.topic for s in h.producer.sent] == ["clean.orders"]
    assert h.sample("streamgate_ruleset_reloads_total", stream="orders", result="error") == 1
