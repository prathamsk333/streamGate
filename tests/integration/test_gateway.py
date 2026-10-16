from __future__ import annotations

import json
import urllib.request
import uuid
from collections import Counter
from pathlib import Path
from typing import Any

import pytest

from streamgate.gateway.kafka import KafkaConsumer, TopicCheckError, TopicLayout, check_topics
from streamgate.timeutil import now_ms
from tests.integration.conftest import Kafka, start, wait_for, write_config

pytestmark = pytest.mark.integration

RULES = """\
  - {id: amount.range, type: range, field: amount, min: 1, max: 100}
  - {id: id.required, type: required, field: id}
"""


def order(i: int, amount: int = 10) -> dict[str, Any]:
    return {
        "key": f"ord-{i}".encode(),
        "value": json.dumps({"id": i, "amount": amount}).encode(),
        "headers": [("trace", f"t{i}".encode())],
        "timestamp": now_ms() - 1_000 + i,
    }


def test_routes_and_commits(kafka: Kafka, stream_name: str, tmp_path: Path) -> None:
    kafka.topics(stream_name, partitions=3)
    good = [order(i) for i in range(20)]
    bad = [order(i, amount=500) for i in range(20, 25)]
    kafka.produce(f"raw.{stream_name}", good + bad)

    group = f"sg-{uuid.uuid4().hex[:8]}"
    running = start(write_config(tmp_path, kafka.bootstrap, stream_name, RULES, group=group))
    try:
        clean = kafka.read(f"clean.{stream_name}", 20)
        quarantined = kafka.read(f"quarantine.{stream_name}", 5)
        wait_for(
            lambda: sum(kafka.committed(group, f"raw.{stream_name}").values()) == 25,
            what="offsets committed",
        )
    finally:
        running.stop()

    assert not running.error
    expected = {m["key"]: m for m in good}
    assert sorted(r.key or b"" for r in clean) == sorted(expected)
    for record in clean:
        sent = expected[record.key or b""]
        assert record.value == sent["value"]
        assert record.headers == (("trace", sent["headers"][0][1]),)
        assert record.timestamp_ms == sent["timestamp"]

    assert len(quarantined) == 5
    for record in quarantined:
        body = json.loads(record.value or b"")
        assert body["source"]["key"] == (record.key or b"").decode()
        assert [v["rule"] for v in body["violations"]] == ["amount.range"]
        assert dict(record.headers)["sg-violations"] == b"amount.range"


def test_poison_records(kafka: Kafka, stream_name: str, tmp_path: Path) -> None:
    kafka.topics(stream_name, partitions=1)
    kafka.produce(f"raw.{stream_name}", [{"key": b"p", "value": b"\x00\x01 not json"}])
    running = start(
        write_config(tmp_path, kafka.bootstrap, stream_name, RULES, group=f"sg-{stream_name}")
    )
    try:
        (record,) = kafka.read(f"quarantine.{stream_name}", 1)
    finally:
        running.stop()
    body = json.loads(record.value or b"")
    assert body["violations"][0]["rule"] == "decode.error"
    assert body["record"]["encoding"] == "base64"


def test_two_instances_split_partitions(kafka: Kafka, stream_name: str, tmp_path: Path) -> None:
    kafka.topics(stream_name, partitions=4)
    group = f"sg-{stream_name}"
    first = start(
        write_config(tmp_path, kafka.bootstrap, stream_name, RULES, group=group, instance_id="a")
    )
    second = start(
        write_config(tmp_path, kafka.bootstrap, stream_name, RULES, group=group, instance_id="b")
    )
    try:
        wait_for(
            lambda: (
                len(first.app.pipeline.assigned) + len(second.app.pipeline.assigned) == 4
                and bool(first.app.pipeline.assigned)
                and bool(second.app.pipeline.assigned)
            ),
            what="rebalance across both instances",
        )
        assert not first.app.pipeline.assigned & second.app.pipeline.assigned

        kafka.produce(f"raw.{stream_name}", [order(i, amount=500) for i in range(40)])
        quarantined = kafka.read(f"quarantine.{stream_name}", 40)
    finally:
        first.stop()
        second.stop()

    keys = Counter(r.key for r in quarantined)
    assert len(keys) == 40 and set(keys.values()) == {1}
    instances = {json.loads(r.value or b"")["streamgate"]["instance"] for r in quarantined}
    assert instances == {"a", "b"}


def test_crash_before_commit_redelivers(
    kafka: Kafka, stream_name: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    kafka.topics(stream_name, partitions=2)
    kafka.produce(f"raw.{stream_name}", [order(i) for i in range(10)])
    group = f"sg-{stream_name}"
    settings = write_config(tmp_path, kafka.bootstrap, stream_name, RULES, group=group)

    def crash(self: KafkaConsumer, offsets: object) -> None:
        raise RuntimeError("simulated crash between flush and commit")

    monkeypatch.setattr(KafkaConsumer, "commit", crash)
    crashed = start(settings)
    crashed.thread.join(timeout=60)
    assert crashed.error and "simulated crash" in str(crashed.error[0])
    assert all(o < 0 for o in kafka.committed(group, f"raw.{stream_name}").values())

    monkeypatch.undo()
    restarted = start(settings)
    try:
        wait_for(
            lambda: sum(kafka.committed(group, f"raw.{stream_name}").values()) == 10,
            what="offsets committed after restart",
        )
        clean = kafka.read(f"clean.{stream_name}", 11)
    finally:
        restarted.stop()

    counts = Counter(r.key for r in clean)
    assert set(counts) == {f"ord-{i}".encode() for i in range(10)}
    assert all(n >= 1 for n in counts.values())
    assert sum(counts.values()) > 10


def test_hot_reload(kafka: Kafka, stream_name: str, tmp_path: Path) -> None:
    kafka.topics(stream_name, partitions=1)
    settings = write_config(
        tmp_path,
        kafka.bootstrap,
        stream_name,
        "  []\n",
        group=f"sg-{stream_name}",
        rules_reload_s=0.2,
    )
    rules_file = settings.streams[0].rules
    running = start(settings)
    raw = f"raw.{stream_name}"
    try:
        kafka.produce(raw, [{"value": b'{"a": 1}'}])
        kafka.read(f"clean.{stream_name}", 1, settle=0)

        rules_file.write_text(
            f"stream: {stream_name}\nrules:\n  - {{id: b.required, type: required, field: b}}\n"
        )
        registry = running.app.metrics.registry

        def reloads(result: str) -> float | None:
            labels = {"stream": stream_name, "result": result}
            return registry.get_sample_value("streamgate_ruleset_reloads_total", labels)

        wait_for(lambda: reloads("ok") == 1, what="reload")
        kafka.produce(raw, [{"value": b'{"a": 2}'}])
        kafka.read(f"quarantine.{stream_name}", 1, settle=0)

        rules_file.write_text("stream: nope\n")
        wait_for(lambda: reloads("error") == 1, what="failed reload")
        kafka.produce(raw, [{"value": b'{"a": 3}'}])
        quarantined = kafka.read(f"quarantine.{stream_name}", 2, settle=0)
    finally:
        running.stop()
    assert len(quarantined) == 2


def test_metrics_and_health(kafka: Kafka, stream_name: str, tmp_path: Path) -> None:
    kafka.topics(stream_name, partitions=2)
    settings = write_config(
        tmp_path,
        kafka.bootstrap,
        stream_name,
        RULES,
        group=f"sg-{stream_name}",
        metrics={"enabled": True, "port": 0, "bind": "127.0.0.1"},
    )
    running = start(settings)
    assert running.app.server is not None
    base = f"http://127.0.0.1:{running.app.server.port}"
    try:
        kafka.produce(f"raw.{stream_name}", [order(1), order(2, amount=0)])
        kafka.read(f"quarantine.{stream_name}", 1, settle=0)
        wait_for(
            lambda: (
                f'streamgate_records_total{{outcome="pass",stream="{stream_name}"}} 1.0'
                in scrape(base)
            ),
            what="metrics",
        )
        body = scrape(base)
        with urllib.request.urlopen(f"{base}/readyz", timeout=5) as response:  # noqa: S310
            assert response.status == 200
    finally:
        running.stop()

    assert f'streamgate_records_total{{outcome="reject",stream="{stream_name}"}} 1.0' in body
    assert (
        f'streamgate_violations_total{{rule="amount.range",severity="reject",stream="{stream_name}"}}'
        " 1.0" in body
    )
    assert f'streamgate_assigned_partitions{{stream="{stream_name}"}} 2.0' in body
    assert 'streamgate_consumer_lag{partition="' in body


def scrape(base: str) -> str:
    with urllib.request.urlopen(f"{base}/metrics", timeout=5) as response:  # noqa: S310
        body: bytes = response.read()
        return body.decode()


def test_topic_checks(kafka: Kafka, stream_name: str) -> None:
    with pytest.raises(TopicCheckError, match=f"missing topics: raw.{stream_name}"):
        check_topics(
            {"bootstrap.servers": kafka.bootstrap},
            [
                TopicLayout(
                    stream_name,
                    f"raw.{stream_name}",
                    f"clean.{stream_name}",
                    f"quarantine.{stream_name}",
                )
            ],
        )

    kafka.topics(stream_name, partitions=3, clean_partitions=2)
    warnings = check_topics(
        {"bootstrap.servers": kafka.bootstrap},
        [
            TopicLayout(
                stream_name,
                f"raw.{stream_name}",
                f"clean.{stream_name}",
                f"quarantine.{stream_name}",
            )
        ],
    )
    assert len(warnings) == 1
    assert "has 3 partitions but" in warnings[0]
