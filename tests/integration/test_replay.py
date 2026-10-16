from __future__ import annotations

import json
from pathlib import Path

import pytest

from streamgate.app import build_replayer
from streamgate.replay import ReplayOptions
from tests.integration.conftest import Kafka, start, wait_for, write_config

pytestmark = pytest.mark.integration

STRICT = "  - {id: amount.range, type: range, field: amount, max: 100}\n"


def test_fix_rule_then_replay(kafka: Kafka, stream_name: str, tmp_path: Path) -> None:
    kafka.topics(stream_name, partitions=2)
    raw, clean, quarantine = (f"{t}.{stream_name}" for t in ("raw", "clean", "quarantine"))
    kafka.produce(
        raw,
        [
            {"key": b"ord-1", "value": b'{"amount": 500}', "headers": [("trace", b"x")]},
            {"key": b"ord-2", "value": b'{"amount": 5000}'},
        ],
    )
    settings = write_config(
        tmp_path, kafka.bootstrap, stream_name, STRICT, group=f"sg-{stream_name}"
    )
    running = start(settings)
    try:
        kafka.read(quarantine, 2, settle=0)
    finally:
        running.stop()

    relaxed = "  - {id: amount.range, type: range, field: amount, max: 1000}\n"
    settings.streams[0].rules.write_text(f"stream: {stream_name}\nrules:\n{relaxed}")

    dry = build_replayer(settings, stream_name, dry_run=True).run(ReplayOptions(dry_run=True))
    assert (dry.scanned, dry.released, dry.still_rejected) == (2, 1, 1)
    assert kafka.read(clean, 0, settle=2) == []

    summary = build_replayer(settings, stream_name).run(ReplayOptions())
    assert (summary.released, summary.still_rejected) == (1, 1)

    (released,) = kafka.read(clean, 1)
    assert released.key == b"ord-1"
    assert json.loads(released.value or b"") == {"amount": 500}
    headers = dict(released.headers)
    assert headers["trace"] == b"x"
    assert (headers["sg-source"] or b"").startswith(f"{raw}/".encode())
    assert "sg-replayed" in headers

    again = build_replayer(settings, stream_name).run(ReplayOptions())
    assert again.released == 1
    wait_for(lambda: len(kafka.read(clean, 2, settle=0)) == 2, what="second release")
