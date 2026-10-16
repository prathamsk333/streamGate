import json

import pytest

from streamgate.codec import JsonCodec
from streamgate.envelope import (
    EnvelopeError,
    EnvelopeMeta,
    decode_envelope,
    encode_envelope,
    quarantine_headers,
    strip_reserved,
)
from streamgate.evaluation import evaluate
from streamgate.gateway.dedup import MemoryDedupStore
from streamgate.record import Record
from tests.helpers import NOW, ruleset

codec = JsonCodec()
META = EnvelopeMeta("sg-1", "orders@1:abc", "0.1.0", NOW)
RULES = ruleset(
    """
    - {id: a, type: required, field: a}
    - {id: b, type: required, field: b}
    """
)


def envelope_for(record: Record) -> bytes:
    ev = evaluate(record, RULES, codec, MemoryDedupStore(), NOW, reject_tombstones=True)
    return encode_envelope(ev, codec, META)


def test_schema() -> None:
    record = Record("raw.orders", 2, 41, b"ord-1", b'{"x": 1}', timestamp_ms=NOW - 1)
    body = json.loads(envelope_for(record))
    assert body == {
        "schema_version": 1,
        "rejected_at": "2025-10-14T10:00:00.000Z",
        "source": {
            "topic": "raw.orders",
            "partition": 2,
            "offset": 41,
            "timestamp_ms": NOW - 1,
            "key": "ord-1",
        },
        "violations": [
            {"rule": "a", "severity": "reject", "message": "a is missing"},
            {"rule": "b", "severity": "reject", "message": "b is missing"},
        ],
        "record": {"encoding": "json", "value": {"x": 1}},
        "streamgate": {"version": "0.1.0", "instance": "sg-1", "ruleset": "orders@1:abc"},
    }


@pytest.mark.parametrize(
    ("key", "value", "decoded_value"),
    [
        (b"ord-1", b'{"x": 1}', b'{"x":1}'),
        (b"\xff\x00", b"\xffjunk", b"\xffjunk"),
        (None, None, None),
    ],
)
def test_round_trip(key: bytes | None, value: bytes | None, decoded_value: bytes | None) -> None:
    record = Record("raw.orders", 1, 7, key, value, timestamp_ms=123)
    env = decode_envelope(envelope_for(record), codec)
    assert (env.topic, env.partition, env.offset, env.timestamp_ms) == ("raw.orders", 1, 7, 123)
    assert env.key == key
    assert env.value == decoded_value
    assert env.instance == "sg-1"
    assert env.ruleset == "orders@1:abc"
    assert env.violations


def test_headers() -> None:
    record = Record("raw.orders", 1, 7, None, b"{}")
    ev = evaluate(record, RULES, codec, MemoryDedupStore(), NOW)
    assert quarantine_headers(ev) == (
        ("sg-schema", b"1"),
        ("sg-violations", b"a,b"),
        ("sg-source", b"raw.orders/1/7"),
    )
    assert strip_reserved((("trace", b"1"), ("sg-schema", b"1"))) == (("trace", b"1"),)


@pytest.mark.parametrize(
    "raw",
    [
        b"nope",
        b'{"schema_version": 9}',
        b'{"schema_version": 1}',
        b'{"schema_version": 1, "source": {}, "record": {}, "streamgate": {}, "violations": []}',
    ],
)
def test_malformed(raw: bytes) -> None:
    with pytest.raises(EnvelopeError):
        decode_envelope(raw, codec)


def test_codec_mismatch() -> None:
    raw = envelope_for(Record("raw.orders", 0, 0, None, b"{}")).replace(b'"json"', b'"avro"')
    with pytest.raises(EnvelopeError, match="does not match codec"):
        decode_envelope(raw, codec)
