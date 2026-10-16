"""The quarantine envelope: what gets written to `quarantine.*` for a rejected record.

See docs/envelope.md for the schema. Bump SCHEMA_VERSION on any incompatible change.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any

from streamgate.codec import Codec
from streamgate.evaluation import Evaluation
from streamgate.record import Headers
from streamgate.rules.base import Severity, Violation
from streamgate.timeutil import format_iso, parse_timestamp

SCHEMA_VERSION = 1

H_SCHEMA = "sg-schema"
H_VIOLATIONS = "sg-violations"
H_SOURCE = "sg-source"
H_WARNINGS = "sg-warnings"
H_REPLAYED = "sg-replayed"
RESERVED_PREFIX = "sg-"


class EnvelopeError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class EnvelopeMeta:
    instance: str
    ruleset: str
    version: str
    rejected_at_ms: int


@dataclass(frozen=True, slots=True)
class Envelope:
    topic: str
    partition: int
    offset: int
    timestamp_ms: int | None
    key: bytes | None
    value: bytes | None
    violations: list[Violation]
    rejected_at_ms: int
    ruleset: str
    instance: str


def encode_envelope(ev: Evaluation, codec: Codec, meta: EnvelopeMeta) -> bytes:
    record = ev.record
    body = {
        "schema_version": SCHEMA_VERSION,
        "rejected_at": format_iso(meta.rejected_at_ms),
        "source": {
            "topic": record.topic,
            "partition": record.partition,
            "offset": record.offset,
            "timestamp_ms": record.timestamp_ms,
            **_encode_key(record.key),
        },
        "violations": [
            {"rule": v.rule_id, "severity": v.severity.value, "message": v.message}
            for v in ev.verdict.violations
        ],
        "record": _encode_record(ev, codec),
        "streamgate": {
            "version": meta.version,
            "instance": meta.instance,
            "ruleset": meta.ruleset,
        },
    }
    return json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode()


def quarantine_headers(ev: Evaluation) -> Headers:
    record = ev.record
    rules = ",".join(dict.fromkeys(v.rule_id for v in ev.verdict.violations))
    return (
        (H_SCHEMA, str(SCHEMA_VERSION).encode()),
        (H_VIOLATIONS, rules.encode()),
        (H_SOURCE, record.coordinates.encode()),
    )


def _encode_key(key: bytes | None) -> dict[str, Any]:
    if key is None:
        return {"key": None}
    try:
        return {"key": key.decode()}
    except UnicodeDecodeError:
        return {"key": base64.b64encode(key).decode(), "key_encoding": "base64"}


def _encode_record(ev: Evaluation, codec: Codec) -> dict[str, Any]:
    if ev.record.value is None:
        return {"encoding": "none", "value": None}
    if ev.decode_error is not None or ev.data is None:
        return {
            "encoding": "base64",
            "value": base64.b64encode(ev.record.value).decode(),
            "error": ev.decode_error,
        }
    data = ev.verdict.rewritten if ev.verdict.rewritten is not None else ev.data
    return {"encoding": codec.name, "value": data}


def decode_envelope(raw: bytes, codec: Codec) -> Envelope:
    try:
        body = json.loads(raw)
        if body.get("schema_version") != SCHEMA_VERSION:
            raise EnvelopeError(f"unsupported schema_version {body.get('schema_version')!r}")
        source, record, meta = body["source"], body["record"], body["streamgate"]
        return Envelope(
            topic=source["topic"],
            partition=source["partition"],
            offset=source["offset"],
            timestamp_ms=source.get("timestamp_ms"),
            key=_decode_key(source),
            value=_decode_value(record, codec),
            violations=[
                Violation(v["rule"], Severity(v["severity"]), v["message"])
                for v in body["violations"]
            ],
            rejected_at_ms=parse_timestamp(body["rejected_at"], "iso8601"),
            ruleset=meta["ruleset"],
            instance=meta["instance"],
        )
    except EnvelopeError:
        raise
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        raise EnvelopeError(f"malformed envelope: {exc!r}") from None


def _decode_key(source: dict[str, Any]) -> bytes | None:
    key = source.get("key")
    if key is None:
        return None
    if source.get("key_encoding") == "base64":
        return base64.b64decode(key)
    return str(key).encode()


def _decode_value(record: dict[str, Any], codec: Codec) -> bytes | None:
    encoding = record["encoding"]
    if encoding == "none":
        return None
    if encoding == "base64":
        return base64.b64decode(record["value"])
    if encoding == codec.name:
        return codec.encode(record["value"])
    raise EnvelopeError(f"record encoding {encoding!r} does not match codec {codec.name!r}")


def strip_reserved(headers: Headers) -> Headers:
    return tuple((k, v) for k, v in headers if not k.startswith(RESERVED_PREFIX))
