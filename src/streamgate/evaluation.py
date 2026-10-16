from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from streamgate.codec import Codec, DecodeError
from streamgate.record import Record
from streamgate.rules.base import Context, DedupStore, Severity, Verdict, Violation
from streamgate.rules.engine import RuleSet

DECODE_ERROR = "decode.error"


@dataclass(frozen=True, slots=True)
class Evaluation:
    record: Record
    data: dict[str, Any] | None
    verdict: Verdict
    decode_error: str | None = None


def evaluate(
    record: Record,
    ruleset: RuleSet,
    codec: Codec,
    dedup: DedupStore,
    now_ms: int,
    *,
    reject_tombstones: bool = False,
) -> Evaluation:
    if record.value is None:
        if reject_tombstones:
            return _undecodable(record, "record has no value")
        # Tombstones carry deletes on compacted topics; they go through untouched by default.
        return Evaluation(record, None, Verdict())
    try:
        data = codec.decode(record.value)
    except DecodeError as exc:
        return _undecodable(record, str(exc))
    return Evaluation(record, data, ruleset.evaluate(data, Context(record, now_ms, dedup)))


def _undecodable(record: Record, reason: str) -> Evaluation:
    verdict = Verdict([Violation(DECODE_ERROR, Severity.REJECT, reason)])
    return Evaluation(record, None, verdict, decode_error=reason)
