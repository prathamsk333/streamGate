from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from streamgate.codec import Codec
from streamgate.envelope import H_WARNINGS, EnvelopeMeta, encode_envelope, quarantine_headers
from streamgate.evaluation import Evaluation
from streamgate.gateway.dedup import MemoryDedupStore
from streamgate.record import Headers
from streamgate.rules.base import Outcome
from streamgate.rules.engine import RuleSet


@dataclass(frozen=True, slots=True)
class Stream:
    name: str
    input: str
    clean: str
    quarantine: str
    rules: Callable[[], RuleSet]
    dedup: MemoryDedupStore


@dataclass(frozen=True, slots=True)
class Output:
    topic: str
    key: bytes | None
    value: bytes | None
    headers: Headers
    timestamp_ms: int | None


def route(
    stream: Stream,
    ev: Evaluation,
    codec: Codec,
    meta: EnvelopeMeta,
    *,
    warn_header: bool = False,
) -> Output:
    record, verdict = ev.record, ev.verdict
    if verdict.outcome is Outcome.REJECT:
        return Output(
            stream.quarantine,
            record.key,
            encode_envelope(ev, codec, meta),
            record.headers + quarantine_headers(ev),
            meta.rejected_at_ms,
        )

    value = record.value if verdict.rewritten is None else codec.encode(verdict.rewritten)
    extra = list(verdict.annotations.items())
    if warn_header and verdict.violations:
        extra.append((H_WARNINGS, ",".join(v.rule_id for v in verdict.violations)))
    headers = record.headers + tuple((k, v.encode()) for k, v in extra)
    return Output(stream.clean, record.key, value, headers, record.timestamp_ms)
