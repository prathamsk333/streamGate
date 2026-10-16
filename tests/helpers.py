from __future__ import annotations

import textwrap
from typing import Any

from streamgate.gateway.dedup import MemoryDedupStore
from streamgate.record import Record
from streamgate.rules.base import Context, Verdict
from streamgate.rules.engine import RuleSet
from streamgate.rules.loader import parse_ruleset

NOW = 1_760_436_000_000  # 2025-10-14T10:00:00Z


def ruleset(rules_yaml: str, stream: str = "orders") -> RuleSet:
    body = textwrap.indent(textwrap.dedent(rules_yaml).strip(), "  ")
    return parse_ruleset(f"stream: {stream}\nrules:\n{body}\n".encode(), "test.yaml")


def run(
    rs: RuleSet,
    data: dict[str, Any],
    *,
    now: int = NOW,
    partition: int = 0,
    timestamp_ms: int | None = None,
    dedup: MemoryDedupStore | None = None,
) -> Verdict:
    record = Record("raw.orders", partition, 0, None, b"{}", timestamp_ms=timestamp_ms)
    store = MemoryDedupStore() if dedup is None else dedup
    return rs.evaluate(data, Context(record, now, store))


def ids(verdict: Verdict) -> list[str]:
    return [v.rule_id for v in verdict.violations]
