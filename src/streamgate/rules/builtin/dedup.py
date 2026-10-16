from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, ClassVar

from streamgate.rules.base import Context, Rule, Severity, StatefulRule
from streamgate.rules.fields import MISSING, FieldPath, show
from streamgate.rules.spec import Duration, FieldName, RuleSpec
from streamgate.timeutil import format_duration


class DedupSpec(RuleSpec):
    kind: ClassVar[str] = "dedup"
    key: FieldName
    window: Duration

    def build(self, severity: Severity) -> Rule:
        return DedupRule(self, severity)


class DedupRule(StatefulRule):
    """Rejects a record whose key was already seen on the same partition within the window.

    Records with the same Kafka key always land on the same partition, so a per-partition
    window is enough as long as the dedup field is (part of) the message key.
    """

    def __init__(self, spec: DedupSpec, severity: Severity) -> None:
        super().__init__(spec.id, severity, spec.message)
        self.key = FieldPath(spec.key)
        self.window = spec.window

    def check(self, data: Mapping[str, Any], ctx: Context) -> str | None:
        value = self.key.get(data)
        if value is MISSING or value is None:
            return None
        if isinstance(value, str | int) and not isinstance(value, bool):
            token = str(value)
        else:
            token = json.dumps(value, sort_keys=True)
        if ctx.dedup.seen_or_add(
            ctx.record.partition, f"{self.id}\x00{token}", ctx.now_ms, self.window
        ):
            return f"duplicate {self.key}={show(value)} within {format_duration(self.window)}"
        return None
