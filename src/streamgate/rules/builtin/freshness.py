from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar, Literal

from pydantic import model_validator

from streamgate.rules.base import Context, Rule, Severity
from streamgate.rules.fields import MISSING, FieldPath
from streamgate.rules.spec import Duration, FieldName, RuleSpec
from streamgate.timeutil import TimestampFormat, format_duration, parse_timestamp


class FreshnessSpec(RuleSpec):
    kind: ClassVar[str] = "freshness"
    field: FieldName | None = None
    source: Literal["field", "kafka"] = "field"
    format: TimestampFormat = "iso8601"
    max_age: Duration
    future_tolerance: Duration = 30_000

    @model_validator(mode="after")
    def _source(self) -> FreshnessSpec:
        if self.source == "field" and self.field is None:
            raise ValueError("field is required when source is 'field'")
        if self.source == "kafka" and self.field is not None:
            raise ValueError("field must not be set when source is 'kafka'")
        return self

    def build(self, severity: Severity) -> Rule:
        return FreshnessRule(self, severity)


class FreshnessRule(Rule):
    def __init__(self, spec: FreshnessSpec, severity: Severity) -> None:
        super().__init__(spec.id, severity, spec.message)
        self.field = FieldPath(spec.field) if spec.field else None
        self.format = spec.format
        self.max_age = spec.max_age
        self.future_tolerance = spec.future_tolerance

    def check(self, data: Mapping[str, Any], ctx: Context) -> str | None:
        if self.field is None:
            label = "kafka timestamp"
            if ctx.record.timestamp_ms is None:
                return "record has no Kafka timestamp"
            ts = ctx.record.timestamp_ms
        else:
            label = str(self.field)
            value = self.field.get(data)
            if value is MISSING or value is None:
                return None
            try:
                ts = parse_timestamp(value, self.format)
            except ValueError as exc:
                return f"{label}: {exc}"

        age = ctx.now_ms - ts
        if age > self.max_age:
            limit = format_duration(self.max_age)
            return f"{label} is {format_duration(age)} old, max age is {limit}"
        if -age > self.future_tolerance:
            return f"{label} is {format_duration(-age)} in the future"
        return None
