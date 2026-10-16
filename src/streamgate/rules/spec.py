"""Pydantic models for rule files.

Each rule type is a `RuleSpec` subclass with a `kind` and a `build` method. The registry
maps the `type:` key of a rule to its spec class, so plugins can add types without touching
this module.
"""

from __future__ import annotations

from typing import Annotated, Any, ClassVar

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
)

from streamgate.rules.base import Rule, Severity
from streamgate.rules.expr import compile_expression
from streamgate.rules.fields import validate_path
from streamgate.timeutil import parse_duration

RuleId = Annotated[str, StringConstraints(pattern=r"^[a-z0-9][a-z0-9_.\-]*$", max_length=128)]
StreamName = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.\-]*$")]
FieldName = Annotated[str, AfterValidator(validate_path)]


def _duration(value: Any) -> Any:
    return parse_duration(value) if isinstance(value, str) else value


Duration = Annotated[int, BeforeValidator(_duration), Field(gt=0)]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class RuleSpec(StrictModel):
    kind: ClassVar[str]

    id: RuleId
    type: str
    severity: Severity | None = None
    when: str | None = None
    message: str | None = None

    @field_validator("when")
    @classmethod
    def _when_compiles(cls, value: str | None) -> str | None:
        if value is not None:
            compile_expression(value)
        return value

    def build(self, severity: Severity) -> Rule:
        raise NotImplementedError


class FieldSpec(RuleSpec):
    field: FieldName


class Defaults(StrictModel):
    severity: Severity = Severity.REJECT


class RuleFile(StrictModel):
    stream: StreamName
    version: int | str | None = None
    defaults: Defaults = Defaults()
    rules: list[Any] = Field(default_factory=list)
