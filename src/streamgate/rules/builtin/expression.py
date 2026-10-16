from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar

from pydantic import field_validator

from streamgate.rules.base import Context, Rule, Severity
from streamgate.rules.expr import compile_expression
from streamgate.rules.spec import RuleSpec


class ExpressionSpec(RuleSpec):
    kind: ClassVar[str] = "expression"
    expr: str

    @field_validator("expr")
    @classmethod
    def _compiles(cls, value: str) -> str:
        compile_expression(value)
        return value

    def build(self, severity: Severity) -> Rule:
        return ExpressionRule(self, severity)


class ExpressionRule(Rule):
    def __init__(self, spec: ExpressionSpec, severity: Severity) -> None:
        super().__init__(spec.id, severity, spec.message)
        self.expr = compile_expression(spec.expr)

    def check(self, data: Mapping[str, Any], ctx: Context) -> str | None:
        try:
            ok = self.expr(data, ctx.now_ms)
        except Exception as exc:
            return f"expression error: {type(exc).__name__}: {exc}"
        return None if ok else f"expression is false: {self.expr.source}"
