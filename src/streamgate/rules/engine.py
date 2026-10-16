from __future__ import annotations

import copy
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from streamgate.rules.base import (
    Context,
    MaskingRule,
    Outcome,
    Rule,
    StatefulRule,
    Verdict,
    Violation,
)
from streamgate.rules.expr import Expression

log = logging.getLogger(__name__)

MASKED_HEADER = "sg-masked"


@dataclass(frozen=True, slots=True)
class BoundRule:
    rule: Rule
    when: Expression | None = None


class RuleSet:
    """The compiled rules of one stream. Immutable; hot reload swaps whole instances."""

    def __init__(
        self,
        stream: str,
        rules: Sequence[BoundRule],
        *,
        version: str | None = None,
        digest: str = "",
    ) -> None:
        self.stream = stream
        self.version = version
        self.digest = digest
        self.rules = tuple(rules)
        self._plain = [r for r in self.rules if not isinstance(r.rule, StatefulRule)]
        self._stateful = [r for r in self.rules if isinstance(r.rule, StatefulRule)]

    @property
    def stateful_ids(self) -> frozenset[str]:
        return frozenset(r.rule.id for r in self._stateful)

    @property
    def id(self) -> str:
        return f"{self.stream}@{self.version or '-'}:{self.digest[:8]}"

    def __len__(self) -> int:
        return len(self.rules)

    def evaluate(self, data: Mapping[str, Any], ctx: Context) -> Verdict:
        verdict = Verdict()
        masked: dict[str, Any] | None = None
        masked_by: list[str] = []

        for bound in self._plain:
            violation = self._run(bound, data, ctx)
            if violation is None:
                continue
            verdict.violations.append(violation)
            rule = bound.rule
            if isinstance(rule, MaskingRule) and rule.masks:
                if masked is None:
                    masked = copy.deepcopy(dict(data))
                rule.mask(masked)
                masked_by.append(rule.id)

        if verdict.outcome is not Outcome.REJECT:
            for bound in self._stateful:
                violation = self._run(bound, data, ctx)
                if violation is not None:
                    verdict.violations.append(violation)

        if masked is not None:
            verdict.rewritten = masked
            verdict.annotations[MASKED_HEADER] = ",".join(masked_by)
        return verdict

    @staticmethod
    def _run(bound: BoundRule, data: Mapping[str, Any], ctx: Context) -> Violation | None:
        rule = bound.rule
        try:
            if bound.when is not None and not bound.when(data, ctx.now_ms):
                return None
            message = rule.check(data, ctx)
        except Exception as exc:
            # A buggy rule or guard must never take the gateway down; treat it as a failure.
            log.warning("rule %s raised %s", rule.id, exc, exc_info=True)
            message = f"rule error: {type(exc).__name__}: {exc}"
            return Violation(rule.id, rule.severity, message)
        if message is None:
            return None
        return Violation(rule.id, rule.severity, rule.message or message)
