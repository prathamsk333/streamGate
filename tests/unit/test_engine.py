from collections.abc import Mapping
from typing import Any, ClassVar

from streamgate.gateway.dedup import MemoryDedupStore
from streamgate.rules.base import Context, Outcome, Rule, Severity
from streamgate.rules.loader import parse_ruleset
from streamgate.rules.registry import BUILTIN_SPECS, Registry
from streamgate.rules.spec import RuleSpec
from tests.helpers import NOW, ids, ruleset, run


def test_collects_every_violation() -> None:
    rs = ruleset(
        """
        - {id: a, type: required, field: a}
        - {id: b, type: required, field: b}
        - {id: c, type: required, field: c, severity: warn}
        """
    )
    verdict = run(rs, {})
    assert ids(verdict) == ["a", "b", "c"]
    assert verdict.outcome is Outcome.REJECT


def test_warn_only() -> None:
    rs = ruleset("- {id: a, type: required, field: a, severity: warn}")
    assert run(rs, {}).outcome is Outcome.WARN


def test_defaults_severity() -> None:
    content = (
        b"stream: s\ndefaults: {severity: warn}\nrules:\n  - {id: a, type: required, field: a}\n"
    )
    rs = parse_ruleset(content, "t.yaml")
    assert run(rs, {}).violations[0].severity is Severity.WARN


def test_custom_message() -> None:
    rs = ruleset("- {id: a, type: required, field: a, message: we need an a}")
    assert run(rs, {}).violations[0].message == "we need an a"


def test_when_guard() -> None:
    rs = ruleset("- {id: pin, type: required, field: pin, when: \"country == 'IN'\"}")
    assert ids(run(rs, {"country": "US"})) == []
    assert ids(run(rs, {"country": "IN"})) == ["pin"]


def test_guard_errors_count_as_violations() -> None:
    rs = ruleset("- {id: a, type: required, field: a, when: 'x > 1'}")
    (violation,) = run(rs, {"x": "str"}).violations
    assert violation.message.startswith("rule error: TypeError")


def test_dedup_skipped_for_rejected_records() -> None:
    rs = ruleset(
        """
        - {id: d, type: dedup, key: id, window: 1m}
        - {id: amount, type: range, field: amount, min: 1}
        """
    )
    store = MemoryDedupStore()
    assert ids(run(rs, {"id": 1, "amount": 0}, dedup=store)) == ["amount"]
    assert ids(run(rs, {"id": 1, "amount": 5}, dedup=store)) == []
    assert ids(run(rs, {"id": 1, "amount": 5}, dedup=store)) == ["d"]


def test_dedup_runs_after_warnings() -> None:
    rs = ruleset(
        """
        - {id: d, type: dedup, key: id, window: 1m}
        - {id: w, type: required, field: x, severity: warn}
        """
    )
    store = MemoryDedupStore()
    run(rs, {"id": 1}, dedup=store)
    assert ids(run(rs, {"id": 1}, dedup=store)) == ["w", "d"]


class ExplodingSpec(RuleSpec):
    kind: ClassVar[str] = "explode"

    def build(self, severity: Severity) -> Rule:
        return Exploding(self.id, severity)


class Exploding(Rule):
    def check(self, data: Mapping[str, Any], ctx: Context) -> str | None:
        raise RuntimeError("boom")


def test_buggy_rule_becomes_a_violation() -> None:
    registry = Registry([*BUILTIN_SPECS, ExplodingSpec])
    rs = parse_ruleset(b"stream: s\nrules:\n  - {id: x, type: explode}\n", "t.yaml", registry)
    (violation,) = run(rs, {}).violations
    assert violation.message == "rule error: RuntimeError: boom"
    assert violation.severity is Severity.REJECT


def test_ruleset_identity() -> None:
    rs = parse_ruleset(b"stream: orders\nversion: 3\nrules: []\n", "t.yaml")
    assert rs.id.startswith("orders@3:")
    assert len(rs) == 0
    assert run(rs, {}, now=NOW).outcome is Outcome.PASS
