import pytest

from streamgate.rules.base import Outcome
from tests.helpers import ids, ruleset, run


@pytest.mark.parametrize(
    ("kind", "text"),
    [
        ("email", "reach me at jane.doe+x@example.co.in please"),
        ("phone", "call +91 98765 43210"),
        ("phone", "9876543210"),
        ("ipv4", "from 192.168.10.4"),
        ("card", "card 4111 1111 1111 1111"),
    ],
)
def test_detects(kind: str, text: str) -> None:
    rs = ruleset(f"- {{id: p, type: pii, patterns: [{kind}]}}")
    verdict = run(rs, {"note": text})
    assert ids(verdict) == ["p"]
    assert verdict.violations[0].message == f"possible PII ({kind}) in record"


@pytest.mark.parametrize(
    ("kind", "text"),
    [
        ("card", "order 4111 1111 1111 1112"),
        ("ipv4", "version 1.2.3"),
        ("phone", "order 12345"),
        ("email", "a@b"),
    ],
)
def test_ignores_lookalikes(kind: str, text: str) -> None:
    rs = ruleset(f"- {{id: p, type: pii, patterns: [{kind}]}}")
    assert ids(run(rs, {"note": text})) == []


def test_only_listed_fields_are_scanned() -> None:
    rs = ruleset("- {id: p, type: pii, fields: [notes], patterns: [email]}")
    assert ids(run(rs, {"contact": "a@example.com"})) == []
    assert ids(run(rs, {"notes": {"nested": ["x", "a@example.com"]}})) == ["p"]


def test_mask_rewrites_a_copy() -> None:
    rs = ruleset(
        "- {id: p, type: pii, fields: [notes, meta.ip], patterns: [email, ipv4], action: mask,"
        " severity: warn}"
    )
    data = {"notes": "mail a@example.com", "meta": {"ip": "10.0.0.1"}, "other": "a@example.com"}
    verdict = run(rs, data)
    assert verdict.outcome is Outcome.WARN
    assert verdict.rewritten == {
        "notes": "mail ***",
        "meta": {"ip": "***"},
        "other": "a@example.com",
    }
    assert verdict.annotations == {"sg-masked": "p"}
    assert data["notes"] == "mail a@example.com"


def test_mask_whole_record_and_custom_regex() -> None:
    rs = ruleset(
        r"- {id: p, type: pii, regex: ['EMP-\d{4}'], action: mask, mask: '[x]', severity: warn}"
    )
    verdict = run(rs, {"a": "EMP-1234", "b": ["EMP-0001", 5]})
    assert verdict.rewritten == {"a": "[x]", "b": ["[x]", 5]}


def test_reject_does_not_mask() -> None:
    rs = ruleset("- {id: p, type: pii, patterns: [email]}")
    verdict = run(rs, {"n": "a@example.com"})
    assert verdict.outcome is Outcome.REJECT
    assert verdict.rewritten is None


def test_message_never_contains_the_match() -> None:
    rs = ruleset("- {id: p, type: pii, patterns: [email]}")
    assert "secret@example.com" not in run(rs, {"n": "secret@example.com"}).violations[0].message
