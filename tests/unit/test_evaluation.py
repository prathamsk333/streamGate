from streamgate.codec import JsonCodec
from streamgate.evaluation import DECODE_ERROR, evaluate
from streamgate.gateway.dedup import MemoryDedupStore
from streamgate.record import Record
from streamgate.rules.base import Outcome
from tests.helpers import NOW, ruleset

RULES = ruleset("- {id: a, type: required, field: a}")


def check(value: bytes | None, **kw: bool) -> tuple[Outcome, list[str], str | None]:
    record = Record("raw.orders", 0, 0, b"k", value)
    result = evaluate(record, RULES, JsonCodec(), MemoryDedupStore(), NOW, **kw)
    return (
        result.verdict.outcome,
        [v.rule_id for v in result.verdict.violations],
        result.decode_error,
    )


def test_good_and_bad() -> None:
    assert check(b'{"a": 1}') == (Outcome.PASS, [], None)
    assert check(b"{}") == (Outcome.REJECT, ["a"], None)


def test_undecodable() -> None:
    outcome, rules, error = check(b"{oops")
    assert (outcome, rules) == (Outcome.REJECT, [DECODE_ERROR])
    assert error is not None and error.startswith("invalid JSON")


def test_tombstones() -> None:
    assert check(None) == (Outcome.PASS, [], None)
    assert check(None, reject_tombstones=True) == (
        Outcome.REJECT,
        [DECODE_ERROR],
        "record has no value",
    )
