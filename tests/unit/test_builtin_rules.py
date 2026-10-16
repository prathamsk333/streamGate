from typing import Any

import pytest

from streamgate.gateway.dedup import MemoryDedupStore
from tests.helpers import NOW, ids, ruleset, run


def messages(rules: str, data: dict[str, Any], **kw: Any) -> list[str]:
    return [v.message for v in run(ruleset(rules), data, **kw).violations]


class TestRequired:
    RULES = "- {id: r, type: required, field: customer.id}"

    def test_present(self) -> None:
        assert messages(self.RULES, {"customer": {"id": 0}}) == []

    def test_missing(self) -> None:
        assert messages(self.RULES, {"customer": {}}) == ["customer.id is missing"]

    def test_null(self) -> None:
        assert messages(self.RULES, {"customer": {"id": None}}) == ["customer.id is null"]


class TestType:
    @pytest.mark.parametrize(
        ("kind", "good", "bad", "got"),
        [
            ("int", 3, 3.0, "float"),
            ("int", 3, True, "bool"),
            ("float", 3.5, 3, "int"),
            ("number", 3, "3", "str"),
            ("str", "x", 1, "int"),
            ("bool", False, 0, "int"),
            ("list", [], {}, "object"),
            ("object", {}, [], "list"),
        ],
    )
    def test_types(self, kind: str, good: Any, bad: Any, got: str) -> None:
        rules = f"- {{id: t, type: type, field: v, is: {kind}}}"
        assert messages(rules, {"v": good}) == []
        assert messages(rules, {"v": bad}) == [f"v should be {kind}, got {got}"]

    def test_absent_passes(self) -> None:
        assert messages("- {id: t, type: type, field: v, is: int}", {}) == []


class TestRange:
    def test_inclusive(self) -> None:
        rules = "- {id: r, type: range, field: amount, min: 1, max: 100}"
        assert messages(rules, {"amount": 1}) == []
        assert messages(rules, {"amount": 100}) == []
        assert messages(rules, {"amount": 0}) == ["amount=0 is below minimum 1"]
        assert messages(rules, {"amount": 100.5}) == ["amount=100.5 exceeds maximum 100"]

    def test_exclusive(self) -> None:
        rules = "- {id: r, type: range, field: x, exclusive_min: 0, exclusive_max: 1}"
        assert messages(rules, {"x": 0.5}) == []
        assert messages(rules, {"x": 0}) == ["x=0 must be greater than 0"]
        assert messages(rules, {"x": 1}) == ["x=1 must be less than 1"]

    def test_not_a_number(self) -> None:
        rules = "- {id: r, type: range, field: x, min: 0}"
        assert messages(rules, {"x": "5"}) == ["x should be a number, got str"]
        assert messages(rules, {"x": True}) == ["x should be a number, got bool"]


class TestLength:
    RULES = "- {id: l, type: length, field: v, min: 2, max: 3}"

    def test_strings_and_lists(self) -> None:
        assert messages(self.RULES, {"v": "ab"}) == []
        assert messages(self.RULES, {"v": [1, 2, 3]}) == []
        assert messages(self.RULES, {"v": "a"}) == ["v has length 1, minimum is 2"]
        assert messages(self.RULES, {"v": "abcd"}) == ["v has length 4, maximum is 3"]

    def test_no_length(self) -> None:
        assert messages(self.RULES, {"v": 12}) == ["v has no length (int)"]


class TestRegex:
    def test_full_match_by_default(self) -> None:
        rules = r"- {id: x, type: regex, field: pin, pattern: '\d{6}'}"
        assert messages(rules, {"pin": "560001"}) == []
        assert messages(rules, {"pin": "5600011"}) == [r"""pin="5600011" does not match '\\d{6}'"""]

    def test_search(self) -> None:
        rules = "- {id: x, type: regex, field: s, pattern: abc, full_match: false}"
        assert messages(rules, {"s": "xxabcxx"}) == []

    def test_not_a_string(self) -> None:
        rules = "- {id: x, type: regex, field: s, pattern: abc}"
        assert messages(rules, {"s": 1}) == ["s should be a string, got int"]


class TestEnum:
    RULES = "- {id: e, type: enum, field: s, values: [NEW, PAID, 1]}"

    def test_member(self) -> None:
        assert messages(self.RULES, {"s": "PAID"}) == []
        assert messages(self.RULES, {"s": 1}) == []

    def test_bool_is_not_one(self) -> None:
        assert messages(self.RULES, {"s": True}) == ['s=true is not one of ["NEW", "PAID", 1]']

    def test_unhashable(self) -> None:
        assert len(messages(self.RULES, {"s": ["PAID"]})) == 1


class TestFreshness:
    RULES = "- {id: f, type: freshness, field: at, max_age: 5m}"

    def test_fresh(self) -> None:
        assert messages(self.RULES, {"at": "2025-10-14T09:56:00Z"}) == []

    def test_stale(self) -> None:
        assert messages(self.RULES, {"at": "2025-10-14T09:00:00Z"}) == [
            "at is 1h old, max age is 5m"
        ]

    def test_future(self) -> None:
        assert messages(self.RULES, {"at": "2025-10-14T10:05:00Z"}) == ["at is 5m in the future"]
        assert messages(self.RULES, {"at": "2025-10-14T10:00:20Z"}) == []

    def test_unparseable(self) -> None:
        assert messages(self.RULES, {"at": "soon"}) == ["at: not an ISO-8601 timestamp: 'soon'"]

    def test_epoch(self) -> None:
        rules = "- {id: f, type: freshness, field: at, format: epoch_ms, max_age: 1s}"
        assert messages(rules, {"at": NOW - 500}) == []
        assert messages(rules, {"at": NOW - 2_000}) == ["at is 2s old, max age is 1s"]

    def test_kafka_timestamp(self) -> None:
        rules = "- {id: f, type: freshness, source: kafka, max_age: 1m}"
        assert messages(rules, {}, timestamp_ms=NOW - 1_000) == []
        assert messages(rules, {}, timestamp_ms=NOW - 120_000) == [
            "kafka timestamp is 2m old, max age is 1m"
        ]
        assert messages(rules, {}) == ["record has no Kafka timestamp"]


class TestExpression:
    def test_true_and_false(self) -> None:
        rules = "- {id: x, type: expression, expr: 'end >= start'}"
        assert messages(rules, {"start": 1, "end": 2}) == []
        assert messages(rules, {"start": 3, "end": 2}) == ["expression is false: end >= start"]

    def test_runtime_error_is_a_violation(self) -> None:
        rules = "- {id: x, type: expression, expr: 'end >= start'}"
        (message,) = messages(rules, {"start": 1})
        assert message.startswith("expression error: TypeError")


class TestDedup:
    RULES = "- {id: d, type: dedup, key: order_id, window: 10m}"

    def test_duplicate_within_window(self) -> None:
        rs, store = ruleset(self.RULES), MemoryDedupStore()
        assert ids(run(rs, {"order_id": 7}, dedup=store)) == []
        assert ids(run(rs, {"order_id": 7}, dedup=store, now=NOW + 60_000)) == ["d"]
        assert ids(run(rs, {"order_id": 7}, dedup=store, now=NOW + 11 * 60_000)) == []

    def test_partitions_are_independent(self) -> None:
        rs, store = ruleset(self.RULES), MemoryDedupStore()
        assert ids(run(rs, {"order_id": 7}, dedup=store, partition=0)) == []
        assert ids(run(rs, {"order_id": 7}, dedup=store, partition=1)) == []

    def test_missing_key_passes(self) -> None:
        assert messages(self.RULES, {}) == []
