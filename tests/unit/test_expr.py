import pytest

from streamgate.rules.expr import ExpressionError, compile_expression

NOW = 1_760_436_000_000
DATA = {
    "amount": 120,
    "status": "PAID",
    "customer": {"tier": "gold", "tags": ["a", "b"]},
    "placed_at": "2025-10-14T09:55:00Z",
    "shipped_at": None,
}


def ev(source: str) -> object:
    return compile_expression(source)(DATA, NOW)


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("amount > 100", True),
        ("0 < amount <= 120", True),
        ("amount * 2 - 40 == 200", True),
        ("amount % 7", 1),
        ("status in ['PAID', 'SHIPPED']", True),
        ("status not in ('NEW',)", True),
        ("customer.tier == 'gold'", True),
        ("customer['tier']", "gold"),
        ("customer.tags[1]", "b"),
        ("customer.tags[9]", None),
        ("missing is None", True),
        ("missing.deeper is None", True),
        ("shipped_at is None or shipped_at >= placed_at", True),
        ("not status", False),
        ("-amount", -120),
        ("'big' if amount > 100 else 'small'", "big"),
        ("len(customer.tags)", 2),
        ("lower(status) == 'paid'", True),
        ("startswith(status, 'PA') and endswith(status, 'ID')", True),
        ("matches(status, '^P')", True),
        ("now()", NOW / 1000),
        ("age(placed_at)", 300.0),
        ("ts(placed_at) < now()", True),
        ("max(amount, 500)", 500),
        ("amount and status", "PAID"),
        ("missing or 'fallback'", "fallback"),
    ],
)
def test_evaluates(source: str, expected: object) -> None:
    assert ev(source) == expected


@pytest.mark.parametrize(
    ("source", "reason"),
    [
        ("__import__('os')", "not allowed"),
        ("amount.__class__", "not allowed"),
        ("_secret", "not allowed"),
        ("open('x')", "unknown function"),
        ("status.lower()", "not allowed"),
        ("[x for x in customer.tags]", "not allowed"),
        ("lambda: 1", "not allowed"),
        ("amount ** 99999", "not allowed"),
        ("f'{amount}'", "not allowed"),
        ("len(*customer.tags)", "not allowed"),
        ("len(x=1)", "not allowed"),
        ("now(1)", "takes no arguments"),
        ("age()", "exactly one argument"),
        ("amount >", "invalid expression"),
        ("amount := 1", "invalid expression"),
        ("customer[amount]", "not allowed"),
        ("b'bytes'", "unsupported literal"),
    ],
)
def test_rejects_at_compile_time(source: str, reason: str) -> None:
    with pytest.raises(ExpressionError, match=reason):
        compile_expression(source)


def test_runtime_errors_surface_as_exceptions() -> None:
    with pytest.raises(TypeError):
        ev("shipped_at > 3")
