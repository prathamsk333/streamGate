import pytest

from streamgate.rules.fields import MISSING, FieldPath, show

DATA = {"a": {"b": [10, {"c": "deep"}]}, "n": None}


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("a.b.0", 10),
        ("a.b.1.c", "deep"),
        ("a.b.-1.c", "deep"),
        ("n", None),
        ("missing", MISSING),
        ("a.b.5", MISSING),
        ("a.b.x", MISSING),
        ("n.x", MISSING),
    ],
)
def test_get(path: str, expected: object) -> None:
    assert FieldPath(path).get(DATA) == expected


@pytest.mark.parametrize("path", ["", "a..b", ".a", "a.", "a b"])
def test_invalid_paths(path: str) -> None:
    with pytest.raises(ValueError, match="invalid field path"):
        FieldPath(path)


def test_set() -> None:
    data = {"a": {"b": [1, 2]}}
    FieldPath("a.b.1").set(data, 9)
    FieldPath("a.c").set(data, "new")
    FieldPath("x.y").set(data, "ignored")
    assert data == {"a": {"b": [1, 9], "c": "new"}}


def test_show_truncates() -> None:
    assert show("x" * 100, limit=10) == '"xxxxxxxx…'
    assert show({"k": 1}) == '{"k": 1}'
