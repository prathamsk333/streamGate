import os
from pathlib import Path

import pytest

from streamgate.rules.loader import (
    RuleFileError,
    RuleWatcher,
    find_rule_files,
    load_ruleset,
    parse_ruleset,
)

EXAMPLE = Path(__file__).parents[2] / "examples" / "rules" / "orders.yaml"


def errors(content: str) -> list[str]:
    with pytest.raises(RuleFileError) as info:
        parse_ruleset(content.encode(), "bad.yaml")
    return info.value.errors


def test_example_loads() -> None:
    rs = load_ruleset(EXAMPLE)
    assert rs.stream == "orders"
    assert len(rs) == 9


def test_reports_every_error_with_location() -> None:
    found = errors(
        """
stream: orders
rules:
  - {id: a, type: range, field: amount}
  - {id: a, type: required, field: x}
  - {id: a, type: required, field: y}
  - {id: Bad Id, type: required, field: z}
  - {id: c, type: nope}
  - {id: d, type: regex, field: s, pattern: '('}
  - {id: e, type: expression, expr: '__import__(1)'}
  - {id: f, type: required}
  - {id: g, type: required, field: x, colour: blue}
  - {id: h, type: freshness, field: at, max_age: soon}
  - {id: i, type: required, field: x, when: 'x >'}
  - just a string
  - {id: j}
"""
    )
    assert found == [
        "rules[0] (id=a): set at least one of min, max, exclusive_min, exclusive_max",
        "rules[2] (id=a): duplicate rule id",
        "rules[3] (id=Bad Id): id: String should match pattern '^[a-z0-9][a-z0-9_.\\-]*$'",
        "rules[4] (id=c): unknown rule type 'nope' (known: dedup, enum, expression, freshness, "
        "length, pii, range, regex, required, type)",
        "rules[5] (id=d): pattern: invalid regular expression: missing ), unterminated subpattern"
        " at position 0",
        "rules[6] (id=e): expr: names starting with '_' are not allowed: __import__",
        "rules[7] (id=f): field: Field required",
        "rules[8] (id=g): colour: Extra inputs are not permitted",
        "rules[9] (id=h): max_age: invalid duration 'soon', expected something like 30s, 5m or"
        " 1h30m",
        "rules[10] (id=i): when: invalid expression 'x >': invalid syntax",
        "rules[11]: expected a mapping",
        "rules[12] (id=j): missing `type`",
    ]


def test_top_level_errors() -> None:
    assert errors("[1, 2]") == ["expected a mapping with `stream` and `rules`"]
    assert errors("stream: [x\n")[0].startswith("invalid YAML")
    assert errors("rules: []\nextra: 1\n") == [
        "stream: Field required",
        "extra: Extra inputs are not permitted",
    ]


def test_error_string_includes_source() -> None:
    with pytest.raises(RuleFileError, match=r"^bad\.yaml: stream: Field required$"):
        parse_ruleset(b"rules: []", "bad.yaml")


def test_stream_mismatch(tmp_path: Path) -> None:
    path = tmp_path / "r.yaml"
    path.write_text("stream: payments\nrules: []\n")
    with pytest.raises(RuleFileError, match="file is for stream 'payments', expected 'orders'"):
        load_ruleset(path, stream="orders")


def test_unreadable_file(tmp_path: Path) -> None:
    with pytest.raises(RuleFileError, match="No such file"):
        load_ruleset(tmp_path / "nope.yaml")


def test_find_rule_files(tmp_path: Path) -> None:
    for name in ("b.yaml", "a.yml", "notes.txt"):
        (tmp_path / name).write_text("")
    assert [p.name for p in find_rule_files(tmp_path)] == ["a.yml", "b.yaml"]
    assert find_rule_files(tmp_path / "b.yaml") == [tmp_path / "b.yaml"]


class TestWatcher:
    def write(self, path: Path, content: str, bump: int) -> None:
        path.write_text(content)
        stat = path.stat()
        os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + bump * 1_000_000_000))

    def test_reload_cycle(self, tmp_path: Path) -> None:
        path = tmp_path / "orders.yaml"
        self.write(path, "stream: orders\nrules: []\n", 0)
        watcher = RuleWatcher({"orders": path})
        first = watcher.get("orders")
        assert watcher.poll() == []

        self.write(path, "stream: orders\nrules:\n  - {id: a, type: required, field: a}\n", 1)
        (result,) = watcher.poll()
        assert result.ok and result.ruleset is watcher.get("orders")
        assert len(watcher.get("orders")) == 1 and watcher.get("orders") is not first

        self.write(path, "stream: orders\nrules:\n  - {id: a, type: nope}\n", 2)
        (failed,) = watcher.poll()
        assert not failed.ok and "unknown rule type" in (failed.error or "")
        assert len(watcher.get("orders")) == 1
        assert watcher.poll() == []

        path.unlink()
        (gone,) = watcher.poll()
        assert not gone.ok
        assert watcher.rulesets == {"orders": watcher.get("orders")}

    def test_touch_without_change_is_quiet(self, tmp_path: Path) -> None:
        path = tmp_path / "orders.yaml"
        self.write(path, "stream: orders\nrules: []\n", 0)
        watcher = RuleWatcher({"orders": path})
        self.write(path, "stream: orders\nrules: []\n", 5)
        assert watcher.poll() == []

    def test_initial_load_fails_loudly(self, tmp_path: Path) -> None:
        path = tmp_path / "orders.yaml"
        path.write_text("stream: other\nrules: []\n")
        with pytest.raises(RuleFileError):
            RuleWatcher({"orders": path})
