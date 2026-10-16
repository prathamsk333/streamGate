import json
from pathlib import Path

from click.testing import CliRunner

from streamgate.cli import main

ROOT = Path(__file__).parents[2]
EXAMPLE = str(ROOT / "examples" / "rules" / "orders.yaml")
DATA = str(ROOT / "tests" / "data" / "orders.jsonl")
NOW = "2025-10-14T10:00:00Z"


def invoke(*args: str, stdin: str | None = None) -> tuple[int, str]:
    result = CliRunner().invoke(main, list(args), input=stdin)
    return result.exit_code, result.output


def test_validate_ok() -> None:
    code, out = invoke("rules", "validate", EXAMPLE)
    assert code == 0
    assert "ok" in out and "orders@1:" in out and "9 rules" in out


def test_validate_reports_failures(tmp_path: Path) -> None:
    (tmp_path / "good.yaml").write_text("stream: a\nrules: []\n")
    (tmp_path / "bad.yaml").write_text("stream: b\nrules:\n  - {id: x, type: nope}\n")
    code, out = invoke("rules", "validate", str(tmp_path))
    assert code == 1
    assert "FAIL" in out and "unknown rule type 'nope'" in out
    assert "ok" in out


def test_test_table() -> None:
    code, out = invoke("rules", "test", EXAMPLE, "--file", DATA, "--now", NOW)
    assert code == 1
    assert "customer_id.required: customer_id is null" in out
    assert "amount.range: amount=34900 exceeds maximum 10000" in out
    assert "notes.pii: possible PII (phone) in notes" in out
    assert "order.dedup: duplicate order_id=1 within 10m" in out
    assert "currency.enum" in out and "placed_at.fresh" in out
    assert "decode.error: invalid JSON" in out


def test_test_json_output() -> None:
    code, out = invoke("rules", "test", EXAMPLE, "--file", DATA, "--now", NOW, "--format", "json")
    rows = [json.loads(line) for line in out.splitlines()]
    assert code == 1
    assert [r["outcome"] for r in rows] == ["pass", "reject", "warn", "reject", "reject", "reject"]
    assert rows[5]["violations"][0]["rule"] == "decode.error"


def test_test_passing_record_from_stdin() -> None:
    record = '{"order_id": 9, "customer_id": 1, "amount": 5, "placed_at": "2025-10-14T09:59:00Z"}'
    code, out = invoke("rules", "test", EXAMPLE, "--file", "-", "--now", NOW, stdin=record + "\n\n")
    assert code == 0
    assert "pass" in out


def test_test_inline_record() -> None:
    code, _ = invoke("rules", "test", EXAMPLE, "--record", "{}", "--now", NOW)
    assert code == 1


def test_test_needs_input() -> None:
    code, out = invoke("rules", "test", EXAMPLE)
    assert code == 2 and "give at least one --record or a --file" in out


def test_test_bad_now() -> None:
    code, out = invoke("rules", "test", EXAMPLE, "--record", "{}", "--now", "noon")
    assert code == 2 and "--now" in out


def test_test_bad_rule_file(tmp_path: Path) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text("rules: []")
    code, out = invoke("rules", "test", str(path), "--record", "{}")
    assert code == 1 and "stream: Field required" in out
