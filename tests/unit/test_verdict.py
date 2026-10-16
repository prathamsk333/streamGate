from streamgate.rules.base import Outcome, Severity, Verdict, Violation


def test_empty_verdict_passes() -> None:
    assert Verdict().outcome is Outcome.PASS


def test_warn_only() -> None:
    verdict = Verdict([Violation("a", Severity.WARN, "meh")])
    assert verdict.outcome is Outcome.WARN


def test_any_reject_wins() -> None:
    verdict = Verdict(
        [Violation("a", Severity.WARN, "meh"), Violation("b", Severity.REJECT, "bad")]
    )
    assert verdict.outcome is Outcome.REJECT
