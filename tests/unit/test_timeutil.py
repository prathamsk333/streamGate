import pytest

from streamgate.timeutil import format_duration, format_iso, parse_duration, parse_timestamp


@pytest.mark.parametrize(
    ("text", "ms"),
    [
        ("500ms", 500),
        ("30s", 30_000),
        ("5m", 300_000),
        ("1h30m", 5_400_000),
        ("1.5s", 1_500),
        ("2d", 172_800_000),
    ],
)
def test_parse_duration(text: str, ms: int) -> None:
    assert parse_duration(text) == ms


@pytest.mark.parametrize("text", ["", "5", "m", "5 minutes", "5m 3s", "-5s", "5x"])
def test_parse_duration_rejects(text: str) -> None:
    with pytest.raises(ValueError, match="invalid duration"):
        parse_duration(text)


@pytest.mark.parametrize(
    ("ms", "text"),
    [(250, "250ms"), (30_000, "30s"), (5_400_000, "1h30m"), (90_061_000, "1d1h1m1s")],
)
def test_format_duration(ms: int, text: str) -> None:
    assert format_duration(ms) == text


def test_iso_timestamps() -> None:
    assert parse_timestamp("2025-10-14T10:00:00Z", "iso8601") == 1_760_436_000_000
    assert parse_timestamp("2025-10-14T15:30:00+05:30", "iso8601") == 1_760_436_000_000
    assert format_iso(1_760_436_000_123) == "2025-10-14T10:00:00.123Z"


def test_epoch_timestamps() -> None:
    assert parse_timestamp(1_760_436_000, "epoch_s") == 1_760_436_000_000
    assert parse_timestamp(1_760_436_000_000, "epoch_ms") == 1_760_436_000_000


@pytest.mark.parametrize(
    ("value", "fmt", "reason"),
    [
        ("2025-10-14T10:00:00", "iso8601", "no timezone"),
        ("yesterday", "iso8601", "not an ISO-8601"),
        (12, "iso8601", "expected an ISO-8601 string"),
        ("12", "epoch_s", "expected an epoch number"),
        (True, "epoch_ms", "expected an epoch number"),
    ],
)
def test_bad_timestamps(value: object, fmt: str, reason: str) -> None:
    with pytest.raises(ValueError, match=reason):
        parse_timestamp(value, fmt)  # type: ignore[arg-type]
