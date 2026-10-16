from __future__ import annotations

import re
import time
from datetime import UTC, datetime
from typing import Any, Literal

TimestampFormat = Literal["iso8601", "epoch_s", "epoch_ms"]

_UNITS = {"ms": 1, "s": 1_000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}
_DURATION_PART = re.compile(r"(\d+(?:\.\d+)?)(ms|s|m|h|d)")


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def parse_duration(text: str) -> int:
    """Parse '500ms', '30s', '5m', '1h30m' and similar into milliseconds."""
    s = text.strip()
    total = 0.0
    pos = 0
    for match in _DURATION_PART.finditer(s):
        if match.start() != pos:
            break
        total += float(match.group(1)) * _UNITS[match.group(2)]
        pos = match.end()
    if not s or pos != len(s):
        raise ValueError(f"invalid duration {text!r}, expected something like 30s, 5m or 1h30m")
    return int(total)


def format_duration(ms: int) -> str:
    if ms < 1_000:
        return f"{ms}ms"
    parts = []
    rest = ms // 1_000
    for unit, size in (("d", 86_400), ("h", 3_600), ("m", 60), ("s", 1)):
        count, rest = divmod(rest, size)
        if count:
            parts.append(f"{count}{unit}")
    return "".join(parts)


def parse_timestamp(value: Any, fmt: TimestampFormat) -> int:
    """Convert a field value to epoch milliseconds. Raises ValueError with a readable reason."""
    if fmt == "iso8601":
        if not isinstance(value, str):
            raise ValueError(f"expected an ISO-8601 string, got {type(value).__name__}")
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            raise ValueError(f"not an ISO-8601 timestamp: {value!r}") from None
        if parsed.tzinfo is None:
            raise ValueError(f"timestamp {value!r} has no timezone offset")
        return int(parsed.timestamp() * 1_000)

    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"expected an epoch number, got {type(value).__name__}")
    return int(value * 1_000) if fmt == "epoch_s" else int(value)


def format_iso(ms: int) -> str:
    stamp = datetime.fromtimestamp(ms / 1_000, tz=UTC).isoformat(timespec="milliseconds")
    return stamp.replace("+00:00", "Z")
