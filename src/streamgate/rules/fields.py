from __future__ import annotations

import json
import re
from collections.abc import Mapping, MutableMapping
from enum import Enum
from typing import Any, Final


class _Missing(Enum):
    MISSING = "MISSING"

    def __repr__(self) -> str:
        return "MISSING"


MISSING: Final = _Missing.MISSING

_SEGMENT = re.compile(r"[^.\s]+")
_INDEX = re.compile(r"-?\d+")


class FieldPath:
    """A dotted path into a decoded record: `customer.address.pin`, `items.0.sku`."""

    __slots__ = ("parts", "text")

    def __init__(self, text: str) -> None:
        parts = tuple(text.split("."))
        if not text or not all(_SEGMENT.fullmatch(p) for p in parts):
            raise ValueError(f"invalid field path {text!r}")
        self.text = text
        self.parts = parts

    def get(self, data: Any) -> Any:
        current = data
        for part in self.parts:
            current = _step(current, part)
            if current is MISSING:
                break
        return current

    def set(self, data: MutableMapping[str, Any], value: Any) -> None:
        parent: Any = data
        for part in self.parts[:-1]:
            parent = _step(parent, part)
            if parent is MISSING:
                return
        last = self.parts[-1]
        if isinstance(parent, MutableMapping):
            parent[last] = value
        elif isinstance(parent, list) and _INDEX.fullmatch(last):
            parent[int(last)] = value

    def __str__(self) -> str:
        return self.text

    def __repr__(self) -> str:
        return f"FieldPath({self.text!r})"


def _step(current: Any, part: str) -> Any:
    if isinstance(current, Mapping):
        return current.get(part, MISSING)
    if isinstance(current, list) and _INDEX.fullmatch(part):
        index = int(part)
        if -len(current) <= index < len(current):
            return current[index]
    return MISSING


def validate_path(text: str) -> str:
    FieldPath(text)
    return text


def show(value: Any, limit: int = 60) -> str:
    """Short JSON rendering of a value for violation messages."""
    text = json.dumps(value, ensure_ascii=False, default=str)
    return text if len(text) <= limit else text[: limit - 1] + "…"
