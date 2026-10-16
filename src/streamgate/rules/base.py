from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol

from streamgate.record import Record


class Severity(StrEnum):
    REJECT = "reject"
    WARN = "warn"


class Outcome(StrEnum):
    PASS = "pass"
    WARN = "warn"
    REJECT = "reject"


@dataclass(frozen=True, slots=True)
class Violation:
    rule_id: str
    severity: Severity
    message: str


class DedupStore(Protocol):
    def seen_or_add(self, partition: int, key: str, now_ms: int, window_ms: int) -> bool: ...


@dataclass(frozen=True, slots=True)
class Context:
    record: Record
    now_ms: int
    dedup: DedupStore


class Rule(ABC):
    """A single check. `check` returns None when the record passes, otherwise the reason."""

    def __init__(self, rule_id: str, severity: Severity, message: str | None = None) -> None:
        self.id = rule_id
        self.severity = severity
        self.message = message

    @abstractmethod
    def check(self, data: Mapping[str, Any], ctx: Context) -> str | None: ...

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.id}>"


class StatefulRule(Rule):
    """Runs after every other rule, and only if the record has not been rejected already.

    Keeps rejected records from polluting state such as a dedup window.
    """


class MaskingRule(Rule):
    """A rule that may rewrite the record instead of only judging it."""

    @property
    @abstractmethod
    def masks(self) -> bool: ...

    @abstractmethod
    def mask(self, data: dict[str, Any]) -> None: ...


@dataclass(slots=True)
class Verdict:
    violations: list[Violation] = field(default_factory=list)
    rewritten: dict[str, Any] | None = None
    annotations: dict[str, str] = field(default_factory=dict)

    @property
    def outcome(self) -> Outcome:
        if any(v.severity is Severity.REJECT for v in self.violations):
            return Outcome.REJECT
        if self.violations:
            return Outcome.WARN
        return Outcome.PASS
