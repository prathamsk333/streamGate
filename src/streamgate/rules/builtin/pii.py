from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from typing import Any, ClassVar, Literal

from pydantic import Field, field_validator, model_validator

from streamgate.rules.base import Context, MaskingRule, Rule, Severity
from streamgate.rules.fields import MISSING, FieldPath
from streamgate.rules.spec import FieldName, RuleSpec

PiiKind = Literal["email", "phone", "ipv4", "card"]

_PATTERNS: dict[str, re.Pattern[str]] = {
    "email": re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"),
    "phone": re.compile(r"(?<![\w+])(?:\+\d{1,3}[ \-]?)?(?:\d[ \-]?){9,11}\d(?!\w)"),
    "ipv4": re.compile(r"\b(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)\b"),
    "card": re.compile(r"(?<!\d)(?:\d[ \-]?){12,18}\d(?!\d)"),
}


def _luhn(candidate: str) -> bool:
    digits = [int(c) for c in candidate if c.isdigit()]
    checksum = 0
    for i, digit in enumerate(reversed(digits)):
        doubled = digit * 2 if i % 2 else digit
        checksum += doubled - 9 if doubled > 9 else doubled
    return checksum % 10 == 0


class PiiSpec(RuleSpec):
    kind: ClassVar[str] = "pii"
    fields: list[FieldName] = Field(default_factory=list)
    patterns: list[PiiKind] = Field(default_factory=list)
    regex: list[str] = Field(default_factory=list)
    action: Literal["reject", "mask"] = "reject"
    mask: str = "***"

    @field_validator("regex")
    @classmethod
    def _compiles(cls, values: list[str]) -> list[str]:
        for value in values:
            try:
                re.compile(value)
            except re.error as exc:
                raise ValueError(f"invalid regular expression {value!r}: {exc}") from None
        return values

    @model_validator(mode="after")
    def _something_to_find(self) -> PiiSpec:
        if not self.patterns and not self.regex:
            raise ValueError("set at least one of patterns, regex")
        return self

    def build(self, severity: Severity) -> Rule:
        return PiiRule(self, severity)


class _Detector:
    def __init__(self, name: str, pattern: re.Pattern[str]) -> None:
        self.name = name
        self.pattern = pattern
        self.valid: Callable[[str], bool] = _luhn if name == "card" else lambda _: True

    def found(self, text: str) -> bool:
        return any(self.valid(m.group()) for m in self.pattern.finditer(text))

    def redact(self, text: str, mask: str) -> str:
        return self.pattern.sub(lambda m: mask if self.valid(m.group()) else m.group(), text)


class PiiRule(MaskingRule):
    """Finds personal data in string values, anywhere below the configured fields.

    Matched values are never put into the violation message.
    """

    def __init__(self, spec: PiiSpec, severity: Severity) -> None:
        super().__init__(spec.id, severity, spec.message)
        self.fields = [FieldPath(f) for f in spec.fields]
        self.detectors = [_Detector(k, _PATTERNS[k]) for k in spec.patterns]
        self.detectors += [_Detector("custom", re.compile(r)) for r in spec.regex]
        self.replacement = spec.mask
        self._masks = spec.action == "mask"

    @property
    def masks(self) -> bool:
        return self._masks

    def check(self, data: Mapping[str, Any], ctx: Context) -> str | None:
        texts = self._strings(data)
        kinds = sorted({d.name for d in self.detectors if any(d.found(t) for t in texts)})
        if not kinds:
            return None
        where = ", ".join(str(f) for f in self.fields) or "record"
        return f"possible PII ({', '.join(kinds)}) in {where}"

    def mask(self, data: dict[str, Any]) -> None:
        if not self.fields:
            for key in list(data):
                data[key] = self._redact(data[key])
            return
        for path in self.fields:
            value = path.get(data)
            if value is not MISSING:
                path.set(data, self._redact(value))

    def _strings(self, data: Mapping[str, Any]) -> list[str]:
        roots = [data] if not self.fields else [f.get(data) for f in self.fields]
        found: list[str] = []
        for root in roots:
            _collect(root, found)
        return found

    def _redact(self, value: Any) -> Any:
        if isinstance(value, str):
            for detector in self.detectors:
                value = detector.redact(value, self.replacement)
            return value
        if isinstance(value, list):
            return [self._redact(v) for v in value]
        if isinstance(value, dict):
            return {k: self._redact(v) for k, v in value.items()}
        return value


def _collect(value: Any, out: list[str]) -> None:
    if isinstance(value, str):
        out.append(value)
    elif isinstance(value, list):
        for item in value:
            _collect(item, out)
    elif isinstance(value, Mapping):
        for item in value.values():
            _collect(item, out)
