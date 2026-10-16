from __future__ import annotations

import re
from abc import abstractmethod
from collections.abc import Callable, Mapping
from typing import Any, ClassVar, Literal

from pydantic import Field, field_validator, model_validator

from streamgate.rules.base import Context, Rule, Severity
from streamgate.rules.fields import MISSING, FieldPath, show
from streamgate.rules.spec import FieldSpec

Number = int | float
Scalar = str | int | float | bool


def is_number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


class FieldRule(Rule):
    """Base for rules about one field. Absent and null values pass; use `required` for those."""

    def __init__(self, spec: FieldSpec, severity: Severity) -> None:
        super().__init__(spec.id, severity, spec.message)
        self.field = FieldPath(spec.field)

    def check(self, data: Mapping[str, Any], ctx: Context) -> str | None:
        value = self.field.get(data)
        if value is MISSING or value is None:
            return None
        return self.check_value(value)

    @abstractmethod
    def check_value(self, value: Any) -> str | None: ...


class RequiredSpec(FieldSpec):
    kind: ClassVar[str] = "required"

    def build(self, severity: Severity) -> Rule:
        return RequiredRule(self, severity)


class RequiredRule(Rule):
    def __init__(self, spec: RequiredSpec, severity: Severity) -> None:
        super().__init__(spec.id, severity, spec.message)
        self.field = FieldPath(spec.field)

    def check(self, data: Mapping[str, Any], ctx: Context) -> str | None:
        value = self.field.get(data)
        if value is MISSING:
            return f"{self.field} is missing"
        if value is None:
            return f"{self.field} is null"
        return None


JsonType = Literal["int", "float", "number", "str", "bool", "list", "object"]


def _instance_of(*types: type) -> Callable[[Any], bool]:
    return lambda value: isinstance(value, types)


_TYPE_CHECKS: dict[str, Callable[[Any], bool]] = {
    "int": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "float": _instance_of(float),
    "number": is_number,
    "str": _instance_of(str),
    "bool": _instance_of(bool),
    "list": _instance_of(list),
    "object": _instance_of(dict),
}


def json_type_name(value: Any) -> str:
    for name in ("bool", "int", "float", "str", "list", "object"):
        if _TYPE_CHECKS[name](value):
            return name
    return type(value).__name__


class TypeSpec(FieldSpec):
    kind: ClassVar[str] = "type"
    is_: JsonType = Field(alias="is")

    def build(self, severity: Severity) -> Rule:
        return TypeRule(self, severity)


class TypeRule(FieldRule):
    def __init__(self, spec: TypeSpec, severity: Severity) -> None:
        super().__init__(spec, severity)
        self.expected = spec.is_
        self._test = _TYPE_CHECKS[spec.is_]

    def check_value(self, value: Any) -> str | None:
        if self._test(value):
            return None
        return f"{self.field} should be {self.expected}, got {json_type_name(value)}"


class RangeSpec(FieldSpec):
    kind: ClassVar[str] = "range"
    min: Number | None = None
    max: Number | None = None
    exclusive_min: Number | None = None
    exclusive_max: Number | None = None

    @model_validator(mode="after")
    def _bounds(self) -> RangeSpec:
        if self.min is not None and self.exclusive_min is not None:
            raise ValueError("min and exclusive_min cannot both be set")
        if self.max is not None and self.exclusive_max is not None:
            raise ValueError("max and exclusive_max cannot both be set")
        low = self.min if self.min is not None else self.exclusive_min
        high = self.max if self.max is not None else self.exclusive_max
        if low is None and high is None:
            raise ValueError("set at least one of min, max, exclusive_min, exclusive_max")
        if low is not None and high is not None and low > high:
            raise ValueError("lower bound is greater than upper bound")
        return self

    def build(self, severity: Severity) -> Rule:
        return RangeRule(self, severity)


class RangeRule(FieldRule):
    def __init__(self, spec: RangeSpec, severity: Severity) -> None:
        super().__init__(spec, severity)
        self.spec = spec

    def check_value(self, value: Any) -> str | None:
        if not is_number(value):
            return f"{self.field} should be a number, got {json_type_name(value)}"
        s = self.spec
        if s.min is not None and value < s.min:
            problem = f"is below minimum {s.min}"
        elif s.exclusive_min is not None and value <= s.exclusive_min:
            problem = f"must be greater than {s.exclusive_min}"
        elif s.max is not None and value > s.max:
            problem = f"exceeds maximum {s.max}"
        elif s.exclusive_max is not None and value >= s.exclusive_max:
            problem = f"must be less than {s.exclusive_max}"
        else:
            return None
        return f"{self.field}={show(value)} {problem}"


class LengthSpec(FieldSpec):
    kind: ClassVar[str] = "length"
    min: int | None = Field(default=None, ge=0)
    max: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def _bounds(self) -> LengthSpec:
        if self.min is None and self.max is None:
            raise ValueError("set at least one of min, max")
        if self.min is not None and self.max is not None and self.min > self.max:
            raise ValueError("min is greater than max")
        return self

    def build(self, severity: Severity) -> Rule:
        return LengthRule(self, severity)


class LengthRule(FieldRule):
    def __init__(self, spec: LengthSpec, severity: Severity) -> None:
        super().__init__(spec, severity)
        self.min, self.max = spec.min, spec.max

    def check_value(self, value: Any) -> str | None:
        if not isinstance(value, str | list | dict):
            return f"{self.field} has no length ({json_type_name(value)})"
        n = len(value)
        if self.min is not None and n < self.min:
            return f"{self.field} has length {n}, minimum is {self.min}"
        if self.max is not None and n > self.max:
            return f"{self.field} has length {n}, maximum is {self.max}"
        return None


class RegexSpec(FieldSpec):
    kind: ClassVar[str] = "regex"
    pattern: str
    full_match: bool = True

    @field_validator("pattern")
    @classmethod
    def _compiles(cls, value: str) -> str:
        try:
            re.compile(value)
        except re.error as exc:
            raise ValueError(f"invalid regular expression: {exc}") from None
        return value

    def build(self, severity: Severity) -> Rule:
        return RegexRule(self, severity)


class RegexRule(FieldRule):
    def __init__(self, spec: RegexSpec, severity: Severity) -> None:
        super().__init__(spec, severity)
        self.pattern = re.compile(spec.pattern)
        self._match = self.pattern.fullmatch if spec.full_match else self.pattern.search

    def check_value(self, value: Any) -> str | None:
        if not isinstance(value, str):
            return f"{self.field} should be a string, got {json_type_name(value)}"
        if self._match(value):
            return None
        return f"{self.field}={show(value)} does not match {self.pattern.pattern!r}"


def _enum_key(value: Any) -> tuple[str, Any]:
    # 1 == True in Python; keep booleans and numbers apart.
    if isinstance(value, bool):
        return ("bool", value)
    return ("value", value)


class EnumSpec(FieldSpec):
    kind: ClassVar[str] = "enum"
    values: list[Scalar] = Field(min_length=1)

    def build(self, severity: Severity) -> Rule:
        return EnumRule(self, severity)


class EnumRule(FieldRule):
    def __init__(self, spec: EnumSpec, severity: Severity) -> None:
        super().__init__(spec, severity)
        self.values = spec.values
        self._allowed = {_enum_key(v) for v in spec.values}

    def check_value(self, value: Any) -> str | None:
        if not isinstance(value, list | dict) and _enum_key(value) in self._allowed:
            return None
        return f"{self.field}={show(value)} is not one of {show(self.values)}"
