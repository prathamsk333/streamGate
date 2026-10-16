from __future__ import annotations

from collections.abc import Iterable
from importlib.metadata import entry_points

from streamgate.rules.builtin.basic import (
    EnumSpec,
    LengthSpec,
    RangeSpec,
    RegexSpec,
    RequiredSpec,
    TypeSpec,
)
from streamgate.rules.builtin.dedup import DedupSpec
from streamgate.rules.builtin.expression import ExpressionSpec
from streamgate.rules.builtin.freshness import FreshnessSpec
from streamgate.rules.builtin.pii import PiiSpec
from streamgate.rules.spec import RuleSpec

ENTRY_POINT_GROUP = "streamgate.rules"

BUILTIN_SPECS: tuple[type[RuleSpec], ...] = (
    RequiredSpec,
    TypeSpec,
    RangeSpec,
    LengthSpec,
    RegexSpec,
    EnumSpec,
    FreshnessSpec,
    ExpressionSpec,
    DedupSpec,
    PiiSpec,
)


class PluginError(RuntimeError):
    pass


class Registry:
    def __init__(self, specs: Iterable[type[RuleSpec]] = ()) -> None:
        self._specs: dict[str, type[RuleSpec]] = {}
        for spec in specs:
            self.register(spec)

    def register(self, spec: type[RuleSpec]) -> None:
        kind = getattr(spec, "kind", None)
        if not isinstance(kind, str) or not kind:
            raise ValueError(f"{spec.__name__} has no `kind`")
        if kind in self._specs:
            raise ValueError(f"rule type {kind!r} is already registered")
        self._specs[kind] = spec

    def get(self, kind: str) -> type[RuleSpec] | None:
        return self._specs.get(kind)

    @property
    def kinds(self) -> list[str]:
        return sorted(self._specs)


def default_registry(*, plugins: bool = True) -> Registry:
    registry = Registry(BUILTIN_SPECS)
    if plugins:
        for ep in entry_points(group=ENTRY_POINT_GROUP):
            try:
                spec = ep.load()
                registry.register(spec)
            except Exception as exc:
                message = f"could not load rule plugin {ep.name!r} ({ep.value}): {exc}"
                raise PluginError(message) from exc
    return registry
