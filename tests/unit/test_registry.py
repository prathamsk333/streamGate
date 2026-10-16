from typing import Any, ClassVar

import pytest

from streamgate.rules import registry as registry_module
from streamgate.rules.base import Rule, Severity
from streamgate.rules.registry import PluginError, Registry, default_registry
from streamgate.rules.spec import RuleSpec


class PluginSpec(RuleSpec):
    kind: ClassVar[str] = "plugin"

    def build(self, severity: Severity) -> Rule:
        raise NotImplementedError


class FakeEntryPoint:
    def __init__(self, name: str, target: Any) -> None:
        self.name, self.value, self._target = name, f"pkg:{name}", target

    def load(self) -> Any:
        if isinstance(self._target, Exception):
            raise self._target
        return self._target


def test_builtins() -> None:
    assert default_registry(plugins=False).kinds == [
        "dedup",
        "enum",
        "expression",
        "freshness",
        "length",
        "pii",
        "range",
        "regex",
        "required",
        "type",
    ]


def test_duplicate_kind() -> None:
    reg = Registry([PluginSpec])
    with pytest.raises(ValueError, match="already registered"):
        reg.register(PluginSpec)


def test_spec_without_kind() -> None:
    class NoKind(RuleSpec):
        pass

    with pytest.raises(ValueError, match="has no `kind`"):
        Registry([NoKind])


def test_entry_point_plugins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        registry_module, "entry_points", lambda group: [FakeEntryPoint("plugin", PluginSpec)]
    )
    assert default_registry().get("plugin") is PluginSpec


def test_broken_plugin(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        registry_module, "entry_points", lambda group: [FakeEntryPoint("bad", ImportError("nope"))]
    )
    with pytest.raises(PluginError, match="could not load rule plugin 'bad' \\(pkg:bad\\): nope"):
        default_registry()
