from __future__ import annotations

import hashlib
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from streamgate.rules.engine import BoundRule, RuleSet
from streamgate.rules.expr import ExpressionError, compile_expression
from streamgate.rules.registry import Registry, default_registry
from streamgate.rules.spec import RuleFile, RuleSpec

RULE_FILE_SUFFIXES = (".yaml", ".yml")


class RuleFileError(ValueError):
    def __init__(self, source: str, errors: list[str]) -> None:
        self.source = source
        self.errors = errors
        super().__init__("\n".join(f"{source}: {e}" for e in errors))


def load_ruleset(
    path: Path, registry: Registry | None = None, *, stream: str | None = None
) -> RuleSet:
    try:
        content = path.read_bytes()
    except OSError as exc:
        raise RuleFileError(str(path), [exc.strerror or str(exc)]) from None
    ruleset = parse_ruleset(content, str(path), registry)
    if stream is not None and ruleset.stream != stream:
        raise RuleFileError(
            str(path), [f"file is for stream {ruleset.stream!r}, expected {stream!r}"]
        )
    return ruleset


def parse_ruleset(content: bytes, source: str, registry: Registry | None = None) -> RuleSet:
    registry = registry or default_registry()
    try:
        raw = yaml.safe_load(content)
    except yaml.YAMLError as exc:
        raise RuleFileError(source, [f"invalid YAML: {exc}"]) from None
    if not isinstance(raw, dict):
        raise RuleFileError(source, ["expected a mapping with `stream` and `rules`"])

    try:
        model = RuleFile.model_validate(raw)
    except ValidationError as exc:
        raise RuleFileError(source, _format(exc)) from None

    errors: list[str] = []
    bound: list[BoundRule] = []
    seen: set[str] = set()
    for index, item in enumerate(model.rules):
        where = _where(index, item)
        spec = _validate_rule(item, registry, where, errors)
        if spec is None:
            continue
        if spec.id in seen:
            errors.append(f"{where}: duplicate rule id")
            continue
        seen.add(spec.id)
        try:
            rule = spec.build(spec.severity or model.defaults.severity)
            when = compile_expression(spec.when) if spec.when else None
        except (ValueError, ExpressionError) as exc:
            errors.append(f"{where}: {exc}")
            continue
        bound.append(BoundRule(rule, when))

    if errors:
        raise RuleFileError(source, errors)
    version = None if model.version is None else str(model.version)
    digest = hashlib.sha256(content).hexdigest()
    return RuleSet(model.stream, bound, version=version, digest=digest)


def _validate_rule(item: Any, registry: Registry, where: str, errors: list[str]) -> RuleSpec | None:
    if not isinstance(item, dict):
        errors.append(f"{where}: expected a mapping")
        return None
    kind = item.get("type")
    if not isinstance(kind, str):
        errors.append(f"{where}: missing `type`")
        return None
    spec_cls = registry.get(kind)
    if spec_cls is None:
        errors.append(f"{where}: unknown rule type {kind!r} (known: {', '.join(registry.kinds)})")
        return None
    try:
        return spec_cls.model_validate(item)
    except ValidationError as exc:
        errors.extend(f"{where}: {e}" for e in _format(exc))
        return None


def _where(index: int, item: Any) -> str:
    rule_id = item.get("id") if isinstance(item, dict) else None
    return f"rules[{index}] (id={rule_id})" if rule_id else f"rules[{index}]"


def _format(exc: ValidationError) -> list[str]:
    messages = []
    for error in exc.errors(include_url=False):
        loc = ".".join(str(p) for p in error["loc"])
        msg = error["msg"].removeprefix("Value error, ")
        messages.append(f"{loc}: {msg}" if loc else msg)
    return messages


def find_rule_files(path: Path) -> list[Path]:
    if path.is_dir():
        return sorted(p for p in path.iterdir() if p.suffix in RULE_FILE_SUFFIXES and p.is_file())
    return [path]


@dataclass(frozen=True, slots=True)
class ReloadResult:
    stream: str
    ruleset: RuleSet | None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None


def _signature(path: Path) -> tuple[int, int] | None:
    try:
        st = os.stat(path)
    except OSError:
        return None
    return (st.st_mtime_ns, st.st_size)


class RuleWatcher:
    """Holds the active rule set per stream and reloads files that changed on disk.

    A file that fails to load is reported once and the previous rule set stays active.
    """

    def __init__(self, sources: Mapping[str, Path], registry: Registry | None = None) -> None:
        self._registry = registry or default_registry()
        self._sources = dict(sources)
        self._active: dict[str, RuleSet] = {}
        self._seen: dict[str, tuple[int, int] | None] = {}
        for stream, path in self._sources.items():
            self._seen[stream] = _signature(path)
            self._active[stream] = load_ruleset(path, self._registry, stream=stream)

    def get(self, stream: str) -> RuleSet:
        return self._active[stream]

    @property
    def rulesets(self) -> dict[str, RuleSet]:
        return dict(self._active)

    def poll(self) -> list[ReloadResult]:
        results = []
        for stream, path in self._sources.items():
            signature = _signature(path)
            if signature == self._seen[stream]:
                continue
            self._seen[stream] = signature
            try:
                ruleset = load_ruleset(path, self._registry, stream=stream)
            except RuleFileError as exc:
                results.append(ReloadResult(stream, None, str(exc)))
                continue
            if ruleset.digest == self._active[stream].digest:
                continue
            self._active[stream] = ruleset
            results.append(ReloadResult(stream, ruleset))
        return results
