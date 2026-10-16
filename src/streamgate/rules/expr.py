"""A small, safe expression language for cross-field rules and `when:` guards.

Expressions use Python syntax but are compiled from an AST whitelist into closures, so
nothing is ever passed to `eval`. Names resolve to record fields; a missing field is None.
"""

from __future__ import annotations

import ast
import operator
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from streamgate.timeutil import parse_timestamp

Env = tuple[Mapping[str, Any], int]
Node = Callable[[Env], Any]


class ExpressionError(ValueError):
    pass


def _ts(value: Any) -> float:
    if isinstance(value, str):
        return parse_timestamp(value, "iso8601") / 1_000
    return parse_timestamp(value, "epoch_s") / 1_000


def _matches(value: str, pattern: str) -> bool:
    return re.search(pattern, value) is not None


_FUNCTIONS: dict[str, Callable[..., Any]] = {
    "len": len,
    "abs": abs,
    "min": min,
    "max": max,
    "str": str,
    "int": int,
    "float": float,
    "lower": lambda s: s.lower(),
    "upper": lambda s: s.upper(),
    "startswith": lambda s, prefix: s.startswith(prefix),
    "endswith": lambda s, suffix: s.endswith(suffix),
    "matches": _matches,
    "ts": _ts,
}
_TIME_FUNCTIONS = {"now", "age"}

_BINARY: dict[type[ast.operator], Callable[[Any, Any], Any]] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
}

_COMPARE: dict[type[ast.cmpop], Callable[[Any, Any], bool]] = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
    ast.In: lambda a, b: a in b,
    ast.NotIn: lambda a, b: a not in b,
    ast.Is: operator.is_,
    ast.IsNot: operator.is_not,
}


@dataclass(frozen=True, slots=True)
class Expression:
    source: str
    _root: Node

    def __call__(self, data: Mapping[str, Any], now_ms: int) -> Any:
        return self._root((data, now_ms))


def compile_expression(source: str) -> Expression:
    try:
        tree = ast.parse(source.strip(), mode="eval")
    except SyntaxError as exc:
        raise ExpressionError(f"invalid expression {source!r}: {exc.msg}") from None
    return Expression(source, _compile(tree.body))


def _compile(node: ast.expr) -> Node:  # noqa: PLR0911, PLR0912
    match node:
        case ast.Constant(value=value):
            if not isinstance(value, str | int | float | bool | None):
                raise ExpressionError(f"unsupported literal {value!r}")
            return lambda env: value

        case ast.Name(id=name):
            _check_name(name)
            return lambda env: _member(env[0], name)

        case ast.Attribute(value=inner, attr=attr):
            _check_name(attr)
            base = _compile(inner)
            return lambda env: _member(base(env), attr)

        case ast.Subscript(value=inner, slice=ast.Constant(value=key)) if isinstance(
            key, str | int
        ) and not isinstance(key, bool):
            base = _compile(inner)
            return lambda env: _member(base(env), key)

        case ast.List(elts=elts) | ast.Tuple(elts=elts):
            items = [_compile(e) for e in elts]
            return lambda env: [item(env) for item in items]

        case ast.BoolOp(op=op, values=values):
            parts = [_compile(v) for v in values]
            return _and(parts) if isinstance(op, ast.And) else _or(parts)

        case ast.UnaryOp(op=ast.Not(), operand=operand):
            inner_fn = _compile(operand)
            return lambda env: not inner_fn(env)

        case ast.UnaryOp(op=ast.USub(), operand=operand):
            inner_fn = _compile(operand)
            return lambda env: -inner_fn(env)

        case ast.BinOp(left=left, op=op, right=right) if type(op) in _BINARY:
            fn = _BINARY[type(op)]
            lhs, rhs = _compile(left), _compile(right)
            return lambda env: fn(lhs(env), rhs(env))

        case ast.Compare(left=left, ops=ops, comparators=comparators):
            return _compare(left, ops, comparators)

        case ast.IfExp(test=test, body=body, orelse=orelse):
            cond, yes, no = _compile(test), _compile(body), _compile(orelse)
            return lambda env: yes(env) if cond(env) else no(env)

        case ast.Call(func=ast.Name(id=name), args=args, keywords=[]):
            return _call(name, args)

    raise ExpressionError(f"{type(node).__name__} is not allowed in expressions")


def _check_name(name: str) -> None:
    if name.startswith("_"):
        raise ExpressionError(f"names starting with '_' are not allowed: {name}")


def _member(container: Any, key: str | int) -> Any:
    if isinstance(container, Mapping):
        return container.get(key)
    if isinstance(container, list) and isinstance(key, int):
        return container[key] if -len(container) <= key < len(container) else None
    return None


def _and(parts: list[Node]) -> Node:
    def run(env: Env) -> Any:
        result: Any = True
        for part in parts:
            result = part(env)
            if not result:
                return result
        return result

    return run


def _or(parts: list[Node]) -> Node:
    def run(env: Env) -> Any:
        result: Any = False
        for part in parts:
            result = part(env)
            if result:
                return result
        return result

    return run


def _compare(left: ast.expr, ops: list[ast.cmpop], comparators: list[ast.expr]) -> Node:
    for op in ops:
        if type(op) not in _COMPARE:
            raise ExpressionError(f"{type(op).__name__} is not allowed in expressions")
    first = _compile(left)
    chain = [(_COMPARE[type(op)], _compile(c)) for op, c in zip(ops, comparators, strict=True)]

    def run(env: Env) -> bool:
        lhs = first(env)
        for fn, operand in chain:
            rhs = operand(env)
            if not fn(lhs, rhs):
                return False
            lhs = rhs
        return True

    return run


def _call(name: str, args: list[ast.expr]) -> Node:
    _check_name(name)
    if any(isinstance(a, ast.Starred) for a in args):
        raise ExpressionError("*args is not allowed in expressions")
    compiled = [_compile(a) for a in args]

    if name == "now":
        if compiled:
            raise ExpressionError("now() takes no arguments")
        return lambda env: env[1] / 1_000
    if name == "age":
        if len(compiled) != 1:
            raise ExpressionError("age() takes exactly one argument")
        arg = compiled[0]
        return lambda env: env[1] / 1_000 - _ts(arg(env))

    fn = _FUNCTIONS.get(name)
    if fn is None:
        allowed = ", ".join(sorted([*_FUNCTIONS, *_TIME_FUNCTIONS]))
        raise ExpressionError(f"unknown function {name}(); available: {allowed}")
    return lambda env: fn(*(a(env) for a in compiled))
