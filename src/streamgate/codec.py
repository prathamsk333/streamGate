from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, NoReturn, Protocol


class DecodeError(ValueError):
    pass


class Codec(Protocol):
    name: str

    def decode(self, value: bytes) -> dict[str, Any]: ...

    def encode(self, data: Mapping[str, Any]) -> bytes: ...


def _reject_constant(name: str) -> NoReturn:
    raise ValueError(f"{name} is not valid JSON")


class JsonCodec:
    name = "json"

    def decode(self, value: bytes) -> dict[str, Any]:
        try:
            text = value.decode()
        except UnicodeDecodeError as exc:
            raise DecodeError(f"value is not valid UTF-8: {exc.reason}") from None
        try:
            data = json.loads(text, parse_constant=_reject_constant)
        except ValueError as exc:
            raise DecodeError(f"invalid JSON: {exc}") from None
        if not isinstance(data, dict):
            raise DecodeError(f"expected a JSON object, got {type(data).__name__}")
        return data

    def encode(self, data: Mapping[str, Any]) -> bytes:
        return json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode()
