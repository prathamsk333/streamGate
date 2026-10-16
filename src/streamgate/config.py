from __future__ import annotations

import secrets
import socket
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

from streamgate.rules.spec import StreamName

KafkaValue = str | int | float | bool

# These keep StreamGate's delivery guarantees; overriding them would silently break them.
LOCKED_CONSUMER: dict[str, KafkaValue] = {
    "enable.auto.commit": False,
    "enable.auto.offset.store": False,
}
LOCKED_PRODUCER: dict[str, KafkaValue] = {
    "enable.idempotence": True,
    "acks": "all",
}
MANAGED_KEYS = {"bootstrap.servers", "group.id", "client.id"}

CONSUMER_DEFAULTS: dict[str, KafkaValue] = {
    "auto.offset.reset": "earliest",
    "max.poll.interval.ms": 300_000,
}
PRODUCER_DEFAULTS: dict[str, KafkaValue] = {
    "compression.type": "lz4",
    "linger.ms": 5,
}


class ConfigError(ValueError):
    pass


class Section(BaseModel):
    model_config = ConfigDict(extra="forbid")


def _normalise(value: KafkaValue) -> str:
    text = str(value).lower()
    return "all" if text == "-1" else text


def _check_overrides(
    section: str, values: dict[str, KafkaValue], locked: dict[str, KafkaValue]
) -> None:
    for key, value in values.items():
        if key in MANAGED_KEYS:
            raise ValueError(f"{section}: set {key!r} through the dedicated setting, not here")
        if key in locked and _normalise(value) != _normalise(locked[key]):
            raise ValueError(f"{section}: {key!r} must be {locked[key]!r} for StreamGate to work")


class KafkaSettings(Section):
    bootstrap_servers: str
    group_id: str = "streamgate"
    common: dict[str, KafkaValue] = Field(default_factory=dict)
    consumer: dict[str, KafkaValue] = Field(default_factory=dict)
    producer: dict[str, KafkaValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _overrides(self) -> KafkaSettings:
        _check_overrides("kafka.common", self.common, LOCKED_CONSUMER | LOCKED_PRODUCER)
        _check_overrides("kafka.consumer", self.consumer, LOCKED_CONSUMER)
        _check_overrides("kafka.producer", self.producer, LOCKED_PRODUCER)
        return self

    def admin_config(self) -> dict[str, Any]:
        return {"bootstrap.servers": self.bootstrap_servers, **self.common}

    def consumer_config(self, client_id: str, group_id: str | None = None) -> dict[str, Any]:
        return {
            **CONSUMER_DEFAULTS,
            **self.admin_config(),
            **self.consumer,
            **LOCKED_CONSUMER,
            "group.id": group_id or self.group_id,
            "client.id": client_id,
        }

    def producer_config(self, client_id: str) -> dict[str, Any]:
        return {
            **PRODUCER_DEFAULTS,
            **self.admin_config(),
            **self.producer,
            **LOCKED_PRODUCER,
            "client.id": client_id,
        }


class TopicPatterns(Section):
    input: str = "raw.{name}"
    clean: str = "clean.{name}"
    quarantine: str = "quarantine.{name}"


class StreamSettings(Section):
    name: StreamName
    rules: Path
    input: str | None = None
    clean: str | None = None
    quarantine: str | None = None

    def topics(self, patterns: TopicPatterns) -> tuple[str, str, str]:
        return (
            self.input or patterns.input.format(name=self.name),
            self.clean or patterns.clean.format(name=self.name),
            self.quarantine or patterns.quarantine.format(name=self.name),
        )


class PipelineSettings(Section):
    batch_size: int = Field(500, ge=1, le=100_000)
    poll_timeout_s: float = Field(1.0, gt=0)
    flush_timeout_s: float = Field(30.0, gt=0)
    warn_header: bool = False
    reject_tombstones: bool = False
    on_produce_error: Literal["exit", "log"] = "exit"
    dedup_max_keys: int = Field(1_000_000, ge=1)


class MetricsSettings(Section):
    enabled: bool = True
    bind: str = "0.0.0.0"  # noqa: S104 - it is a server meant to be scraped
    port: int = Field(9108, ge=0, le=65535)
    liveness_timeout_s: float = Field(120.0, gt=0)


class LoggingSettings(Section):
    level: Literal["debug", "info", "warning", "error"] = "info"
    format: Literal["json", "console"] = "json"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="STREAMGATE_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    kafka: KafkaSettings
    topics: TopicPatterns = TopicPatterns()
    streams: list[StreamSettings] = Field(min_length=1)
    pipeline: PipelineSettings = PipelineSettings()
    metrics: MetricsSettings = MetricsSettings()
    logging: LoggingSettings = LoggingSettings()
    rules_reload_s: float = Field(5.0, ge=0)
    instance_id: str | None = None

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # Environment variables override the config file.
        return (env_settings, init_settings)

    @model_validator(mode="after")
    def _topology(self) -> Settings:
        names: set[str] = set()
        inputs: set[str] = set()
        outputs: set[str] = set()
        for stream in self.streams:
            if stream.name in names:
                raise ValueError(f"stream {stream.name!r} is defined twice")
            names.add(stream.name)
            source, clean, quarantine = stream.topics(self.topics)
            if len({source, clean, quarantine}) < 3:
                raise ValueError(f"stream {stream.name!r}: input, clean and quarantine must differ")
            if source in inputs:
                raise ValueError(f"topic {source!r} is the input of more than one stream")
            inputs.add(source)
            outputs.update((clean, quarantine))
        if looped := inputs & outputs:
            raise ValueError(f"topics used as both input and output: {', '.join(sorted(looped))}")
        return self

    @property
    def resolved_instance_id(self) -> str:
        return self.instance_id or f"{socket.gethostname()}-{secrets.token_hex(2)}"

    @classmethod
    def from_file(cls, path: Path) -> Settings:
        try:
            raw = yaml.safe_load(path.read_text()) or {}
        except (OSError, yaml.YAMLError) as exc:
            raise ConfigError(f"{path}: {exc}") from None
        if not isinstance(raw, dict):
            raise ConfigError(f"{path}: expected a mapping at the top level")
        unknown = set(raw) - set(cls.model_fields)
        if unknown:
            raise ConfigError(f"{path}: unknown settings: {', '.join(sorted(unknown))}")
        for stream in raw.get("streams") or []:
            if isinstance(stream, dict) and "rules" in stream:
                stream["rules"] = str((path.parent / str(stream["rules"])).resolve())
        try:
            return cls(**raw)
        except ValidationError as exc:
            raise ConfigError(_format(path, exc)) from None


def _format(path: Path, exc: ValidationError) -> str:
    lines = [f"{path}: invalid configuration"]
    for error in exc.errors(include_url=False):
        loc = ".".join(str(p) for p in error["loc"]) or "<root>"
        lines.append(f"  {loc}: {error['msg'].removeprefix('Value error, ')}")
    return "\n".join(lines)
