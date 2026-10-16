from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
import yaml
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

from streamgate.app import App, build_app
from streamgate.config import Settings
from streamgate.gateway.kafka import to_record
from streamgate.record import Record


def wait_for(predicate: Callable[[], bool], timeout: float = 30, what: str = "condition") -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    raise AssertionError(f"timed out waiting for {what}")


@dataclass
class Kafka:
    bootstrap: str
    created: list[str] = field(default_factory=list)

    admin: AdminClient = field(init=False)

    def __post_init__(self) -> None:
        self.admin = AdminClient({"bootstrap.servers": self.bootstrap})

    def topics(self, stream: str, partitions: int = 3, clean_partitions: int | None = None) -> None:
        layout = {
            f"raw.{stream}": partitions,
            f"clean.{stream}": clean_partitions or partitions,
            f"quarantine.{stream}": partitions,
        }
        new = [NewTopic(name, num_partitions=n, replication_factor=1) for name, n in layout.items()]
        for future in self.admin.create_topics(new).values():
            future.result(timeout=30)
        self.created.extend(layout)

    def produce(self, topic: str, messages: list[dict[str, Any]]) -> None:
        producer = Producer({"bootstrap.servers": self.bootstrap})
        for m in messages:
            producer.produce(topic, **m)
        assert producer.flush(30) == 0

    def read(
        self, topic: str, count: int, settle: float = 1.0, timeout: float = 30
    ) -> list[Record]:
        consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap,
                "group.id": f"reader-{uuid.uuid4().hex}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([topic])
        records: list[Record] = []
        deadline = time.monotonic() + timeout
        settled_at: float | None = None
        try:
            while time.monotonic() < deadline:
                for msg in consumer.consume(num_messages=500, timeout=0.2):
                    if msg.error() is None:
                        records.append(to_record(msg))
                if len(records) >= count:
                    settled_at = settled_at or time.monotonic()
                    if time.monotonic() - settled_at >= settle:
                        break
        finally:
            consumer.close()
        assert len(records) >= count, f"{topic}: expected {count} records, got {len(records)}"
        return records

    def committed(self, group: str, topic: str) -> dict[int, int]:
        meta = self.admin.list_topics(topic, timeout=10).topics[topic]
        consumer = Consumer({"bootstrap.servers": self.bootstrap, "group.id": group})
        try:
            parts = [TopicPartition(topic, p) for p in meta.partitions]
            return {tp.partition: tp.offset for tp in consumer.committed(parts, timeout=10)}
        finally:
            consumer.close()


@pytest.fixture
def kafka(bootstrap: str) -> Iterator[Kafka]:
    k = Kafka(bootstrap)
    yield k
    if k.created:
        for future in k.admin.delete_topics(k.created).values():
            future.result(timeout=30)


@pytest.fixture
def stream_name() -> str:
    return f"it{uuid.uuid4().hex[:10]}"


@dataclass
class Running:
    app: App
    thread: threading.Thread
    error: list[BaseException]

    def stop(self) -> None:
        self.app.stop()
        self.thread.join(timeout=60)
        assert not self.thread.is_alive()


def write_config(
    tmp_path: Path,
    bootstrap: str,
    stream: str,
    rules: str,
    *,
    group: str,
    **overrides: Any,
) -> Settings:
    rules_path = tmp_path / f"{stream}.yaml"
    rules_path.write_text(f"stream: {stream}\nrules:\n{rules}")
    config: dict[str, Any] = {
        "kafka": {"bootstrap_servers": bootstrap, "group_id": group},
        "streams": [{"name": stream, "rules": rules_path.name}],
        "pipeline": {"poll_timeout_s": 0.2, "batch_size": 100},
        "metrics": {"enabled": False},
        "logging": {"level": "warning"},
    }
    for section, values in overrides.items():
        if isinstance(values, dict):
            config.setdefault(section, {}).update(values)
        else:
            config[section] = values
    path = tmp_path / f"streamgate-{uuid.uuid4().hex[:6]}.yaml"
    path.write_text(yaml.safe_dump(config))
    return Settings.from_file(path)


def start(settings: Settings) -> Running:
    app = build_app(settings)
    errors: list[BaseException] = []

    def target() -> None:
        try:
            app.run()
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    wait_for(lambda: bool(app.pipeline.assigned) or bool(errors), what="partition assignment")
    return Running(app, thread, errors)
