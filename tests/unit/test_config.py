from pathlib import Path

import pytest

from streamgate.config import ConfigError, Settings

BASE = """
kafka:
  bootstrap_servers: k1:9092
streams:
  - name: orders
    rules: rules/orders.yaml
"""


def load(tmp_path: Path, content: str = BASE) -> Settings:
    path = tmp_path / "streamgate.yaml"
    path.write_text(content)
    return Settings.from_file(path)


def test_defaults(tmp_path: Path) -> None:
    s = load(tmp_path)
    assert s.streams[0].topics(s.topics) == ("raw.orders", "clean.orders", "quarantine.orders")
    assert s.streams[0].rules == (tmp_path / "rules" / "orders.yaml").resolve()
    assert s.pipeline.batch_size == 500
    assert s.metrics.port == 9108


def test_client_configs(tmp_path: Path) -> None:
    s = load(
        tmp_path,
        BASE + "  - {name: payments, rules: p.yaml, input: payments.in}\n"
        "kafka:\n  bootstrap_servers: k1:9092\n  common: {security.protocol: SSL}\n"
        "  consumer: {auto.offset.reset: latest}\n  producer: {linger.ms: 50, acks: -1}\n",
    )
    consumer = s.kafka.consumer_config("me")
    assert consumer["bootstrap.servers"] == "k1:9092"
    assert consumer["security.protocol"] == "SSL"
    assert consumer["auto.offset.reset"] == "latest"
    assert consumer["enable.auto.commit"] is False
    assert consumer["group.id"] == "streamgate"
    assert s.kafka.consumer_config("me", "other")["group.id"] == "other"
    producer = s.kafka.producer_config("me")
    assert producer["linger.ms"] == 50
    assert producer["enable.idempotence"] is True
    assert producer["acks"] == "all"
    assert s.streams[1].topics(s.topics)[0] == "payments.in"


@pytest.mark.parametrize(
    ("extra", "reason"),
    [
        ("  consumer: {enable.auto.commit: true}", "'enable.auto.commit' must be False"),
        ("  producer: {acks: 1}", "'acks' must be 'all'"),
        ("  common: {enable.idempotence: false}", "'enable.idempotence' must be True"),
        ("  consumer: {group.id: x}", "set 'group.id' through the dedicated setting"),
    ],
)
def test_locked_settings(tmp_path: Path, extra: str, reason: str) -> None:
    with pytest.raises(ConfigError, match=reason):
        load(tmp_path, BASE + "kafka:\n  bootstrap_servers: k\n" + extra + "\n")


@pytest.mark.parametrize(
    ("content", "reason"),
    [
        ("kafka: {bootstrap_servers: k}\nstreams: []\n", "streams: List should have at least 1"),
        (BASE + "  - {name: orders, rules: x.yaml}\n", "stream 'orders' is defined twice"),
        (BASE + "  - {name: b, rules: x.yaml, input: raw.orders}\n", "more than one stream"),
        (BASE + "  - {name: b, rules: x.yaml, input: clean.orders}\n", "both input and output"),
        (
            "kafka: {bootstrap_servers: k}\nstreams:\n  - {name: a, rules: r, clean: raw.a}\n",
            "must differ",
        ),
        (BASE + "colour: blue\n", "unknown settings: colour"),
        (BASE + "pipeline: {batch_size: 0}\n", "pipeline.batch_size"),
        ("- just\n- a list\n", "expected a mapping"),
        ("kafka: [\n", "while parsing"),
    ],
)
def test_invalid(tmp_path: Path, content: str, reason: str) -> None:
    with pytest.raises(ConfigError, match=reason):
        load(tmp_path, content)


def test_env_overrides_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STREAMGATE_KAFKA__BOOTSTRAP_SERVERS", "k9:9092")
    monkeypatch.setenv("STREAMGATE_METRICS__PORT", "9200")
    monkeypatch.setenv("STREAMGATE_TEST_BOOTSTRAP", "ignored")
    s = load(tmp_path)
    assert s.kafka.bootstrap_servers == "k9:9092"
    assert s.metrics.port == 9200


def test_instance_id(tmp_path: Path) -> None:
    s = load(tmp_path, BASE + "instance_id: sg-7\n")
    assert s.resolved_instance_id == "sg-7"
    assert load(tmp_path).resolved_instance_id
