import json
import logging
import sys
import urllib.error
import urllib.request

from streamgate.health import Health, HttpServer
from streamgate.logs import JsonFormatter, configure
from streamgate.metrics import Metrics
from tests.helpers import ruleset


def get(url: str) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(url, timeout=5) as response:  # noqa: S310
            return response.status, response.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


def test_health_server() -> None:
    clock = [0.0]
    health = Health(10, clock=lambda: clock[0])
    metrics = Metrics()
    metrics.build.labels("1.2.3").set(1)
    server = HttpServer("127.0.0.1", 0, metrics.registry, health)
    server.start()
    base = f"http://127.0.0.1:{server.port}"
    try:
        assert get(f"{base}/readyz") == (503, "not ready\n")
        health.ready = True
        assert get(f"{base}/readyz") == (200, "ready\n")
        assert get(f"{base}/healthz") == (200, "ok\n")
        clock[0] = 11
        assert get(f"{base}/healthz") == (503, "stalled\n")
        health.beat()
        assert get(f"{base}/healthz?x=1") == (200, "ok\n")
        status, body = get(f"{base}/metrics")
        assert status == 200
        assert 'streamgate_build_info{version="1.2.3"} 1.0' in body
        assert get(f"{base}/nope")[0] == 404
    finally:
        server.stop()


def test_ruleset_info_replaces_previous() -> None:
    metrics = Metrics()
    first = ruleset("- {id: a, type: required, field: a}")
    second = ruleset("- {id: b, type: required, field: b}")
    metrics.active_ruleset(first)
    metrics.active_ruleset(second)
    samples = [
        s for m in metrics.ruleset.collect() for s in m.samples if s.labels["stream"] == "orders"
    ]
    assert [s.labels["digest"] for s in samples] == [second.digest[:12]]


def test_json_log_lines() -> None:
    record = logging.LogRecord("streamgate.x", logging.INFO, "f", 1, "hello %s", ("you",), None)
    record.stream = "orders"
    line = json.loads(JsonFormatter().format(record))
    assert line["msg"] == "hello you"
    assert line["level"] == "info"
    assert line["stream"] == "orders"
    assert line["ts"].endswith("Z")


def test_json_log_includes_exceptions() -> None:
    try:
        raise ValueError("bad")
    except ValueError:
        record = logging.LogRecord("x", logging.ERROR, "f", 1, "oops", (), sys.exc_info())
    assert "ValueError: bad" in json.loads(JsonFormatter().format(record))["exc"]


def test_configure_replaces_handlers() -> None:
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    try:
        configure("debug", "console")
        assert len(root.handlers) == 1
        assert root.level == logging.DEBUG
        configure("info", "json")
        assert isinstance(root.handlers[0].formatter, JsonFormatter)
    finally:
        root.handlers[:] = handlers
        root.setLevel(level)
