from __future__ import annotations

import os
import socket

import pytest


def _reachable(bootstrap: str, timeout: float = 1.0) -> bool:
    host, _, port = bootstrap.split(",", maxsplit=1)[0].rpartition(":")
    try:
        with socket.create_connection((host or "localhost", int(port)), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def bootstrap() -> str:
    servers = os.environ.get("STREAMGATE_TEST_BOOTSTRAP", "localhost:9092")
    if not _reachable(servers):
        pytest.skip(f"no Kafka broker at {servers}")
    return servers
