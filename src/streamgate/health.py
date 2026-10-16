from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest

log = logging.getLogger(__name__)


class Health:
    """Liveness and readiness as seen by the pipeline loop."""

    def __init__(self, liveness_timeout_s: float, clock: Callable[[], float] = time.monotonic):
        self.timeout = liveness_timeout_s
        self._clock = clock
        self._last_beat = clock()
        self.ready = False

    def beat(self) -> None:
        self._last_beat = self._clock()

    @property
    def alive(self) -> bool:
        return self._clock() - self._last_beat <= self.timeout


class HttpServer:
    """Serves /metrics, /healthz and /readyz on a background thread."""

    def __init__(self, bind: str, port: int, registry: CollectorRegistry, health: Health) -> None:
        routes: dict[str, Callable[[], tuple[int, str, bytes]]] = {
            "/metrics": lambda: (200, CONTENT_TYPE_LATEST, generate_latest(registry)),
            "/healthz": lambda: _status(health.alive, b"ok\n", b"stalled\n"),
            "/readyz": lambda: _status(health.ready, b"ready\n", b"not ready\n"),
        }

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                route = routes.get(self.path.split("?", 1)[0])
                status, content_type, body = route() if route else (404, "text/plain", b"")
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: object) -> None:
                pass

        self._server = ThreadingHTTPServer((bind, port), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, name="http", daemon=True)

    @property
    def port(self) -> int:
        return int(self._server.server_address[1])

    def start(self) -> None:
        self._thread.start()
        log.info("serving metrics and health on port %d", self.port)

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()


def _status(ok: bool, good: bytes, bad: bytes) -> tuple[int, str, bytes]:
    return (200, "text/plain", good) if ok else (503, "text/plain", bad)
