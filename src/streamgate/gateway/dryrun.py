from __future__ import annotations

import logging

from streamgate.gateway.ports import DeliveryCallback
from streamgate.record import Headers

log = logging.getLogger(__name__)


class DryRunProducer:
    """Logs what would be produced and reports every delivery as successful."""

    def __init__(self) -> None:
        self.count = 0

    def produce(
        self,
        topic: str,
        *,
        key: bytes | None,
        value: bytes | None,
        headers: Headers,
        timestamp_ms: int | None,
        on_delivery: DeliveryCallback,
    ) -> None:
        self.count += 1
        log.info(
            "would produce to %s",
            topic,
            extra={
                "topic": topic,
                "key": key,
                "headers": dict(headers),
                "bytes": len(value or b""),
            },
        )
        on_delivery(None)

    def flush(self, timeout_s: float) -> int:
        return 0
