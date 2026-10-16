"""Rough throughput numbers for one StreamGate process.

    uv run python scripts/bench.py local          # rule evaluation + routing, no Kafka
    uv run python scripts/bench.py kafka          # end to end against localhost:9092

Uses examples/rules/orders.yaml with ~5% bad records.
"""

from __future__ import annotations

import argparse
import json
import random
import threading
import time
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from confluent_kafka import Consumer, Producer
from confluent_kafka import TopicPartition as KafkaTopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

from streamgate.app import build_app
from streamgate.codec import JsonCodec
from streamgate.config import KafkaSettings, PipelineSettings, Settings, StreamSettings
from streamgate.gateway.dedup import MemoryDedupStore
from streamgate.gateway.pipeline import Pipeline
from streamgate.gateway.ports import DeliveryCallback, TopicOffset, TopicPartition
from streamgate.gateway.router import Stream
from streamgate.health import Health
from streamgate.metrics import Metrics
from streamgate.record import Headers, Record
from streamgate.rules.loader import load_ruleset

RULES = Path(__file__).parents[1] / "examples" / "rules" / "orders.yaml"


def make_orders(n: int, seed: int = 7) -> list[bytes]:
    rng = random.Random(seed)  # noqa: S311
    now = datetime.now(UTC).isoformat(timespec="seconds")
    values = []
    for i in range(n):
        order = {
            "order_id": i,
            "customer_id": rng.randint(1, 50_000),
            "amount": round(rng.uniform(1, 5_000), 2),
            "currency": "INR",
            "placed_at": now,
            "notes": "leave at the door",
        }
        if rng.random() < 0.05:
            order["amount"] = -1
        values.append(json.dumps(order).encode())
    return values


class NullConsumer:
    def subscribe(self, *args: object) -> None: ...

    def consume(self, max_messages: int, timeout_s: float) -> list[Record]:
        return []

    def commit(self, offsets: Sequence[TopicOffset]) -> None: ...

    def watermarks(self, tp: TopicPartition, *, cached: bool = True) -> tuple[int, int]:
        return 0, -1

    def close(self) -> None: ...


class NullProducer:
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
        on_delivery(None)

    def flush(self, timeout_s: float) -> int:
        return 0


def bench_local(n: int, batch: int) -> None:
    ruleset = load_ruleset(RULES)
    stream = Stream(
        "orders",
        "raw.orders",
        "clean.orders",
        "quarantine.orders",
        rules=lambda: ruleset,
        dedup=MemoryDedupStore(),
    )
    pipeline = Pipeline(
        consumer=NullConsumer(),
        producer=NullProducer(),
        streams=[stream],
        codec=JsonCodec(),
        settings=PipelineSettings(batch_size=batch),
        metrics=Metrics(),
        health=Health(60),
        instance_id="bench",
    )
    records = [
        Record("raw.orders", i % 3, i, str(i).encode(), v) for i, v in enumerate(make_orders(n))
    ]
    started = time.perf_counter()
    for i in range(0, n, batch):
        pipeline.process(records[i : i + batch])
    elapsed = time.perf_counter() - started
    rate = n / elapsed
    print(f"local: {n} records in {elapsed:.2f}s = {rate:,.0f} records/s ({len(ruleset)} rules)")


def bench_kafka(n: int, bootstrap: str, partitions: int) -> None:
    run = uuid.uuid4().hex[:8]
    topics = [f"bench-{run}.{kind}" for kind in ("raw", "clean", "quarantine")]
    admin = AdminClient({"bootstrap.servers": bootstrap})
    for f in admin.create_topics([NewTopic(t, partitions, 1) for t in topics]).values():
        f.result()
    try:
        producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 20})
        for i, value in enumerate(make_orders(n)):
            while True:
                try:
                    producer.produce(topics[0], value=value, key=str(i).encode())
                    break
                except BufferError:
                    producer.poll(0.1)
        producer.flush()

        group = f"bench-{run}"
        settings = Settings(
            kafka=KafkaSettings(bootstrap_servers=bootstrap, group_id=group),
            streams=[
                StreamSettings(
                    name="orders",
                    rules=RULES,
                    input=topics[0],
                    clean=topics[1],
                    quarantine=topics[2],
                )
            ],
            metrics={"enabled": False},
        )
        app = build_app(settings)
        thread = threading.Thread(target=app.run, daemon=True)
        started = time.perf_counter()
        thread.start()
        checker = Consumer({"bootstrap.servers": bootstrap, "group.id": group})
        parts = [KafkaTopicPartition(topics[0], p) for p in range(partitions)]
        while sum(max(tp.offset, 0) for tp in checker.committed(parts, timeout=10)) < n:
            time.sleep(0.2)
        elapsed = time.perf_counter() - started
        checker.close()
        app.stop()
        thread.join()
        print(
            f"kafka: {n} records in {elapsed:.2f}s = {n / elapsed:,.0f} records/s "
            f"(one process, {partitions} partitions, includes startup and group join)"
        )
    finally:
        for f in admin.delete_topics(topics).values():
            f.result()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["local", "kafka"])
    parser.add_argument("-n", type=int, default=200_000)
    parser.add_argument("--batch", type=int, default=500)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--partitions", type=int, default=3)
    args = parser.parse_args()
    if args.mode == "local":
        bench_local(args.n, args.batch)
    else:
        bench_kafka(args.n, args.bootstrap, args.partitions)


if __name__ == "__main__":
    main()
