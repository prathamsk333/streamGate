"""Wires settings into a running gateway. Used by `streamgate run` and the integration tests."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from functools import partial

from streamgate import __version__
from streamgate.codec import JsonCodec
from streamgate.config import Settings
from streamgate.gateway.dedup import MemoryDedupStore
from streamgate.gateway.dryrun import DryRunProducer
from streamgate.gateway.kafka import KafkaConsumer, KafkaProducer, TopicLayout, check_topics
from streamgate.gateway.pipeline import Pipeline
from streamgate.gateway.ports import ProducerPort
from streamgate.gateway.router import Stream
from streamgate.health import Health, HttpServer
from streamgate.metrics import Metrics
from streamgate.replay import Replayer
from streamgate.rules.loader import RuleWatcher, load_ruleset
from streamgate.rules.registry import Registry, default_registry
from streamgate.timeutil import now_ms

log = logging.getLogger(__name__)


@dataclass
class App:
    pipeline: Pipeline
    metrics: Metrics
    health: Health
    server: HttpServer | None

    def run(self) -> None:
        if self.server is not None:
            self.server.start()
        try:
            self.pipeline.run()
        finally:
            if self.server is not None:
                self.server.stop()

    def stop(self) -> None:
        self.pipeline.stop()


def build_app(
    settings: Settings,
    *,
    dry_run: bool = False,
    registry: Registry | None = None,
    check: bool = True,
) -> App:
    instance = settings.resolved_instance_id
    watcher = RuleWatcher(
        {s.name: s.rules for s in settings.streams}, registry or default_registry()
    )

    streams = []
    for spec in settings.streams:
        source, clean, quarantine = spec.topics(settings.topics)
        streams.append(
            Stream(
                spec.name,
                source,
                clean,
                quarantine,
                rules=partial(watcher.get, spec.name),
                dedup=MemoryDedupStore(settings.pipeline.dedup_max_keys),
            )
        )

    if check:
        layout = [TopicLayout(s.name, s.input, s.clean, s.quarantine) for s in streams]
        for warning in check_topics(settings.kafka.admin_config(), layout):
            log.warning(warning)

    metrics = Metrics()
    metrics.build.labels(__version__).set(1)
    for ruleset in watcher.rulesets.values():
        metrics.active_ruleset(ruleset)

    group = f"{settings.kafka.group_id}-dryrun-{instance}" if dry_run else None
    consumer = KafkaConsumer(settings.kafka.consumer_config(instance, group))
    producer: ProducerPort = (
        DryRunProducer() if dry_run else KafkaProducer(settings.kafka.producer_config(instance))
    )
    health = Health(settings.metrics.liveness_timeout_s)
    server = None
    if settings.metrics.enabled:
        server = HttpServer(settings.metrics.bind, settings.metrics.port, metrics.registry, health)

    pipeline = Pipeline(
        consumer=consumer,
        producer=producer,
        streams=streams,
        codec=JsonCodec(),
        settings=settings.pipeline,
        metrics=metrics,
        health=health,
        instance_id=instance,
        watcher=watcher,
        reload_interval_s=settings.rules_reload_s,
        commit=not dry_run,
    )
    return App(pipeline, metrics, health, server)


def build_replayer(
    settings: Settings,
    stream_name: str,
    *,
    dry_run: bool = False,
    group_id: str | None = None,
    registry: Registry | None = None,
) -> Replayer:
    spec = next((s for s in settings.streams if s.name == stream_name), None)
    if spec is None:
        known = ", ".join(s.name for s in settings.streams)
        raise ValueError(f"no stream named {stream_name!r} (configured: {known})")
    ruleset = load_ruleset(spec.rules, registry or default_registry(), stream=spec.name)
    source, clean, quarantine = spec.topics(settings.topics)
    stream = Stream(
        spec.name, source, clean, quarantine, rules=lambda: ruleset, dedup=MemoryDedupStore()
    )

    instance = settings.resolved_instance_id
    group = group_id or f"{settings.kafka.group_id}-replay-{now_ms()}"
    consumer = KafkaConsumer(settings.kafka.consumer_config(instance, group))
    producer: ProducerPort = (
        DryRunProducer() if dry_run else KafkaProducer(settings.kafka.producer_config(instance))
    )
    return Replayer(
        source=consumer, producer=producer, stream=stream, codec=JsonCodec(), instance_id=instance
    )
