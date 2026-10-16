from __future__ import annotations

from contextlib import suppress

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

from streamgate.rules.base import Verdict
from streamgate.rules.engine import RuleSet

BATCH_SECONDS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)
BATCH_SIZES = (1, 10, 50, 100, 250, 500, 1_000, 5_000, 10_000)


class Metrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        r = self.registry
        self.records = Counter(
            "streamgate_records",
            "Records processed, by outcome.",
            ["stream", "outcome"],
            registry=r,
        )
        self.violations = Counter(
            "streamgate_violations",
            "Rule violations.",
            ["stream", "rule", "severity"],
            registry=r,
        )
        self.batch_seconds = Histogram(
            "streamgate_batch_duration_seconds",
            "Time to process one batch, including produce and commit.",
            buckets=BATCH_SECONDS,
            registry=r,
        )
        self.batch_size = Histogram(
            "streamgate_batch_size", "Records per batch.", buckets=BATCH_SIZES, registry=r
        )
        self.lag = Gauge(
            "streamgate_consumer_lag",
            "Records between the committed position and the end of the partition.",
            ["stream", "partition"],
            registry=r,
        )
        self.assigned = Gauge(
            "streamgate_assigned_partitions",
            "Partitions owned by this instance.",
            ["stream"],
            registry=r,
        )
        self.reloads = Counter(
            "streamgate_ruleset_reloads", "Rule file reloads.", ["stream", "result"], registry=r
        )
        self.ruleset = Gauge(
            "streamgate_ruleset_info",
            "Active rule set (value is always 1).",
            ["stream", "version", "digest"],
            registry=r,
        )
        self.produce_errors = Counter(
            "streamgate_produce_errors", "Failed deliveries.", ["topic"], registry=r
        )
        self.build = Gauge("streamgate_build_info", "Build information.", ["version"], registry=r)
        self._ruleset_labels: dict[str, tuple[str, str, str]] = {}

    def observe(self, stream: str, verdict: Verdict) -> None:
        self.records.labels(stream, verdict.outcome.value).inc()
        for v in verdict.violations:
            self.violations.labels(stream, v.rule_id, v.severity.value).inc()

    def batch(self, size: int, seconds: float) -> None:
        self.batch_size.observe(size)
        self.batch_seconds.observe(seconds)

    def set_lag(self, stream: str, partition: int, lag: int) -> None:
        self.lag.labels(stream, str(partition)).set(max(lag, 0))

    def forget_partition(self, stream: str, partition: int) -> None:
        with suppress(KeyError):
            self.lag.remove(stream, str(partition))

    def active_ruleset(self, ruleset: RuleSet) -> None:
        previous = self._ruleset_labels.pop(ruleset.stream, None)
        if previous is not None:
            self.ruleset.remove(*previous)
        labels = (ruleset.stream, ruleset.version or "", ruleset.digest[:12])
        self.ruleset.labels(*labels).set(1)
        self._ruleset_labels[ruleset.stream] = labels
