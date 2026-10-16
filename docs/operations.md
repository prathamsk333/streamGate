# Operations

## Delivery semantics

StreamGate processes records in batches: consume, evaluate, produce every result, wait for
the brokers to acknowledge all of them, then commit the input offsets. So:

- **No loss.** An input offset is committed only after its output is durably written.
- **Possible duplicates.** If the process dies between producing and committing, the batch is
  processed again by whoever owns the partition next. At most one batch per partition is
  duplicated. Consumers of `clean.*` should tolerate duplicates, as with any at-least-once
  Kafka pipeline.
- **Ordering per key** is preserved: records are produced in the order they were consumed and
  the idempotent producer keeps that order across retries.

If a batch can't be delivered within `flush_timeout_s`, or the brokers reject it, the default
(`on_produce_error: exit`) is to stop without committing and exit with status 1, so your
orchestrator restarts it and the batch is retried. `on_produce_error: log` commits anyway and
keeps going; that trades the no-loss guarantee for availability, so only use it knowingly.

## Scaling

Run more instances with the same `kafka.group_id`. Kafka splits the input partitions between
them; there is nothing else to coordinate. Instances beyond the input partition count sit idle
as hot standbys.

Dedup windows are per partition and in memory. When a partition moves to another instance,
its window starts empty there, and the same happens after a restart. A duplicate arriving
right after a rebalance can therefore slip through.

## Rule changes

Edit the rule file in place (or update the ConfigMap). Within `rules_reload_s`, the change is
picked up between two batches, so a batch is always evaluated by one version. If the new file
is invalid, the error is logged, `streamgate_ruleset_reloads_total{result="error"}` goes up
and the previous rules stay active. Run `streamgate rules validate` in CI to avoid that.

## Metrics

Served on `metrics.port` at `/metrics`.

| metric | labels | |
|---|---|---|
| `streamgate_records_total` | `stream`, `outcome` | `pass`, `warn` or `reject` |
| `streamgate_violations_total` | `stream`, `rule`, `severity` | |
| `streamgate_consumer_lag` | `stream`, `partition` | records not yet processed |
| `streamgate_assigned_partitions` | `stream` | |
| `streamgate_batch_duration_seconds` | | histogram, includes produce and commit |
| `streamgate_batch_size` | | histogram |
| `streamgate_ruleset_reloads_total` | `stream`, `result` | |
| `streamgate_ruleset_info` | `stream`, `version`, `digest` | always 1; the active rules |
| `streamgate_produce_errors_total` | `topic` | |
| `streamgate_build_info` | `version` | |

Useful alerts:

- reject rate for a stream jumps, e.g.
  `rate(streamgate_records_total{outcome="reject"}[5m]) / rate(streamgate_records_total[5m]) > 0.05`.
  A sudden change is almost always a producer release.
- one rule dominating `streamgate_violations_total` tells you which field broke.
- `streamgate_consumer_lag` growing means StreamGate is falling behind; add instances.
- any increase in `streamgate_produce_errors_total` or `streamgate_ruleset_reloads_total{result="error"}`.

## Health

- `/healthz` returns 200 while the processing loop is turning, 503 if it hasn't for
  `metrics.liveness_timeout_s`. Use it as the liveness probe.
- `/readyz` returns 200 once the instance has been assigned partitions, 503 before that and
  during shutdown.

## Shutdown

On SIGTERM or SIGINT the current batch is finished, flushed and committed, the consumer leaves
the group (its partitions are reassigned immediately) and the process exits 0. Give it at
least `flush_timeout_s` before a hard kill.

## Timestamps and retention

Clean records keep the original record's timestamp. If producers send old timestamps (a
backfill, a skewed clock), time-based retention on `clean.*` counts from those timestamps, and
the records may be deleted much sooner than you expect. Set
`message.timestamp.type=LogAppendTime` on the clean topics if that is a concern.

Quarantine records are timestamped with the time they were rejected (the original timestamp
is in the envelope), so quarantine retention and `replay --from/--to` work in rejection time.
Give quarantine topics enough retention to cover how long it takes you to notice and fix a
problem; a week is a reasonable start.

## Replay

When a producer bug is fixed, or a rule was too strict, quarantined records can be checked
again and released to the clean topic:

```bash
# 1. fix the rule file (or the producer), then see what would happen
streamgate replay -c streamgate.yaml --stream orders --from 2026-10-14T10:00:00Z --dry-run

# 2. release
streamgate replay -c streamgate.yaml --stream orders --from 2026-10-14T10:00:00Z
```

What replay does:

- reads `quarantine.<stream>` from `--from` (default: the beginning) up to where it ended when
  the replay started, optionally stopping at `--to`; it never commits offsets
- rebuilds each original record (key, headers, value) from its envelope and evaluates it with
  the **current** rule file
- records that now pass are produced to the clean topic with their original key and
  timestamp, plus `sg-replayed` and `sg-source` headers; the rest are counted, and with
  `--requarantine` written back to quarantine with fresh reasons

Things to know:

- **Time-based rules** are evaluated as of the moment the record was first rejected
  (`--as-of original`), so a `freshness` rule doesn't reject everything simply because time
  has passed. Use `--as-of now` to judge records by today's clock.
- **Duplicates stay rejected.** A record quarantined by a `dedup` rule is not released, since
  replay can't know what the gateway had seen at the time. `--release-duplicates` overrides it.
- **Replay is not idempotent on the clean side.** Running it twice releases the same records
  twice. Use `--from`/`--to` to replay each window once.
- Released records arrive in the clean topic after newer records with the same key. If your
  consumers depend on per-key order, keep that in mind before replaying old windows.

## Dry run

`streamgate run --dry-run` consumes under a separate group (`<group_id>-dryrun-<instance>`),
evaluates everything and logs what it would produce, but never produces or commits. It does
not disturb a running deployment. Because it is a fresh group it starts from
`auto.offset.reset` (earliest by default).
