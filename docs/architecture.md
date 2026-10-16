# Architecture

StreamGate is an ordinary Kafka client: a consumer group on the input topics and a producer to
the clean and quarantine topics. It runs no broker and stores no state that can't be rebuilt.

```
                         ┌──────────────────── streamgate process ────────────────────┐
raw.orders ──consume──▶  │ KafkaConsumer ─▶ codec ─▶ RuleSet ─▶ router ─▶ KafkaProducer │ ──▶ clean.orders
                         │      ▲                       ▲                   │          │ ──▶ quarantine.orders
                         │      └──── commit after ─────┼──── flush ────────┘          │
                         │                         RuleWatcher      Metrics / Health   │
                         └──────────────────────────────────────────────────────────────┘
```

## Modules

| module | does |
|---|---|
| `record.py` | `Record`: one consumed message, bytes untouched |
| `codec.py` | bytes ⇄ dict. JSON for now; a codec is a small protocol |
| `rules/spec.py` | pydantic models for rule files; each rule type is a `RuleSpec` subclass |
| `rules/builtin/` | the ten built-in rule types |
| `rules/expr.py` | the safe expression language, compiled from a Python AST whitelist to closures |
| `rules/registry.py` | rule type name → spec class, plus entry-point plugins |
| `rules/engine.py` | `RuleSet.evaluate(data) → Verdict`: runs every rule, then stateful rules, applies masking |
| `rules/loader.py` | rule files → `RuleSet`, error reporting, `RuleWatcher` for hot reload |
| `evaluation.py` | one record through decode + rules, the unit shared by run, replay and `rules test` |
| `envelope.py` | the quarantine envelope (encode and decode) and the `sg-` headers |
| `gateway/router.py` | verdict → output topic, key, value, headers, timestamp |
| `gateway/pipeline.py` | the consume / produce / flush / commit loop and rebalance handling |
| `gateway/ports.py` | the two protocols the pipeline needs from Kafka |
| `gateway/kafka.py` | confluent-kafka implementations of those protocols, topic checks |
| `gateway/dedup.py` | per-partition in-memory windows for `dedup` rules |
| `replay.py` | bounded re-read of a quarantine topic through the current rules |
| `config.py` | settings file + environment, client configuration with locked keys |
| `metrics.py`, `health.py`, `logs.py` | Prometheus metrics, `/healthz` `/readyz`, JSON logging |
| `app.py` | builds a running gateway or replayer from settings |
| `cli.py` | `run`, `replay`, `rules validate`, `rules test`, `version` |

The rules package has no Kafka dependency; it can be imported and used on its own.

## The loop

1. `consume(batch_size)` from the assigned partitions.
2. For each record: decode, evaluate with the stream's current `RuleSet`, route, `produce`.
   Undecodable values become a `decode.error` violation instead of an exception.
3. `flush()` and wait for every delivery report.
4. If anything failed: stop without committing (default), so the batch is redone after restart.
5. `commit()` the next offset of every partition seen in the batch.
6. Update lag, check rule files for changes, mark the loop alive.

Rebalances happen inside `consume`, between batches, so there is never uncommitted work when
partitions are revoked. Revoked partitions drop their dedup windows.

## Decisions

- **Batch commit after flush** rather than per record: one round-trip per batch, at most one
  batch duplicated per partition on a crash.
- **`enable.auto.offset.store=false`** and explicit offsets on commit: what's committed is what
  was produced, not what was fetched.
- **Idempotent producer, same key, consumption order**: per-key ordering carries over to the
  clean topic.
- **No topic creation**: partition counts, retention and ACLs are the operator's decisions.
- **Rules are data**: no code deploy to change validation; hot reload swaps whole immutable
  `RuleSet` objects between batches.
- **Ports and adapters**: the pipeline is unit tested against in-memory fakes; the Kafka
  adapters are thin and covered by integration tests against a real broker.

## Limits of 0.1

- JSON values only. Avro/Protobuf with Schema Registry is planned.
- At-least-once. Exactly-once via Kafka transactions is planned; commit logic sits in one place
  so it can be swapped.
- Dedup state is in memory, per instance.
