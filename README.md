# StreamGate

**A data-quality firewall for Kafka.** Declare what a valid record looks like in YAML;
StreamGate checks every record on a topic, passes the good ones through untouched, and
quarantines the bad ones with every reason they failed. Open source, Kafka-only, no schema
registry required.

```
producers ─▶ orders ─▶ streamgate ─┬─▶ orders.clean ──────▶ inventory, billing, analytics
                                   └─▶ orders.quarantine ─▶ dashboards, replay
```

## Why

Kafka guarantees that every byte a producer sends is stored and delivered. It says nothing
about whether those bytes make sense. One bad deploy and a producer starts sending amounts in
the wrong unit, nulls where IDs should be, the same event five times, timestamps from last
year. Kafka delivers all of it, and every consumer downstream fails in its own way: one
crashes on the bad offset, one emails a customer the wrong total, one quietly corrupts a
dashboard that nobody checks until Tuesday.

The usual fix is a copy of the same checks inside every consumer, written by different teams,
drifting apart, redeployed on every change, and silently dropping what fails. StreamGate puts
those checks in one place, as configuration, at the boundary between the people who write a
topic and the people who read it.

## What you get

- **Rules as YAML, not code.** Required fields, types, numeric ranges, lengths, regexes, enums,
  timestamp freshness, cross-field expressions, windowed dedup, PII detection and masking.
  Rule files are validated with precise errors, hot-reloaded while running, and testable
  offline. New rule types plug in as Python packages.
- **Byte-exact pass-through.** Clean records keep their key, value bytes, headers and
  timestamp. Consumers switch topics and change nothing else.
- **Nothing lost, nothing hidden.** Rejected records go to a quarantine topic wrapped in an
  envelope listing every failed rule and the original topic, partition and offset.
- **Never blocked.** A record that can't even be parsed is quarantined, not retried forever.
- **Replay after a fix.** Re-check a window of quarantined records against today's rules and
  release the ones that now pass, with duplicates and time-based rules handled correctly.
- **Safe delivery.** Offsets are committed only after outputs are acknowledged: a crash means a
  re-processed batch at worst, never a gap. Ordering per key carries through.
- **Scales like Kafka.** Run more instances with the same group id; partitions are shared out
  automatically.
- **Built to be operated.** Prometheus metrics per stream and per rule, consumer lag, health
  and readiness endpoints, JSON logs, graceful shutdown, dry-run mode, a small non-root Docker
  image.

## A rule file

```yaml
stream: orders
version: 3
rules:
  - {id: order_id.required,    type: required, field: order_id}
  - {id: customer_id.required, type: required, field: customer_id}
  - {id: amount.type,          type: type, field: amount, is: number}
  - {id: amount.range,         type: range, field: amount, min: 1, max: 10000}
  - {id: currency.enum,        type: enum, field: currency, values: [INR, USD, EUR]}
  - {id: placed_at.fresh,      type: freshness, field: placed_at, max_age: 5m}

  - id: dates.ordered
    type: expression
    expr: "shipped_at is None or ts(shipped_at) >= ts(placed_at)"
    message: shipped_at is before placed_at

  - id: notes.pii
    type: pii
    fields: [notes]
    patterns: [email, phone]
    action: mask          # rewrite instead of reject
    severity: warn

  - {id: order.dedup, type: dedup, key: order_id, window: 10m}
```

Try it against sample records before it goes anywhere near Kafka:

```console
$ streamgate rules test rules/orders.yaml --file samples.jsonl
   1  pass
   2  reject
        customer_id.required: customer_id is null
        amount.range: amount=34900 exceeds maximum 10000
   3  warn
        notes.pii: possible PII (phone) in notes
   4  reject
        order.dedup: duplicate order_id=1 within 10m
```

## Quickstart

Needs Docker.

```bash
git clone <this repo> && cd streamgate
docker compose -f docker/compose.example.yml up -d --build
```

That starts a Kafka broker, creates `raw.orders`, `clean.orders` and `quarantine.orders`, and
runs StreamGate with [examples/rules/orders.yaml](examples/rules/orders.yaml).

Send one good and one bad order:

```bash
NOW=$(date -u +%FT%TZ)
printf '%s\n' \
  "o1|{\"order_id\":1,\"customer_id\":8,\"amount\":349,\"currency\":\"INR\",\"placed_at\":\"$NOW\"}" \
  "o2|{\"order_id\":2,\"amount\":-5,\"currency\":\"INR\",\"placed_at\":\"$NOW\"}" |
docker compose -f docker/compose.example.yml exec -T kafka \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic raw.orders --property parse.key=true --property key.separator='|'
```

See where they went:

```bash
for t in clean.orders quarantine.orders; do
  echo "== $t"
  docker compose -f docker/compose.example.yml exec -T kafka \
    /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic $t --from-beginning --property print.key=true --timeout-ms 3000 2>/dev/null
done
curl -s localhost:9108/metrics | grep streamgate_violations_total
```

`o1` is in `clean.orders`, byte for byte. `o2` is in `quarantine.orders` with
`customer_id.required` and `amount.range` as reasons. Edit `examples/rules/orders.yaml` and
the running gateway picks up the change within five seconds.

Clean up with `docker compose -f docker/compose.example.yml down -v`.

## On an existing cluster

StreamGate is a Kafka client: it connects to your brokers and needs nothing else. Keep your
topic as it is, add two topics next to it, and point StreamGate at all three:

```bash
kafka-topics.sh --create --topic orders.clean      --partitions <same as orders> --replication-factor 3
kafka-topics.sh --create --topic orders.quarantine --partitions 3                --replication-factor 3
```

```yaml
# streamgate.yaml
kafka:
  bootstrap_servers: kafka-1:9092,kafka-2:9092,kafka-3:9092
streams:
  - name: orders
    rules: rules/orders.yaml
    input: orders              # producers keep writing here
    clean: orders.clean        # consumers move here
    quarantine: orders.quarantine
```

```bash
streamgate run -c streamgate.yaml
```

Producers don't change. Consumers move from `orders` to `orders.clean` one at a time, whenever
they're ready. Add more `streams:` entries to guard more topics with the same deployment, and
run more copies of the process to share the load.

## Commands

```bash
streamgate rules validate rules/                              # check rule files, report every error
streamgate rules test rules/orders.yaml --file samples.jsonl  # run records through rules offline
streamgate run -c streamgate.yaml                             # run the gateway
streamgate run -c streamgate.yaml --dry-run                   # evaluate live traffic, write nothing
streamgate replay -c streamgate.yaml --stream orders \
  --from 2026-10-14T10:00:00Z --dry-run                       # what would a replay release?
streamgate replay -c streamgate.yaml --stream orders --from 2026-10-14T10:00:00Z
```

Install from a clone with `pip install .`, or build the image with
`docker build -f docker/Dockerfile -t streamgate .`.

## Watching it

A rising reject rate on one stream is almost always a producer release; the rule that's
firing tells you which field broke.

```promql
sum by (stream) (rate(streamgate_records_total{outcome="reject"}[5m]))
  / sum by (stream) (rate(streamgate_records_total[5m]))

topk(5, sum by (stream, rule) (rate(streamgate_violations_total[5m])))
```

`/healthz` and `/readyz` are ready-made liveness and readiness probes. See
[operations](docs/operations.md) for the full list of metrics and alerts.

## Performance

One process on a laptop (Python 3.12, single local broker), running the nine-rule example
above including PII scanning and dedup, with about 5% bad records:

| | records/s |
|---|---|
| rule evaluation and routing (`scripts/bench.py local`) | ~40,000 |
| end to end through Kafka, 3 partitions (`scripts/bench.py kafka`) | ~28,000 |

Throughput grows with the number of instances, up to the input topic's partition count. Run
the script with your own rules and hardware before sizing.

## Documentation

- [Rule files](docs/rules.md): every rule type, expressions, examples
- [Configuration](docs/configuration.md): all settings, environment overrides
- [Operations](docs/operations.md): delivery guarantees, scaling, metrics, health, replay runbook
- [Quarantine envelope](docs/envelope.md): the format of rejected records
- [Architecture](docs/architecture.md): how it's built and why
- [Custom rule types](docs/plugins.md): write your own

## Roadmap

- Avro and Protobuf values
- Exactly-once delivery with Kafka transactions
- Dedup windows that survive restarts
- Hot-partition and key-skew detection

## Development

Requires [uv](https://docs.astral.sh/uv/) and Docker.

```bash
make setup        # create the virtualenv
make check        # lint, typecheck, unit tests
make kafka-up     # single-node Kafka on localhost:9092
make itest        # integration tests against it
make bench        # throughput on this machine
```

Contributions are welcome; see [CONTRIBUTING](CONTRIBUTING.md).

## License

Apache-2.0
