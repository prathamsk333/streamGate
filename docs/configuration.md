# Configuration

`streamgate run -c streamgate.yaml` reads one YAML file. Any value can be overridden with an
environment variable named `STREAMGATE_` + the path joined by `__`, for example
`STREAMGATE_KAFKA__BOOTSTRAP_SERVERS=k1:9092` or `STREAMGATE_METRICS__PORT=9200`.
Environment variables win over the file. Unknown top-level keys in the file are an error.

```yaml
kafka:
  bootstrap_servers: localhost:9092
  group_id: streamgate          # every instance of one deployment shares this
  common: {}                    # librdkafka settings for every client (security, SASL, ...)
  consumer: {}                  # librdkafka consumer overrides
  producer: {}                  # librdkafka producer overrides

topics:                         # name patterns; {name} is the stream name
  input: "raw.{name}"
  clean: "clean.{name}"
  quarantine: "quarantine.{name}"

streams:
  - name: orders
    rules: rules/orders.yaml    # relative to this file
    # input / clean / quarantine override the patterns for this stream

pipeline:
  batch_size: 500               # max records per poll
  poll_timeout_s: 1.0
  flush_timeout_s: 30           # how long to wait for the brokers to ack a batch
  warn_header: false            # add sg-warnings to clean records that had warn-level hits
  reject_tombstones: false      # null values pass through by default (compacted topics)
  on_produce_error: exit        # exit | log, see operations.md
  dedup_max_keys: 1000000       # per partition, for dedup rules

rules_reload_s: 5               # how often to check rule files for changes; 0 disables

metrics:
  enabled: true
  bind: 0.0.0.0
  port: 9108
  liveness_timeout_s: 120       # /healthz fails if the loop hasn't turned for this long

logging:
  level: info                   # debug | info | warning | error
  format: json                  # json | console

instance_id: null               # defaults to <hostname>-<random>; shows up in envelopes
```

## Kafka client settings

StreamGate builds the client configuration as: its own defaults, then `common`, then
`consumer` / `producer`, then the settings it requires. The required ones cannot be
overridden, because they are what makes the delivery guarantees hold:

| client | setting | value |
|---|---|---|
| consumer | `enable.auto.commit` | `false` |
| consumer | `enable.auto.offset.store` | `false` |
| producer | `enable.idempotence` | `true` |
| producer | `acks` | `all` |

`bootstrap.servers`, `group.id` and `client.id` come from the dedicated settings above.

Defaults you may change: `auto.offset.reset: earliest`, `max.poll.interval.ms: 300000`,
`compression.type: lz4`, `linger.ms: 5`.

## Topics

StreamGate never creates topics. On startup it checks that every input, clean and quarantine
topic exists and refuses to start otherwise. It warns when a clean topic has a different
partition count from its input: ordering per key is still kept, but a key no longer lands on
the same partition number on both sides, which matters if something joins the two.

The same topic can't be the input of two streams, and no topic can be both an input and an
output.
