# Rule files

A rule file describes the checks for one stream. StreamGate loads one file per stream,
reloads it when it changes on disk, and keeps the previous version if the new one is broken.

```yaml
stream: orders          # must match the stream name in streamgate.yaml
version: 3              # optional; shows up in metrics and quarantine envelopes
defaults:
  severity: reject      # default for rules that don't set one
rules:
  - id: amount.range
    type: range
    field: amount
    min: 1
    max: 10000
```

Check a file before deploying it:

```bash
streamgate rules validate rules/
streamgate rules test rules/orders.yaml --file samples.jsonl --now 2025-10-14T10:00:00Z
```

`rules test` exits 1 when any record is rejected, so it can run in CI against a file of
known-good records.

## Common keys

Every rule has:

| key | | |
|---|---|---|
| `id` | required | lowercase letters, digits, `.`, `_`, `-`; unique in the file. Used in metrics and envelopes, so keep it stable. |
| `type` | required | one of the types below |
| `severity` | optional | `reject` sends the record to quarantine, `warn` lets it through and counts it |
| `when` | optional | an [expression](#expressions); the rule only runs when it is true |
| `message` | optional | replaces the generated failure message |

All rules run on every record, so a quarantined record lists every rule it broke, not just
the first.

## Fields

`field` is a dotted path: `customer.address.pin`, `items.0.sku` (list index), `items.-1.sku`.
Keys containing dots can't be addressed.

Except for `required`, rules **pass** when their field is absent or null. Combine with a
`required` rule when the field must be present.

## Rule types

### required

```yaml
- {id: customer_id.required, type: required, field: customer_id}
```

Fails when the field is absent or null.

### type

```yaml
- {id: amount.type, type: type, field: amount, is: number}
```

`is` is one of `int`, `float`, `number` (int or float), `str`, `bool`, `list`, `object`.
Booleans are never numbers, and `1.0` is a float, not an int.

### range

```yaml
- {id: amount.range, type: range, field: amount, min: 1, max: 10000}
- {id: ratio.range, type: range, field: ratio, exclusive_min: 0, exclusive_max: 1}
```

Inclusive `min`/`max` and exclusive `exclusive_min`/`exclusive_max`; set at least one.
Non-numeric values fail.

### length

```yaml
- {id: name.length, type: length, field: name, min: 1, max: 200}
```

Length of a string, list or object.

### regex

```yaml
- {id: pin.format, type: regex, field: pin, pattern: '\d{6}'}
- {id: sku.prefix, type: regex, field: sku, pattern: '^SKU-', full_match: false}
```

Python regular expressions. The whole value must match unless `full_match: false`.

### enum

```yaml
- {id: currency.enum, type: enum, field: currency, values: [INR, USD, EUR]}
```

### freshness

```yaml
- {id: placed_at.fresh, type: freshness, field: placed_at, max_age: 5m}
- {id: ts.fresh, type: freshness, field: ts, format: epoch_ms, max_age: 1h, future_tolerance: 2m}
- {id: kafka.fresh, type: freshness, source: kafka, max_age: 10m}
```

Fails when the timestamp is older than `max_age` or further in the future than
`future_tolerance` (default `30s`). `format` is `iso8601` (default; must include a timezone),
`epoch_s` or `epoch_ms`. With `source: kafka` the Kafka message timestamp is used instead of
a field.

Durations are written as `500ms`, `30s`, `5m`, `2h`, `1d`, or combined like `1h30m`.

### expression

```yaml
- id: dates.ordered
  type: expression
  expr: "shipped_at is None or ts(shipped_at) >= ts(placed_at)"
  message: shipped_at is before placed_at
```

Fails when the expression is false. An error while evaluating (comparing a string with a
number, say) is also a failure, with the error in the message.

### dedup

```yaml
- {id: order.dedup, type: dedup, key: order_id, window: 10m}
```

Fails when the same `key` value was seen in the last `window` **on the same partition**.
This is correct when the dedup field is the Kafka message key (or determines it), because
Kafka routes equal keys to the same partition. Dedup state lives in memory: it starts empty
after a restart and moves with partitions on a rebalance.

Dedup runs after every other rule and only for records that aren't already rejected, so a
broken record doesn't block the corrected one that follows it.

### pii

```yaml
- id: notes.pii
  type: pii
  fields: [notes, customer.comment]   # omit to scan the whole record
  patterns: [email, phone, ipv4, card]
  regex: ['EMP-\d{4}']                 # extra patterns of your own
  action: mask                         # or reject (default)
  mask: "***"
  severity: warn
```

Scans string values (including nested lists and objects) under `fields`. `card` matches are
Luhn-checked. With `action: mask` the matches are replaced in the record before it is written
anywhere, in clean and quarantine alike, and the record gets an `sg-masked` header. Matched
values never appear in messages.

## Expressions

Used by `expression` rules and `when:` guards. The syntax is a safe subset of Python:

- field names, `a.b.c` and `a['b']`, `items[0]`; a missing field is `None`
- literals: strings, numbers, `True`, `False`, `None`, lists `[...]`, tuples `(...)`
- `and`, `or`, `not`, `+ - * / // %`, comparisons including `in`, `not in`, `is`, `is not`,
  and `x if cond else y`
- functions: `len abs min max str int float lower upper startswith endswith`,
  `matches(value, regex)`, `ts(value)` (ISO-8601 or epoch seconds → epoch seconds),
  `now()` (epoch seconds), `age(value)` (seconds since a timestamp)

Anything else (attribute access on values, method calls, comprehensions, lambdas, names
starting with `_`, `**`) is rejected when the file is loaded.
