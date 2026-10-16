# Quarantine envelope

Every rejected record is written to the stream's quarantine topic as a JSON envelope. The
message key is the original key, headers are the original headers plus three of ours, and the
message timestamp is the time of rejection.

```json
{
  "schema_version": 1,
  "rejected_at": "2026-10-14T10:00:03.117Z",
  "source": {
    "topic": "raw.orders",
    "partition": 1,
    "offset": 4211,
    "timestamp_ms": 1760436000000,
    "key": "ord-18"
  },
  "violations": [
    {"rule": "customer_id.required", "severity": "reject", "message": "customer_id is null"},
    {"rule": "amount.range", "severity": "reject", "message": "amount=34900 exceeds maximum 10000"}
  ],
  "record": {"encoding": "json", "value": {"order_id": 18, "customer_id": null, "amount": 34900}},
  "streamgate": {"version": "0.1.0", "instance": "sg-7f3a", "ruleset": "orders@3:9c1f2a0b"}
}
```

| field | |
|---|---|
| `schema_version` | `1`. Incompatible changes bump it. |
| `source` | where the record came from. `key` is a UTF-8 string, or base64 with `"key_encoding": "base64"` when the key isn't valid UTF-8, or `null`. |
| `violations` | every rule the record broke, including warnings. |
| `record.encoding` | `json`: `value` is the decoded record (after PII masking, if any). `base64`: the value couldn't be decoded; `value` is the raw bytes and `error` says why. `none`: the record was a tombstone. |
| `streamgate.ruleset` | `<stream>@<version>:<sha256 prefix of the rule file>`, so you can tell exactly which rules rejected it. |

Headers added:

| header | value |
|---|---|
| `sg-schema` | `1` |
| `sg-violations` | comma-separated rule ids |
| `sg-source` | `topic/partition/offset` |

Clean records may carry `sg-masked` (ids of the PII rules that rewrote the value) and, with
`pipeline.warn_header`, `sg-warnings`. Header names starting with `sg-` are reserved.
