# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-10-16

First release.

### Added

- Rule files with ten built-in types: `required`, `type`, `range`, `length`, `regex`, `enum`,
  `freshness`, `expression`, `dedup`, `pii` (detect or mask).
- Safe expression language for cross-field rules and `when:` guards.
- Rule type plugins through the `streamgate.rules` entry point.
- `streamgate rules validate` and `streamgate rules test`.
- `streamgate run`: the gateway, routing records to clean and quarantine topics with
  at-least-once delivery, per-key ordering and byte-exact pass-through.
- Quarantine envelope, schema version 1.
- Hot reload of rule files.
- Prometheus metrics, `/healthz` and `/readyz`, JSON logs, graceful shutdown.
- `--dry-run` mode.
- `streamgate replay`: re-check quarantined records against the current rules and release the
  ones that pass, with time windows, dry run, re-quarantine, and duplicate protection.
- Docker image (non-root, health check) and an example compose stack.
- `scripts/bench.py` for throughput measurements.
