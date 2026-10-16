# Contributing

1. `make setup`
2. Make your change with tests.
3. `make check` must pass. If you touched anything that talks to Kafka, run `make kafka-up itest` too.
4. Add a line under `Unreleased` in `CHANGELOG.md` for user-visible changes.

Keep runtime dependencies to what is already in `pyproject.toml`; new ones need a good reason.
