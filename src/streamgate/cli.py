from __future__ import annotations

import json
import logging
import signal
import sys
from collections.abc import Iterator
from importlib.metadata import version as dist_version
from pathlib import Path
from typing import IO, Any

import click
import confluent_kafka

from streamgate import __version__, logs
from streamgate.app import build_app, build_replayer
from streamgate.codec import JsonCodec
from streamgate.config import ConfigError, Settings
from streamgate.evaluation import Evaluation, evaluate
from streamgate.gateway.dedup import MemoryDedupStore
from streamgate.gateway.kafka import TopicCheckError
from streamgate.gateway.pipeline import ProduceError
from streamgate.record import Record
from streamgate.replay import Replayed, ReplayError, ReplayOptions
from streamgate.rules.base import Outcome
from streamgate.rules.loader import RuleFileError, find_rule_files, load_ruleset
from streamgate.rules.registry import PluginError, default_registry
from streamgate.timeutil import now_ms, parse_timestamp


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(__version__, prog_name="streamgate")
def main() -> None:
    """StreamGate: a data-quality firewall for Kafka."""


@main.command()
def version() -> None:
    """Show StreamGate and Kafka client versions."""
    client = dist_version("confluent-kafka")
    librdkafka = confluent_kafka.libversion()[0]
    click.echo(f"streamgate {__version__} (confluent-kafka {client}, librdkafka {librdkafka})")


@main.command()
@click.option(
    "-c",
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default="streamgate.yaml",
    show_default=True,
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Consume and evaluate under a separate consumer group; never produce or commit.",
)
def run(config_path: Path, dry_run: bool) -> None:
    """Run the gateway."""
    try:
        settings = Settings.from_file(config_path)
    except ConfigError as exc:
        raise click.ClickException(str(exc)) from None
    logs.configure(settings.logging.level, settings.logging.format)
    log = logging.getLogger("streamgate")

    try:
        app = build_app(settings, dry_run=dry_run)
    except (RuleFileError, TopicCheckError, PluginError) as exc:
        log.error("%s", exc)
        sys.exit(1)

    def stop(signum: int, _: object) -> None:
        log.info("received %s, finishing the current batch", signal.Signals(signum).name)
        app.stop()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        app.run()
    except ProduceError as exc:
        log.error("stopping without committing the last batch: %s", exc)
        sys.exit(1)


@main.command()
@click.option(
    "-c",
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default="streamgate.yaml",
    show_default=True,
)
@click.option("--stream", "stream_name", required=True, help="Stream whose quarantine to replay.")
@click.option("--from", "since", help="Only records rejected at or after this ISO-8601 time.")
@click.option("--to", "until", help="Only records rejected at or before this ISO-8601 time.")
@click.option(
    "--as-of",
    type=click.Choice(["original", "now"]),
    default="original",
    show_default=True,
    help="Clock for time-based rules: when the record was rejected, or now.",
)
@click.option("--limit", type=click.IntRange(min=1), help="Stop after this many records.")
@click.option(
    "--requarantine", is_flag=True, help="Write records that still fail back to quarantine."
)
@click.option(
    "--release-duplicates",
    is_flag=True,
    help="Also release records that were rejected by a dedup rule.",
)
@click.option("--group-id", help="Consumer group id (offsets are never committed).")
@click.option("--dry-run", is_flag=True, help="Show what would happen; produce nothing.")
def replay(  # click passes options by name
    *,
    config_path: Path,
    stream_name: str,
    since: str | None,
    until: str | None,
    as_of: str,
    limit: int | None,
    requarantine: bool,
    release_duplicates: bool,
    group_id: str | None,
    dry_run: bool,
) -> None:
    """Re-check quarantined records with the current rules and release those that pass."""
    try:
        settings = Settings.from_file(config_path)
    except ConfigError as exc:
        raise click.ClickException(str(exc)) from None
    logs.configure("warning", "console")
    options = ReplayOptions(
        since_ms=_parse_time(since, "--from") if since else None,
        until_ms=_parse_time(until, "--to") if until else None,
        as_of="now" if as_of == "now" else "original",
        dry_run=dry_run,
        limit=limit,
        requarantine=requarantine,
        release_duplicates=release_duplicates,
    )
    try:
        replayer = build_replayer(settings, stream_name, dry_run=dry_run, group_id=group_id)
        summary = replayer.run(options, on_record=_print_replayed if dry_run else None)
    except (ValueError, RuleFileError, TopicCheckError, ReplayError) as exc:
        raise click.ClickException(str(exc)) from None

    verb = "would release" if dry_run else "released"
    click.echo(
        f"scanned {summary.scanned}, {verb} {summary.released}, "
        f"still rejected {summary.still_rejected}, malformed {summary.malformed}"
    )
    for rule, count in summary.by_rule.most_common():
        click.echo(f"  {rule}: {count}")


def _print_replayed(item: Replayed) -> None:
    env, verdict = item.envelope, item.evaluation.verdict
    rules = ", ".join(v.rule_id for v in verdict.violations)
    source = f"{env.topic}/{env.partition}/{env.offset}"
    click.echo(f"{verdict.outcome.value:<6}  {source}  {rules}".rstrip())


@main.group()
def rules() -> None:
    """Work with rule files offline."""


@rules.command("validate")
@click.argument("paths", nargs=-1, required=True, type=click.Path(exists=True, path_type=Path))
def rules_validate(paths: tuple[Path, ...]) -> None:
    """Check rule files (or directories of them) and report every error."""
    registry = _registry()
    failed = False
    for path in (f for p in paths for f in find_rule_files(p)):
        try:
            ruleset = load_ruleset(path, registry)
        except RuleFileError as exc:
            failed = True
            click.secho(f"FAIL {path}", fg="red", err=True)
            for error in exc.errors:
                click.echo(f"     {error}", err=True)
            continue
        click.echo(f"ok   {path}  {ruleset.id}, {len(ruleset)} rules")
    sys.exit(1 if failed else 0)


@rules.command("test")
@click.argument("path", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--record", "records", multiple=True, help="A JSON record. Repeatable.")
@click.option(
    "--file",
    "record_file",
    type=click.File("rb"),
    help="JSON Lines file of records, or - for stdin.",
)
@click.option("--now", help="Evaluate as if it were this ISO-8601 time.")
@click.option("--format", "fmt", type=click.Choice(["table", "json"]), default="table")
def rules_test(
    path: Path,
    records: tuple[str, ...],
    record_file: IO[bytes] | None,
    now: str | None,
    fmt: str,
) -> None:
    """Run records through a rule file. Exits 1 if any record is rejected."""
    if not records and record_file is None:
        raise click.UsageError("give at least one --record or a --file")
    try:
        ruleset = load_ruleset(path, _registry())
    except RuleFileError as exc:
        raise click.ClickException(str(exc)) from None

    clock = _parse_time(now, "--now") if now else now_ms()
    codec = JsonCodec()
    dedup = MemoryDedupStore()
    rejected = False
    for line, value in _inputs(records, record_file):
        record = Record(f"test.{ruleset.stream}", 0, line - 1, None, value, timestamp_ms=clock)
        result = evaluate(record, ruleset, codec, dedup, clock)
        rejected |= result.verdict.outcome is Outcome.REJECT
        click.echo(_render(line, result, fmt))
    sys.exit(1 if rejected else 0)


def _registry() -> Any:
    try:
        return default_registry()
    except PluginError as exc:
        raise click.ClickException(str(exc)) from None


def _parse_time(text: str, option: str) -> int:
    try:
        return parse_timestamp(text, "iso8601")
    except ValueError as exc:
        raise click.BadParameter(str(exc), param_hint=option) from None


def _inputs(records: tuple[str, ...], record_file: IO[bytes] | None) -> Iterator[tuple[int, bytes]]:
    line = 0
    for text in records:
        line += 1
        yield line, text.encode()
    if record_file is not None:
        for raw in record_file:
            line += 1
            if raw.strip():
                yield line, raw.rstrip(b"\r\n")


def _render(line: int, result: Evaluation, fmt: str) -> str:
    verdict = result.verdict
    if fmt == "json":
        return json.dumps(
            {
                "line": line,
                "outcome": verdict.outcome.value,
                "violations": [
                    {"rule": v.rule_id, "severity": v.severity.value, "message": v.message}
                    for v in verdict.violations
                ],
            }
        )
    colour = {Outcome.PASS: "green", Outcome.WARN: "yellow", Outcome.REJECT: "red"}
    head = click.style(f"{verdict.outcome.value:<6}", fg=colour[verdict.outcome])
    lines = [f"{line:>4}  {head}"]
    lines += [f"        {v.rule_id}: {v.message}" for v in verdict.violations]
    return "\n".join(lines)
