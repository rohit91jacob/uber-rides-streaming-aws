"""``uber-rides`` command line.

Exit codes: 0 ok, 1 unexpected error, 2 usage/config error, 3 data-quality gate failed,
4 stream reconciliation failed, 5 state machine failed.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from uber_rides import __version__
from uber_rides.config import ConfigError, Settings
from uber_rides.logs import configure_logging, get_logger

log = get_logger("uber_rides.cli")

EXIT_DQ = 3
EXIT_STREAM = 4
EXIT_STATE_MACHINE = 5


def _print(obj: Any) -> None:
    print(json.dumps(obj, indent=2, default=str))


def _settings(args: argparse.Namespace) -> Settings:
    settings = Settings.from_env()
    overrides: dict[str, Any] = {}
    if getattr(args, "data_dir", None):
        overrides["data_dir"] = Path(args.data_dir)
    if getattr(args, "source_dir", None):
        overrides["tlc_source_dir"] = Path(args.source_dir)
        overrides.setdefault("weather_source_dir", Path(args.source_dir))
    if getattr(args, "weather_dir", None):
        overrides["weather_source_dir"] = Path(args.weather_dir)
    return settings.with_overrides(**overrides).validate() if overrides else settings


# --------------------------------------------------------------------------- commands
def cmd_latest_month(args: argparse.Namespace, s: Settings) -> int:
    from uber_rides.sources import tlc

    print(tlc.latest_available_month(s))
    return 0


def cmd_ingest(args: argparse.Namespace, s: Settings) -> int:
    from uber_rides.sources import tlc, weather

    result = tlc.ingest_month(s, args.month, force=args.force)
    out = {
        "month": args.month,
        "trips_file": {k: result["trips_file"][k] for k in ("path", "bytes", "sha256", "changed")},
        "columns": result["columns"],
        "licensees": {r["license_num"]: r["trips"] for r in result["profile"]["licensees"]},
    }
    if not args.skip_weather:
        out["weather"] = weather.fetch_month(s, args.month, force=args.force)
    _print(out)
    return 0


def cmd_load(args: argparse.Namespace, s: Settings) -> int:
    from uber_rides.orchestration.steps import step_load

    _print(step_load(s, {"month": args.month}))
    return 0


def cmd_dq(args: argparse.Namespace, s: Settings) -> int:
    from uber_rides.quality import gates

    layers = gates.LAYERS if args.layer == "all" else (args.layer,)
    worst = 0
    for layer in layers:
        results = gates.run_gate(s, layer, args.month, raise_on_error=False)
        summary = gates.summarize(results)
        _print({"layer": layer, **summary, "failed": [r.check_name for r in results if r.status == "fail"]})
        if summary["errors"] and s.dq_fail_on_error:
            worst = EXIT_DQ
    return worst


def cmd_stream(args: argparse.Namespace, s: Settings) -> int:
    from uber_rides.streaming.run import run_stream

    summary = run_stream(
        s,
        start=datetime.fromisoformat(args.start),
        end=datetime.fromisoformat(args.end),
        sample_rate=args.sample_rate,
        speedup=args.speedup,
        duplicate_every=args.duplicate_every,
        malformed_every=args.malformed_every,
        seed=args.seed,
        concurrent=args.concurrent,
        replays=args.replays,
    )
    _print(
        {
            k: summary[k]
            for k in (
                "run_id",
                "aws_mode",
                "consumer",
                "dlq_by_reason",
                "reconciliation",
                "elapsed_s",
                "events_per_second",
            )
        }
    )
    return 0 if summary["reconciliation"]["ok"] else EXIT_STREAM


def cmd_load_events(args: argparse.Namespace, s: Settings) -> int:
    from uber_rides.lake.events_load import load_events

    _print(load_events(s, full_refresh=args.full_refresh))
    return 0


def cmd_publish(args: argparse.Namespace, s: Settings) -> int:
    from uber_rides.lake.serving import publish

    _print(publish(s))
    return 0


def cmd_transform(args: argparse.Namespace, s: Settings) -> int:
    from uber_rides.lake.serving import publish
    from uber_rides.orchestration.steps import run_dbt

    if args.target in (None, "dev", "ci"):
        publish(s)
    proc = run_dbt(s, [args.dbt_command, *args.dbt_args], target=args.target)
    return proc.returncode


def cmd_batch(args: argparse.Namespace, s: Settings) -> int:
    from uber_rides.orchestration import asl
    from uber_rides.orchestration.steps import run_step

    payload: dict[str, Any] = {"month": args.month}
    if args.force:
        payload["force"] = True
    if args.replay_start and args.replay_end:
        payload["replay"] = {
            "start": args.replay_start,
            "end": args.replay_end,
            "sample_rate": args.replay_sample_rate,
            "duplicate_every": args.duplicate_every,
            "malformed_every": args.malformed_every,
            "replays": args.replays,
        }

    def invoke(step_payload: dict[str, Any]) -> dict[str, Any]:
        if payload.get("force"):
            step_payload = {**step_payload, "force": True}
        return run_step(s, step_payload)

    execution = asl.execute(asl.load_definition(), payload, invoke)
    record = {
        "status": execution.status,
        "error": execution.error,
        "cause": execution.cause,
        "path": [h["state"] for h in execution.history],
        "history": execution.history,
        "output": execution.output,
    }
    out = s.ops_dir / "batch_runs" / "latest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(record, indent=2, default=str), "utf-8")
    _print({k: record[k] for k in ("status", "error", "cause", "path")})
    if execution.status == "SUCCEEDED":
        return 0
    return EXIT_DQ if execution.error == "DataQualityGateFailed" else EXIT_STATE_MACHINE


def cmd_verify(args: argparse.Namespace, s: Settings) -> int:
    from uber_rides.verify import verify_month

    result = verify_month(s, args.month, offline_dir=Path(args.offline_dir) if args.offline_dir else None)
    _print(result)
    return 0


def cmd_report(args: argparse.Namespace, s: Settings) -> int:
    from uber_rides.report import build_site

    _print(build_site(s, Path(args.out)))
    return 0


def cmd_sql(args: argparse.Namespace, s: Settings) -> int:
    from uber_rides.lake.serving import connect

    con = connect(s, read_only=True)
    try:
        cur = con.execute(args.query)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchmany(args.limit)
    finally:
        con.close()
    print("\t".join(cols))
    for r in rows:
        print("\t".join("" if v is None else str(v) for v in r))
    return 0


def cmd_bootstrap(args: argparse.Namespace, s: Settings) -> int:
    from uber_rides.streaming.aws import aws_context, bootstrap

    if s.aws_mode == "moto":
        print(
            "moto mode is in-process: resources are created per run; set UBER_AWS_ENDPOINT_URL to bootstrap an emulator",
            file=sys.stderr,
        )
        return 2
    with aws_context(s):
        _print(bootstrap(s))
    return 0


# --------------------------------------------------------------------------- parser
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="uber-rides", description=__doc__.splitlines()[0])
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    p.add_argument("--data-dir", help="override UBER_DATA_DIR")
    p.add_argument("--source-dir", help="use pre-downloaded TLC/weather files from this directory")
    p.add_argument("--weather-dir", help="use pre-downloaded Open-Meteo files from this directory")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("latest-month", help="newest month with a published HVFHV file").set_defaults(
        func=cmd_latest_month
    )

    sp = sub.add_parser("ingest", help="land a TLC month, the zone lookup and the month's weather")
    sp.add_argument("--month", required=True)
    sp.add_argument("--force", action="store_true")
    sp.add_argument("--skip-weather", action="store_true")
    sp.set_defaults(func=cmd_ingest)

    sp = sub.add_parser("load", help="load a landed month into the Iceberg tables (idempotent)")
    sp.add_argument("--month", required=True)
    sp.set_defaults(func=cmd_load)

    sp = sub.add_parser("dq", help="run data-quality gates (exit 3 on error-level failures)")
    sp.add_argument("--layer", choices=["landing", "lake", "stream", "marts", "all"], default="all")
    sp.add_argument("--month", required=True)
    sp.set_defaults(func=cmd_dq)

    sp = sub.add_parser("stream", help="replay real trips through Kinesis -> Lambda -> DynamoDB (emulated)")
    sp.add_argument("--start", required=True, help="request_datetime window start, e.g. 2026-06-12T17:00")
    sp.add_argument("--end", required=True)
    sp.add_argument("--sample-rate", type=float, default=1.0)
    sp.add_argument(
        "--speedup", type=float, default=None, help="0 = as fast as possible (default UBER_REPLAY_SPEEDUP)"
    )
    sp.add_argument("--duplicate-every", type=int, default=0, help="SIMULATED: resend every Nth event")
    sp.add_argument(
        "--malformed-every", type=int, default=0, help="SIMULATED: corrupt a copy of every Nth event"
    )
    sp.add_argument("--seed", type=int, default=7)
    sp.add_argument("--replays", type=int, default=1, help="replay the window N times (dedupe drill)")
    sp.add_argument("--concurrent", action="store_true", help="consume while producing")
    sp.set_defaults(func=cmd_stream)

    sp = sub.add_parser("load-events", help="landing zone -> Iceberg ride_events (incremental)")
    sp.add_argument("--full-refresh", action="store_true")
    sp.set_defaults(func=cmd_load_events)

    sub.add_parser("publish", help="expose current Iceberg snapshots to DuckDB (schema lake)").set_defaults(
        func=cmd_publish
    )

    sp = sub.add_parser("transform", help="run dbt (default: build) against the serving DuckDB")
    sp.add_argument("dbt_command", nargs="?", default="build")
    sp.add_argument("--target", default=None, help="dbt target: dev | ci | athena")
    sp.add_argument("dbt_args", nargs=argparse.REMAINDER)
    sp.set_defaults(func=cmd_transform)

    sp = sub.add_parser(
        "batch", help="run the daily-batch state machine locally (same ASL as Step Functions)"
    )
    sp.add_argument("--month", default=None, help="default: newest published month")
    sp.add_argument("--force", action="store_true", help="re-run ingest/load even if the month was processed")
    sp.add_argument("--replay-start")
    sp.add_argument("--replay-end")
    sp.add_argument("--replay-sample-rate", type=float, default=1.0)
    sp.add_argument("--duplicate-every", type=int, default=0)
    sp.add_argument("--malformed-every", type=int, default=0)
    sp.add_argument("--replays", type=int, default=1)
    sp.set_defaults(func=cmd_batch)

    sp = sub.add_parser("verify", help="reconcile with TLC-published figures")
    sp.add_argument("--month", required=True)
    sp.add_argument("--offline-dir", help="read published files from here instead of downloading")
    sp.set_defaults(func=cmd_verify)

    sp = sub.add_parser("report", help="write the static results site")
    sp.add_argument("--out", default="site")
    sp.set_defaults(func=cmd_report)

    sp = sub.add_parser("sql", help="ad-hoc read-only SQL on the serving DuckDB")
    sp.add_argument("query")
    sp.add_argument("--limit", type=int, default=50)
    sp.set_defaults(func=cmd_sql)

    sub.add_parser(
        "bootstrap", help="create stream/tables/queue/bucket on an emulator endpoint"
    ).set_defaults(func=cmd_bootstrap)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        settings = _settings(args)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2
    configure_logging(settings.log_level, settings.log_format)
    from uber_rides.quality.gates import GateFailed

    try:
        return int(args.func(args, settings) or 0)
    except GateFailed as exc:
        log.error(str(exc))
        return EXIT_DQ
    except (ValueError, FileNotFoundError, LookupError) as exc:
        log.error("command failed", extra={"error": str(exc)})
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
