"""The batch steps behind the Step Functions state machine.

On AWS each Task state invokes one Lambda (``handler``) with ``{"step": ..., ...}``; locally
:mod:`uber_rides.orchestration.asl` calls :func:`run_step` directly. Steps return small JSON
documents that the state machine's ``ResultSelector`` picks fields from. Data-quality steps
*return* their error count (the state machine's Choice states stop the run) instead of raising.
"""

from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path
from typing import Any

from uber_rides.config import Settings
from uber_rides.lake import events_load, load, serving
from uber_rides.logs import get_logger
from uber_rides.quality import gates
from uber_rides.sources import tlc, weather

log = get_logger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]
# The container image sets UBER_DBT_DIR (the package is installed, not run from a checkout).
DBT_DIR = Path(os.environ.get("UBER_DBT_DIR", str(REPO_ROOT / "dbt")))


def _state_path(settings: Settings) -> Path:
    return settings.ops_dir / "batch_state.json"


def processed_months(settings: Settings) -> set[str]:
    path = _state_path(settings)
    return set(json.loads(path.read_text("utf-8")).get("processed_months", [])) if path.exists() else set()


def mark_processed(settings: Settings, month: str) -> None:
    months = processed_months(settings) | {month}
    path = _state_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"processed_months": sorted(months)}, indent=2), "utf-8")


def step_resolve_month(settings: Settings, payload: dict[str, Any]) -> dict[str, Any]:
    execution_input = payload.get("input") or {}
    month = payload.get("month") or execution_input.get("month") or tlc.latest_available_month(settings)
    tlc.parse_month(month)
    force = payload.get("force") or execution_input.get("force")
    is_new = force or month not in processed_months(settings)
    return {"month": month, "is_new": bool(is_new)}


def step_ingest(settings: Settings, payload: dict[str, Any]) -> dict[str, Any]:
    month = payload["month"]
    result = tlc.ingest_month(settings, month, force=payload.get("force", False))
    wx = weather.fetch_month(settings, month)
    return {"month": month, "trips_changed": result["trips_file"]["changed"], "weather_hours": wx["hours"]}


def step_load(settings: Settings, payload: dict[str, Any]) -> dict[str, Any]:
    month = payload["month"]
    trips = load.load_trips(settings, month)
    zones = load.load_zones(settings)
    wx = load.load_weather(settings, month)
    mark_processed(settings, month)
    return {
        "month": month,
        "trips_written": trips["trips_written"],
        "quarantine_written": trips["quarantine_written"],
        **zones,
        **wx,
    }


def step_dq(settings: Settings, payload: dict[str, Any]) -> dict[str, Any]:
    results = gates.run_gate(settings, payload["layer"], payload["month"], raise_on_error=False)
    return {"layer": payload["layer"], **gates.summarize(results)}


def step_replay(settings: Settings, payload: dict[str, Any]) -> dict[str, Any]:
    from uber_rides.streaming.run import run_stream

    spec = payload["replay"]
    summary = run_stream(
        settings,
        start=datetime.fromisoformat(spec["start"]),
        end=datetime.fromisoformat(spec["end"]),
        sample_rate=float(spec.get("sample_rate", 1.0)),
        speedup=float(spec.get("speedup", 0)),
        duplicate_every=int(spec.get("duplicate_every", 0)),
        malformed_every=int(spec.get("malformed_every", 0)),
        replays=int(spec.get("replays", 1)),
    )
    return {"run_id": summary["run_id"], "reconciled": summary["reconciliation"]["ok"]}


def step_load_events(settings: Settings, payload: dict[str, Any]) -> dict[str, Any]:
    extra: dict[str, Any] = {}
    if settings.aws_mode == "aws":
        # On AWS the stream runs continuously: pull new landing objects, snapshot the live
        # counters and drain the DLQ into Iceberg (locally `stream` does this per replay).
        from uber_rides.streaming import run

        extra["landing_objects_fetched"] = events_load.sync_landing_from_s3(settings)
        extra["counter_items"] = run.export_counters(settings, run.scan_counters(settings))
        extra["dlq_messages"] = run.export_dlq(settings, run.drain_dlq(settings))
    result = events_load.load_events(settings, full_refresh=bool(payload.get("full_refresh", False)))
    return {**result, **extra}


def run_dbt(settings: Settings, args: list[str], *, target: str | None = None) -> subprocess.CompletedProcess:
    """Run dbt in ``dbt/`` against the serving DuckDB (or ``target``)."""
    import sys

    env = {
        **os.environ,
        "UBER_DUCKDB_PATH": str(settings.serving_db.resolve()),
        "UBER_DUCKDB_MEMORY_LIMIT": settings.duckdb_memory_limit,
        "DBT_TARGET": target or settings.dbt_target,
        "DBT_PROFILES_DIR": str(DBT_DIR),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
    # Same interpreter as this process: works from a checkout, a wheel and the Lambda image.
    cmd = [sys.executable, "-c", "from dbt.cli.main import cli; cli()", *args, "--project-dir", str(DBT_DIR)]
    log.info("dbt", extra={"cmd": " ".join(args), "target": env["DBT_TARGET"]})
    return subprocess.run(cmd, cwd=DBT_DIR, env=env, check=False)  # noqa: S603 - fixed argv


def step_transform(settings: Settings, payload: dict[str, Any]) -> dict[str, Any]:
    if settings.dbt_target in {"dev", "ci"}:  # DuckDB targets read the published snapshot views
        serving.publish(settings)
    proc = run_dbt(settings, ["build", *payload.get("dbt_args", [])])
    if proc.returncode != 0:
        raise RuntimeError(f"dbt build failed with exit code {proc.returncode}")
    return {"ok": True}


STEPS: dict[str, Callable[[Settings, dict[str, Any]], dict[str, Any]]] = {
    "resolve_month": step_resolve_month,
    "ingest": step_ingest,
    "load": step_load,
    "dq": step_dq,
    "replay": step_replay,
    "load_events": step_load_events,
    "transform": step_transform,
}


def _jsonable(value: Any) -> Any:
    return json.loads(json.dumps(value, default=lambda v: v.isoformat() if isinstance(v, date) else str(v)))


def run_step(settings: Settings, payload: dict[str, Any]) -> dict[str, Any]:
    step = payload.get("step")
    if step not in STEPS:
        raise ValueError(f"unknown step {step!r}; expected one of {sorted(STEPS)}")
    log.info("step start", extra={"step": step})
    result = _jsonable(STEPS[step](settings, payload))
    log.info("step done", extra={"step": step})
    return result


def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    """Lambda entry point for every Task state of the daily batch state machine."""
    return run_step(Settings.from_env(), event)
