"""Data-quality gates between layers.

Each layer has a list of checks. A check is one SQL query (DuckDB, over the landed file or the
published ``lake.*`` / dbt ``marts.*`` relations) returning ``(failing_rows, total_rows)``; it
fails when ``failing_rows / total_rows`` exceeds its threshold (0 = no failing rows allowed).

* ``error`` failures stop the pipeline: :class:`GateFailed` is raised and the CLI exits 3.
* ``warn`` failures are recorded and reported, nothing more.

Every result is appended to the Iceberg table ``uber_lake_ops.dq_results`` and the latest run
per layer is written to ``<data>/ops/dq/<layer>.json`` for the results site.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

from uber_rides.config import Settings
from uber_rides.lake.catalog import get_catalog
from uber_rides.lake.serving import connect, publish
from uber_rides.lake.tables import DQ_RESULTS, ensure_table, to_arrow
from uber_rides.logs import get_logger
from uber_rides.sources import tlc

log = get_logger(__name__)

LAYERS = ("landing", "lake", "stream", "marts")


class GateFailed(RuntimeError):
    """At least one error-level check failed."""

    def __init__(self, layer: str, failed: list[CheckResult]) -> None:
        self.layer = layer
        self.failed = failed
        names = ", ".join(f.check_name for f in failed)
        super().__init__(f"data-quality gate '{layer}' failed: {names}")


@dataclass(frozen=True)
class Check:
    name: str
    severity: str  # "error" | "warn"
    sql: str
    detail: str
    threshold: float = 0.0
    params: tuple[Any, ...] = ()


@dataclass
class CheckResult:
    run_id: str
    checked_at: datetime
    layer: str
    check_name: str
    severity: str
    status: str  # pass | fail | skip
    failing_rows: int
    total_rows: int
    threshold: float
    detail: str

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["checked_at"] = self.checked_at.isoformat()
        return d


# --------------------------------------------------------------------------- check catalogue
def landing_checks(settings: Settings, month: str) -> list[Check]:
    path = tlc.landed_trip_path(settings, month).as_posix()
    start, end = tlc.month_bounds(month)
    days = (end - start).days
    src = f"(SELECT * FROM read_parquet('{path}') WHERE hvfhs_license_num = '{settings.license_num}')"
    return [
        Check(
            "landing_rows_for_licensee",
            "error",
            f"SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END, 1 FROM {src}",
            f"the file has trips for {settings.license_num}",
        ),
        Check(
            "landing_request_datetime_not_null",
            "error",
            f"SELECT count(*) FILTER (WHERE request_datetime IS NULL), count(*) FROM {src}",
            "request_datetime is present (needed for event-time replay)",
        ),
        Check(
            "landing_on_scene_datetime_not_null",
            "warn",
            f"SELECT count(*) FILTER (WHERE on_scene_datetime IS NULL), count(*) FROM {src}",
            "on_scene_datetime is present (driver_on_scene events)",
            threshold=0.01,
        ),
        Check(
            "landing_pickup_inside_month",
            "warn",
            f"SELECT count(*) FILTER (WHERE pickup_datetime < DATE '{start}' OR pickup_datetime >= DATE '{end}'), count(*) FROM {src}",
            "pickups fall inside the file's month",
            threshold=0.001,
        ),
        Check(
            "landing_all_days_present",
            "warn",
            f"SELECT {days} - count(DISTINCT CAST(pickup_datetime AS DATE)), {days} FROM {src}",
            f"every one of the {days} days has trips (fails on sampled fixtures)",
        ),
    ]


def lake_checks(settings: Settings, month: str) -> list[Check]:
    start, end = tlc.month_bounds(month)
    days = (end - start).days
    profile_path = tlc.profile_path(settings, month)
    expected = 0
    if profile_path.exists():
        profile = json.loads(profile_path.read_text("utf-8"))
        expected = next(
            (r["trips"] for r in profile["licensees"] if r["license_num"] == settings.license_num), 0
        )
    m = f"source_month = '{month}'"
    return [
        Check(
            "trips_reconcile_with_source",
            "error",
            f"""SELECT abs({expected} - (SELECT count(*) FROM lake.trips WHERE {m})
                              - (SELECT count(*) FROM lake.trips_quarantine WHERE {m})),
                       {expected}""",
            f"trips + quarantine = {expected:,} {settings.license_num} rows in the raw file (no row lost or duplicated)",
        ),
        Check(
            "trips_trip_id_unique",
            "error",
            f"SELECT count(*) - count(DISTINCT trip_id), count(*) FROM lake.trips WHERE {m}",
            "trip_id is unique",
        ),
        Check(
            "trips_keys_not_null",
            "error",
            f"""SELECT count(*) FILTER (WHERE trip_id IS NULL OR request_datetime IS NULL OR pickup_datetime IS NULL
                                     OR dropoff_datetime IS NULL OR pu_location_id IS NULL OR do_location_id IS NULL),
                       count(*) FROM lake.trips WHERE {m}""",
            "ids, timestamps and zones are present",
        ),
        Check(
            "trips_zone_in_range",
            "error",
            f"""SELECT count(*) FILTER (WHERE pu_location_id NOT BETWEEN 1 AND 265 OR do_location_id NOT BETWEEN 1 AND 265),
                       count(*) FROM lake.trips WHERE {m}""",
            "zone ids are TLC zones 1-265",
        ),
        Check(
            "trips_quarantine_rate",
            "error",
            f"""SELECT (SELECT count(*) FROM lake.trips_quarantine WHERE {m}),
                       (SELECT count(*) FROM lake.trips WHERE {m}) + (SELECT count(*) FROM lake.trips_quarantine WHERE {m})""",
            "at most 1% of trips fail a hard validity rule",
            threshold=0.01,
        ),
        Check(
            "trips_unknown_zone_rate",
            "warn",
            f"SELECT count(*) FILTER (WHERE is_unknown_zone), count(*) FROM lake.trips WHERE {m}",
            "at most 10% of trips start or end in unknown zones 264/265",
            threshold=0.10,
        ),
        Check(
            "trips_non_positive_fare_rate",
            "warn",
            f"SELECT count(*) FILTER (WHERE is_non_positive_fare), count(*) FROM lake.trips WHERE {m}",
            "at most 1% of trips have base_passenger_fare <= 0",
            threshold=0.01,
        ),
        Check(
            "trips_pickup_before_request_rate",
            "warn",
            f"SELECT count(*) FILTER (WHERE pickup_datetime < request_datetime), count(*) FROM lake.trips WHERE {m}",
            "at most 2% of trips are picked up before they were requested (scheduled rides; June 2026: 1.21%)",
            threshold=0.02,
        ),
        Check(
            "zones_complete",
            "error",
            "SELECT abs(265 - count(DISTINCT location_id)) + (count(*) - count(DISTINCT location_id)), 265 FROM lake.taxi_zones",
            "265 unique taxi zones",
        ),
        Check(
            "weather_hours_complete",
            "error",
            f"""SELECT CASE WHEN abs(count(DISTINCT observed_hour) - {days * 24}) <= 1 THEN 0 ELSE 1 END, 1
                FROM lake.weather_hourly WHERE source_month = '{month}'""",
            f"{days * 24} hourly weather rows for the month (+-1 for a DST change)",
        ),
        Check(
            "weather_temperature_plausible",
            "warn",
            f"""SELECT count(*) FILTER (WHERE temperature_2m IS NULL OR temperature_2m NOT BETWEEN -30 AND 45), count(*)
                FROM lake.weather_hourly WHERE source_month = '{month}'""",
            "temperature present and between -30 and 45 C",
        ),
    ]


def latest_stream_run(settings: Settings) -> dict[str, Any] | None:
    path = settings.ops_dir / "stream_runs" / "latest.json"
    return json.loads(path.read_text("utf-8")) if path.exists() else None


def stream_checks(settings: Settings, _month: str) -> list[Check]:
    run = latest_stream_run(settings)
    if run is None:
        return []
    recon = run["reconciliation"]
    injected_malformed = sum(r["malformed_injected"] for r in run["replays"])
    distinct_events = run["replays"][0]["events"]
    return [
        Check(
            "stream_counters_match_batch",
            "error",
            f"SELECT {recon['cells_mismatched']}, {max(recon['cells_compared'], 1)}",
            "DynamoDB zone/window counters equal the counts recomputed from the batch trips",
        ),
        Check(
            "stream_exactly_once_accepts",
            "error",
            f"SELECT abs({run['consumer']['accepted']} - {distinct_events}), {distinct_events}",
            "every distinct event accepted exactly once despite injected duplicates and replays",
        ),
        Check(
            "stream_dlq_matches_injected",
            "error",
            f"SELECT abs({run['dlq_messages']} - {injected_malformed}), {max(injected_malformed, 1)}",
            "every injected malformed record reached the dead-letter queue, nothing else did",
        ),
        Check(
            "ride_events_trip_exists",
            "error",
            """SELECT count(*) FILTER (WHERE t.trip_id IS NULL), count(*)
               FROM (SELECT DISTINCT trip_id FROM lake.ride_events) e
               LEFT JOIN lake.trips t USING (trip_id)""",
            "every landed event belongs to a trip in the lake",
        ),
        Check(
            "ride_events_known_type",
            "error",
            """SELECT count(*) FILTER (WHERE event_type NOT IN ('requested', 'driver_on_scene', 'picked_up', 'dropped_off')),
                      count(*) FROM lake.ride_events""",
            "event_type is one of the four lifecycle types",
        ),
    ]


def marts_checks(settings: Settings, month: str) -> list[Check]:
    return [
        Check(
            "mart_hourly_demand_reconciles",
            "error",
            "SELECT abs((SELECT sum(requests) FROM marts.mart_hourly_zone_demand) - (SELECT count(*) FROM staging.stg_trips)), (SELECT count(*) FROM staging.stg_trips)",
            "hourly zone demand sums to the number of trips",
        ),
        Check(
            "mart_borough_flows_reconciles",
            "error",
            "SELECT abs((SELECT sum(trips) FROM marts.mart_borough_flows) - (SELECT count(*) FROM staging.stg_trips)), (SELECT count(*) FROM staging.stg_trips)",
            "borough flows sum to the number of trips",
        ),
        Check(
            "mart_rates_between_0_and_1",
            "error",
            """SELECT (SELECT count(*) FROM marts.mart_hourly_zone_demand WHERE pct_pickup_within_10_min NOT BETWEEN 0 AND 1)
                    + (SELECT count(*) FROM marts.mart_shared_rides WHERE match_rate NOT BETWEEN 0 AND 1),
                      (SELECT count(*) FROM marts.mart_hourly_zone_demand) + (SELECT count(*) FROM marts.mart_shared_rides)""",
            "fulfilment and match rates are proportions",
        ),
        Check(
            "mart_driver_pay_share_plausible",
            "warn",
            "SELECT count(*) FILTER (WHERE driver_pay_share_of_fare NOT BETWEEN 0.4 AND 1.2), count(*) FROM marts.mart_driver_pay_share WHERE trips >= 100",
            "driver pay is 40-120% of the base fare in every cell with >= 100 trips",
        ),
        Check(
            "mart_wait_distribution_complete",
            "error",
            "SELECT abs((SELECT sum(trips) FROM marts.mart_wait_time_distribution) - (SELECT count(*) FROM staging.stg_trips WHERE wait_seconds >= 0)), 1",
            "every trip with a non-negative wait lands in exactly one wait bucket",
        ),
    ]


CHECKS: dict[str, Callable[[Settings, str], list[Check]]] = {
    "landing": landing_checks,
    "lake": lake_checks,
    "stream": stream_checks,
    "marts": marts_checks,
}


# --------------------------------------------------------------------------- runner
def evaluate(con: duckdb.DuckDBPyConnection, check: Check) -> tuple[int, int, str | None]:
    try:
        failing, total = con.execute(check.sql, list(check.params)).fetchone()
    except duckdb.Error as exc:
        return 1, 1, f"check query failed: {str(exc).splitlines()[0][:300]}"
    return int(failing or 0), int(total or 0), None


def run_gate(
    settings: Settings, layer: str, month: str, *, raise_on_error: bool | None = None
) -> list[CheckResult]:
    if layer not in CHECKS:
        raise ValueError(f"layer must be one of {LAYERS}")
    raise_on_error = settings.dq_fail_on_error if raise_on_error is None else raise_on_error
    checks = CHECKS[layer](settings, month)
    run_id = f"{layer}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}"
    now = datetime.now(UTC).replace(tzinfo=None, microsecond=0)
    results: list[CheckResult] = []
    if not checks:
        results.append(
            CheckResult(
                run_id,
                now,
                layer,
                f"{layer}_inputs_present",
                "warn",
                "skip",
                0,
                0,
                0.0,
                "nothing to check (no stream run yet)",
            )
        )
    else:
        if layer in {"lake", "stream"}:
            publish(settings)  # gate exactly the committed Iceberg snapshots
        con = duckdb.connect() if layer == "landing" else connect(settings)
        try:
            for check in checks:
                failing, total, error = evaluate(con, check)
                ratio = failing / total if total else (1.0 if failing else 0.0)
                failed = error is not None or (failing > 0 and ratio > check.threshold)
                results.append(
                    CheckResult(
                        run_id,
                        now,
                        layer,
                        check.name,
                        check.severity,
                        "fail" if failed else "pass",
                        failing,
                        total,
                        check.threshold,
                        error or check.detail,
                    )
                )
        finally:
            con.close()
    _persist(settings, layer, month, results)
    errors = [r for r in results if r.status == "fail" and r.severity == "error"]
    for r in results:
        level = "error" if r.status == "fail" and r.severity == "error" else "info"
        getattr(log, level)("dq check", extra={k: v for k, v in r.to_dict().items() if k != "checked_at"})
    if errors and raise_on_error:
        raise GateFailed(layer, errors)
    return results


def _persist(settings: Settings, layer: str, month: str, results: list[CheckResult]) -> Path:
    out = settings.ops_dir / "dq" / f"{layer}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps({"layer": layer, "month": month, "results": [r.to_dict() for r in results]}, indent=2),
        "utf-8",
    )
    table = ensure_table(get_catalog(settings), settings, DQ_RESULTS)
    table.append(to_arrow(DQ_RESULTS, pa.Table.from_pylist([asdict(r) for r in results])))
    return out


def summarize(results: list[CheckResult]) -> dict[str, Any]:
    return {
        "checks": len(results),
        "passed": sum(r.status == "pass" for r in results),
        "warned": sum(r.status == "fail" and r.severity == "warn" for r in results),
        "errors": sum(r.status == "fail" and r.severity == "error" for r in results),
        "skipped": sum(r.status == "skip" for r in results),
    }
