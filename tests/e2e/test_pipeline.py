"""End-to-end on the committed real fixtures: the whole state machine, dbt included, then reruns.

The first module-scoped run executes the daily-batch ASL locally (ingest -> landing gate ->
Iceberg load -> lake gate -> replay through emulated Kinesis/Lambda/DynamoDB -> event load ->
stream gate -> dbt build -> marts gate). Later tests rerun pieces and assert nothing changes.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import duckdb
import pytest

from tests.conftest import PUBLISHED_FIXTURES, TLC_FIXTURES, WEATHER_FIXTURES
from uber_rides.cli import main

pytestmark = pytest.mark.e2e

MARTS = (
    "mart_hourly_zone_demand",
    "mart_wait_time_distribution",
    "mart_wait_time_percentiles",
    "mart_driver_pay_share",
    "mart_airport_trips",
    "mart_cbd_congestion_fee",
    "mart_shared_rides",
    "mart_weather_demand",
    "mart_borough_flows",
    "fct_daily_summary",
)


def cli(data_dir: Path, *args: str) -> int:
    return main(
        [
            "--data-dir",
            str(data_dir),
            "--source-dir",
            str(TLC_FIXTURES),
            "--weather-dir",
            str(WEATHER_FIXTURES),
            *args,
        ]
    )


def fingerprint(db: Path) -> dict[str, str]:
    con = duckdb.connect(str(db), read_only=True)
    try:
        out = {}
        for mart in MARTS:
            rows = con.execute(f"SELECT * FROM marts.{mart} ORDER BY ALL").fetchall()
            # parallel float aggregation is not bit-for-bit reproducible: compare at 9 digits
            norm = [tuple(f"{v:.9g}" if isinstance(v, float) else v for v in r) for r in rows]
            out[mart] = hashlib.sha256(repr(norm).encode()).hexdigest()
        return out
    finally:
        con.close()


def scalar(db: Path, sql: str):
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql).fetchone()[0]
    finally:
        con.close()


REPLAY = [
    "--replay-start",
    "2026-06-12T18:00",
    "--replay-end",
    "2026-06-12T18:01",
    "--replay-sample-rate",
    "0.5",
]


@pytest.fixture(scope="module")
def lake(tmp_path_factory: pytest.TempPathFactory) -> Path:
    data_dir = tmp_path_factory.mktemp("e2e") / "lake"
    rc = cli(
        data_dir,
        "batch",
        "--month",
        "2026-06",
        *REPLAY,
        "--duplicate-every",
        "5",
        "--malformed-every",
        "9",
        "--replays",
        "2",
    )
    assert rc == 0
    return data_dir


def test_state_machine_succeeded_through_every_state(lake: Path):
    run = json.loads((lake / "ops" / "batch_runs" / "latest.json").read_text("utf-8"))
    assert run["status"] == "SUCCEEDED"
    assert run["path"][-3:] == ["GateMarts", "MartsPassed?", "BatchSucceeded"]
    assert "ReplayWindow" in run["path"]


def test_gates_have_no_error_failures(lake: Path):
    for layer in ("landing", "lake", "stream", "marts"):
        doc = json.loads((lake / "ops" / "dq" / f"{layer}.json").read_text("utf-8"))
        errors = [r for r in doc["results"] if r["status"] == "fail" and r["severity"] == "error"]
        assert not errors, (layer, errors)


def test_stream_is_exactly_once_and_dead_letters_the_malformed(lake: Path):
    run = json.loads((lake / "ops" / "stream_runs" / "latest.json").read_text("utf-8"))
    assert run["reconciliation"]["ok"]
    assert run["consumer"]["accepted"] == run["replays"][0]["events"]
    assert run["dlq_messages"] == sum(r["malformed_injected"] for r in run["replays"]) > 0
    db = lake / "serving" / "uber.duckdb"
    assert scalar(db, "SELECT count(*) FROM staging.stg_ride_events") == run["replays"][0]["events"]


def test_marts_reconcile_to_raw_trips(lake: Path):
    db = lake / "serving" / "uber.duckdb"
    assert scalar(db, "SELECT sum(trips) FROM marts.fct_daily_summary") == 2647
    assert scalar(db, "SELECT sum(requests) FROM marts.mart_hourly_zone_demand") == 2647
    assert scalar(db, "SELECT count(*) FROM marts.mart_weather_demand WHERE temperature_c IS NULL") == 0


def test_verify_and_report_on_fixtures(lake: Path, tmp_path: Path):
    assert cli(lake, "verify", "--month", "2026-06", "--offline-dir", str(PUBLISHED_FIXTURES)) == 0
    ver = json.loads((lake / "ops" / "verification" / "2026-06.json").read_text("utf-8"))
    assert all(c["status"] == "match" for c in ver["internal_checks"])
    assert cli(lake, "report", "--out", str(tmp_path / "site")) == 0
    page = (tmp_path / "site" / "index.html").read_text("utf-8")
    assert "2,647" in page and "Simulated" in page
    summary = json.loads((tmp_path / "site" / "summary.json").read_text("utf-8"))
    assert summary["trips"] == 2647 and summary["dq_errors"] == 0


def test_rerunning_the_batch_changes_nothing(lake: Path):
    db = lake / "serving" / "uber.duckdb"
    before = fingerprint(db)
    trips_before = scalar(db, "SELECT count(*) FROM lake.trips")
    # same month again, forced through ingest + load, plus the same replay window again
    assert cli(lake, "batch", "--month", "2026-06", "--force", *REPLAY) == 0
    assert scalar(db, "SELECT count(*) FROM lake.trips") == trips_before == 2647
    assert fingerprint(db) == before
    # the second replay landed every event again (at-least-once); staging still dedupes
    assert scalar(db, "SELECT count(*) - count(DISTINCT event_id) FROM lake.ride_events") > 0
    assert scalar(db, "SELECT count(*) - count(DISTINCT event_id) FROM staging.stg_ride_events") == 0


def test_rerunning_event_load_is_a_no_op(lake: Path):
    assert cli(lake, "load-events") == 0
    db = lake / "serving" / "uber.duckdb"
    n = scalar(db, "SELECT count(*) FROM lake.ride_events")
    assert cli(lake, "load-events") == 0
    assert cli(lake, "publish") == 0
    assert scalar(db, "SELECT count(*) FROM lake.ride_events") == n


def test_processed_month_is_skipped_without_force(lake: Path):
    assert cli(lake, "batch", "--month", "2026-06") == 0
    run = json.loads((lake / "ops" / "batch_runs" / "latest.json").read_text("utf-8"))
    assert "IngestMonth" not in run["path"]
