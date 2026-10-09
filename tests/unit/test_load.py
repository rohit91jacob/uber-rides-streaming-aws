"""Batch loads into Iceberg: hard rules, idempotent month replacement, zones, weather."""

from __future__ import annotations

from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

from tests.conftest import TLC_FIXTURES
from uber_rides.lake import load
from uber_rides.lake.catalog import get_catalog, local_path
from uber_rides.lake.tables import TAXI_ZONES, TRIPS, TRIPS_QUARANTINE, WEATHER_HOURLY
from uber_rides.sources import tlc, weather


def _count(settings, tdef, month=None):
    data = get_catalog(settings).load_table(tdef.identifier(settings)).scan().to_arrow()
    if month is None:
        return data.num_rows
    return sum(1 for m in data.column("source_month").to_pylist() if m == month)


def test_load_month_twice_gives_identical_rows(settings):
    tlc.ingest_month(settings, "2026-06")
    first = load.load_trips(settings, "2026-06")
    second = load.load_trips(settings, "2026-06")
    assert first["trips_written"] == second["trips_written"] == 2647
    assert _count(settings, TRIPS, "2026-06") == 2647
    assert _count(settings, TRIPS_QUARANTINE) == 0
    ids = (
        get_catalog(settings)
        .load_table(TRIPS.identifier(settings))
        .scan(selected_fields=("trip_id",))
        .to_arrow()
    )
    assert len(set(ids.column("trip_id").to_pylist())) == 2647


def test_hard_rules_quarantine_unplaceable_trips(settings, tmp_path):
    """TEST DATA: real fixture rows with three of them deliberately broken in this test only."""
    raw = pq.read_table(TLC_FIXTURES / "fhvhv_tripdata_2026-06.parquet").slice(0, 50).to_pylist()
    a, b, c = [i for i, r in enumerate(raw) if r["hvfhs_license_num"] == "HV0003"][:3]
    raw[a]["dropoff_datetime"] = datetime(2026, 6, 1, 0, 0)
    raw[a]["pickup_datetime"] = datetime(2026, 6, 2, 0, 0)
    raw[b]["PULocationID"] = 999
    raw[c]["pickup_datetime"] = datetime(2026, 3, 1, 0, 0)
    raw[c]["dropoff_datetime"] = datetime(2026, 3, 1, 0, 30)
    src = tmp_path / "src"
    src.mkdir()
    schema = pq.read_schema(TLC_FIXTURES / "fhvhv_tripdata_2026-06.parquet")
    pq.write_table(pa.Table.from_pylist(raw, schema=schema), src / "fhvhv_tripdata_2026-06.parquet")
    (src / "taxi_zone_lookup.csv").write_bytes((TLC_FIXTURES / "taxi_zone_lookup.csv").read_bytes())
    s = settings.with_overrides(tlc_source_dir=src)
    tlc.ingest_month(s, "2026-06")
    staged = load.stage_month(s, "2026-06")
    assert staged["rejected_by_rule"] == {
        "dropoff_before_pickup": 1,
        "pickup_outside_file_month": 1,
        "zone_out_of_range": 1,
    }
    assert staged["clean_rows"] + staged["rejected_rows"] == sum(
        1 for r in raw if r["hvfhs_license_num"] == "HV0003"
    )


def test_trip_flags_are_derived(settings):
    tlc.ingest_month(settings, "2026-06")
    load.load_trips(settings, "2026-06")
    data = get_catalog(settings).load_table(TRIPS.identifier(settings)).scan().to_arrow().to_pylist()
    unknown = [r for r in data if r["is_unknown_zone"]]
    assert all(r["pu_location_id"] > 263 or r["do_location_id"] > 263 for r in unknown)
    assert all(r["trip_id"].startswith("HV0003-2026-06-") for r in data)
    assert {r["hvfhs_license_num"] for r in data} == {"HV0003"}


def test_zones_and_weather_load_idempotently(settings):
    tlc.ingest_month(settings, "2026-06")
    weather.fetch_month(settings, "2026-06")
    for _ in range(2):
        assert load.load_zones(settings) == {"zones": 265}
        assert load.load_weather(settings, "2026-06") == {"weather_hours": 720}
    assert _count(settings, TAXI_ZONES) == 265
    assert _count(settings, WEATHER_HOURLY) == 720


def test_local_path_handles_windows_and_posix_uris():
    assert local_path("file:///C:/x/y").as_posix() == "C:/x/y"
    assert local_path("file:///tmp/x").as_posix() == "/tmp/x"
