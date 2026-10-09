from __future__ import annotations

from datetime import date

import pytest

from tests.conftest import PUBLISHED_FIXTURES, TLC_FIXTURES, WEATHER_FIXTURES
from uber_rides.sources import published, tlc, weather


def test_month_helpers():
    assert tlc.parse_month("2026-06") == (2026, 6)
    assert tlc.month_bounds("2026-12") == (date(2026, 12, 1), date(2027, 1, 1))
    assert tlc.previous_months("2026-02", 3) == ["2026-02", "2026-01", "2025-12"]
    assert tlc.trip_file_name("2026-06") == "fhvhv_tripdata_2026-06.parquet"
    for bad in ("2026-13", "26-06", "2026/06"):
        with pytest.raises(ValueError, match="YYYY-MM"):
            tlc.parse_month(bad)


def test_fixture_has_the_25_hvfhv_columns():
    cols = tlc.check_schema(TLC_FIXTURES / "fhvhv_tripdata_2026-06.parquet")
    assert set(tlc.EXPECTED_COLUMNS) <= set(cols)
    assert len(tlc.EXPECTED_COLUMNS) == 25


def test_profile_counts_licensees_and_bases():
    profile = tlc.profile_month(TLC_FIXTURES / "fhvhv_tripdata_2026-06.parquet", "2026-06").to_dict()
    by = {r["license_num"]: r for r in profile["licensees"]}
    assert by["HV0003"]["trips"] == 2647 and by["HV0005"]["trips"] == 556
    assert sum(b["trips"] for b in by["HV0003"]["bases"]) == 2647
    assert by["HV0003"]["licensee"] == "Uber"


def test_ingest_from_local_source_is_idempotent(settings):
    first = tlc.ingest_month(settings, "2026-06")
    second = tlc.ingest_month(settings, "2026-06")
    assert first["trips_file"]["changed"] is True
    assert second["trips_file"]["changed"] is False
    assert first["trips_file"]["sha256"] == second["trips_file"]["sha256"]


def test_weather_fixture_flattens_to_hours(settings):
    summary = weather.fetch_month(settings, "2026-06")
    assert summary["hours"] == 720
    rows = weather.hourly_rows(weather.weather_file(settings, "2026-06"))
    assert rows[0]["observed_hour"].isoformat() == "2026-06-01T00:00:00"
    assert set(weather.HOURLY_VARIABLES) <= set(rows[0])
    assert WEATHER_FIXTURES.exists()


def test_monthly_report_parsing():
    row = published.hv_monthly_row(PUBLISHED_FIXTURES / "data_reports_monthly.csv", "2026-06")
    assert row["trips_per_day"] == 707670
    assert row["trips_per_day_shared"] == 8619
    assert published.hv_monthly_row(PUBLISHED_FIXTURES / "data_reports_monthly.csv", "1999-01") is None


def test_base_aggregate_parsing():
    bases = published.base_trips(PUBLISHED_FIXTURES / "fhv_base_aggregate_2026-05.json")
    assert bases["UBER"]["total_dispatched_trips"] == 15354816
    assert bases["LYFT"]["total_dispatched_shared_trips"] == 2022


@pytest.mark.parametrize(
    ("raw", "value"), [("707,670", 707670.0), ("-", None), ("81%", 81.0), ("", None), (None, None)]
)
def test_number_parsing(raw, value):
    assert published._num(raw) == value
