from __future__ import annotations

import duckdb
import pytest

from uber_rides import report, verify
from uber_rides.config import ConfigError, Settings
from uber_rides.lake import load
from uber_rides.quality import gates
from uber_rides.sources import tlc, weather


def test_check_threshold_semantics():
    con = duckdb.connect()
    ok = gates.Check("c", "error", "SELECT 1, 1000", "", threshold=0.01)
    bad = gates.Check("c", "error", "SELECT 20, 1000", "", threshold=0.01)
    broken = gates.Check("c", "error", "SELECT * FROM nowhere", "")
    assert gates.evaluate(con, ok)[:2] == (1, 1000)
    assert gates.evaluate(con, bad)[:2] == (20, 1000)
    failing, total, error = gates.evaluate(con, broken)
    assert (failing, total) == (1, 1) and "check query failed" in error


def _loaded(settings: Settings) -> Settings:
    tlc.ingest_month(settings, "2026-06")
    weather.fetch_month(settings, "2026-06")
    load.load_trips(settings, "2026-06")
    load.load_zones(settings)
    load.load_weather(settings, "2026-06")
    return settings


def test_lake_gate_passes_on_real_fixture_and_persists(settings):
    results = gates.run_gate(_loaded(settings), "lake", "2026-06")
    summary = gates.summarize(results)
    assert summary["errors"] == 0
    assert {r.check_name for r in results} >= {
        "trips_reconcile_with_source",
        "zones_complete",
        "weather_hours_complete",
    }
    assert (settings.ops_dir / "dq" / "lake.json").exists()


def test_lake_gate_fails_when_rows_are_lost(settings):
    s = _loaded(settings)
    profile = tlc.profile_path(s, "2026-06")
    profile.write_text(profile.read_text("utf-8").replace('"trips": 2647', '"trips": 2700'), "utf-8")
    with pytest.raises(gates.GateFailed) as err:
        gates.run_gate(s, "lake", "2026-06")
    assert [f.check_name for f in err.value.failed] == ["trips_reconcile_with_source"]


def test_stream_gate_skips_without_a_run(settings):
    results = gates.run_gate(_loaded(settings), "stream", "2026-06")
    assert [r.status for r in results] == ["skip"]


def test_compare_statuses():
    assert verify.compare("x", 101, 100, tolerance_pct=1.0, source="s")["status"] == "match"
    assert verify.compare("x", 102, 100, tolerance_pct=1.0, source="s")["status"] == "close"
    assert verify.compare("x", 110, 100, tolerance_pct=1.0, source="s")["status"] == "mismatch"
    assert verify.compare("x", 110, None, tolerance_pct=1.0, source="s")["status"] == "not_published"


def test_report_helpers():
    assert report.fmt(1234567) == "1,234,567"
    assert report.fmt(1.23456) == "1.23"
    assert report.pct(0.1234) == "12.3%"
    svg = report.bar_chart("t", ["a", "b"], [1.0, 2.0])
    assert svg.count('class="bar"') == 2 and "<title>b: 2.00</title>" in svg
    assert "no rows" in report.table("c", ["h"], [])


def test_settings_validation(monkeypatch):
    monkeypatch.setenv("UBER_COUNTER_WINDOW_MINUTES", "7")
    with pytest.raises(ConfigError):
        Settings.from_env()
    monkeypatch.setenv("UBER_COUNTER_WINDOW_MINUTES", "15")
    monkeypatch.setenv("UBER_AWS_MODE", "endpoint")
    monkeypatch.delenv("UBER_AWS_ENDPOINT_URL", raising=False)
    with pytest.raises(ConfigError):
        Settings.from_env()
    monkeypatch.setenv("UBER_AWS_ENDPOINT_URL", "http://localhost:4566")
    assert Settings.from_env().aws_endpoint_url == "http://localhost:4566"
