"""Reconcile pipeline outputs with figures the TLC publishes independently.

Comparisons for a month (each reported with the published value, ours, the difference and a
status, never silently adjusted):

1. FHV Base Aggregate Report: ``UBER`` dispatched trips and shared trips vs our HV0003 rows,
   and ``LYFT`` vs HV0005 (published per month, about two months after it ends).
2. TLC monthly data report, ``FHV - High Volume`` class (all HVFHS licensees combined):
   trips per day, shared trips per day and average minutes per trip.
3. Internal: the lake and the marts against the raw file (no row lost between layers).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from uber_rides.config import Settings
from uber_rides.lake.serving import connect
from uber_rides.logs import get_logger
from uber_rides.sources import published, tlc

log = get_logger(__name__)

BASE_AGGREGATE_PAGE = "https://data.cityofnewyork.us/Transportation/FHV-Base-Aggregate-Report/2v9c-2k7f"
MONTHLY_REPORT_PAGE = "https://www.nyc.gov/site/tlc/about/aggregated-reports.page"


def _status(diff_pct: float | None, tolerance_pct: float) -> str:
    if diff_pct is None:
        return "not_published"
    if abs(diff_pct) <= tolerance_pct:
        return "match"
    if abs(diff_pct) <= 3 * tolerance_pct:
        return "close"
    return "mismatch"


def compare(
    name: str, ours: float | None, theirs: float | None, *, tolerance_pct: float, source: str, note: str = ""
) -> dict[str, Any]:
    diff = None if ours is None or theirs is None else ours - theirs
    diff_pct = None if diff is None or not theirs else 100.0 * diff / theirs
    return {
        "check": name,
        "ours": ours,
        "published": theirs,
        "diff": diff,
        "diff_pct": None if diff_pct is None else round(diff_pct, 3),
        "tolerance_pct": tolerance_pct,
        "status": _status(diff_pct, tolerance_pct),
        "source": source,
        "note": note,
    }


def _licensee(profile: dict, license_num: str) -> dict | None:
    return next((r for r in profile["licensees"] if r["license_num"] == license_num), None)


def verify_month(settings: Settings, month: str, *, offline_dir: Path | None = None) -> dict[str, Any]:
    profile_path = tlc.profile_path(settings, month)
    if not profile_path.exists():
        raise FileNotFoundError(f"no source profile for {month}; run `uber-rides ingest --month {month}`")
    profile = json.loads(profile_path.read_text("utf-8"))
    start, end = tlc.month_bounds(month)
    days = (end - start).days
    empty = {"trips": 0, "shared_matched": 0, "shared_requested": 0}
    uber = _licensee(profile, "HV0003") or empty
    lyft = _licensee(profile, "HV0005") or empty
    all_trips = sum(r["trips"] for r in profile["licensees"])
    all_shared = sum(r["shared_matched"] for r in profile["licensees"])
    all_seconds = sum(r["trip_seconds"] or 0 for r in profile["licensees"])
    results: list[dict[str, Any]] = []

    # 1. FHV Base Aggregate Report (per base; Uber and Lyft report under one base each).
    base_path = published.fetch_base_aggregate(settings, month, offline_dir=offline_dir)
    bases = published.base_trips(base_path)
    src = f"FHV Base Aggregate Report ({BASE_AGGREGATE_PAGE})"
    for label, base, ours in (("Uber", "UBER", uber), ("Lyft", "LYFT", lyft)):
        pub = bases.get(base)
        results.append(
            compare(
                f"{label} dispatched trips",
                ours["trips"],
                pub["total_dispatched_trips"] if pub else None,
                tolerance_pct=1.0,
                source=src,
                note=f"raw-file rows for {('HV0003' if base == 'UBER' else 'HV0005')} vs base '{base}'",
            )
        )
        results.append(
            compare(
                f"{label} shared trips",
                ours["shared_matched"],
                pub["total_dispatched_shared_trips"] if pub else None,
                tolerance_pct=5.0,
                source=src,
                note="our shared_match_flag = 'Y' rows vs total_dispatched_shared_trips (definitions may differ)",
            )
        )
        results.append(
            compare(
                f"{label} shared requests",
                ours.get("shared_requested"),
                pub["total_dispatched_shared_trips"] if pub else None,
                tolerance_pct=5.0,
                source=src,
                note="our shared_request_flag = 'Y' rows vs total_dispatched_shared_trips (alternative definition)",
            )
        )

    # 2. TLC monthly data report (HV class = every HVFHS licensee).
    report_path = published.fetch_monthly_report(settings, offline_dir=offline_dir)
    hv = published.hv_monthly_row(report_path, month)
    src = f"TLC monthly data reports, class 'FHV - High Volume' ({MONTHLY_REPORT_PAGE})"
    results.append(
        compare(
            "HVFHS trips per day (all licensees)",
            round(all_trips / days, 1),
            hv["trips_per_day"] if hv else None,
            tolerance_pct=1.0,
            source=src,
            note=f"{all_trips:,} raw-file trips / {days} days",
        )
    )
    results.append(
        compare(
            "HVFHS shared trips per day (all licensees)",
            round(all_shared / days, 1),
            hv["trips_per_day_shared"] if hv else None,
            tolerance_pct=5.0,
            source=src,
            note="shared_match_flag = 'Y' rows / days",
        )
    )
    results.append(
        compare(
            "HVFHS average minutes per trip",
            round(all_seconds / all_trips / 60.0, 2) if all_trips else None,
            hv["avg_minutes_per_trip"] if hv else None,
            tolerance_pct=2.5,
            source=src,
            note="mean trip_time; the report rounds to whole minutes",
        )
    )

    # 3. Internal reconciliation through the layers (only if the month is loaded).
    internal = _internal(settings, month, uber["trips"])
    out = {
        "month": month,
        "license_num": settings.license_num,
        "published_checks": results,
        "internal_checks": internal,
        "summary": {
            "match": sum(r["status"] == "match" for r in results),
            "close": sum(r["status"] == "close" for r in results),
            "mismatch": sum(r["status"] == "mismatch" for r in results),
            "not_published": sum(r["status"] == "not_published" for r in results),
        },
    }
    path = settings.ops_dir / "verification" / f"{month}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2), "utf-8")
    log.info("verification written", extra={"month": month, **out["summary"]})
    return out


def _internal(settings: Settings, month: str, raw_uber_trips: int) -> list[dict[str, Any]]:
    if not settings.serving_db.exists():
        return []
    con = connect(settings, read_only=True)
    checks = []
    try:
        queries = {
            "lake trips + quarantine = raw HV0003 rows": (
                f"SELECT (SELECT count(*) FROM lake.trips WHERE source_month = '{month}') + "
                f"(SELECT count(*) FROM lake.trips_quarantine WHERE source_month = '{month}')"
            ),
            "dbt int_trips_enriched + quarantine = raw HV0003 rows": (
                f"SELECT (SELECT count(*) FROM intermediate.int_trips_enriched WHERE source_month = '{month}') + "
                f"(SELECT count(*) FROM lake.trips_quarantine WHERE source_month = '{month}')"
            ),
        }
        for name, sql in queries.items():
            try:
                value = con.execute(sql).fetchone()[0]
            except Exception as exc:  # table not built yet
                checks.append({"check": name, "status": "skipped", "note": str(exc).splitlines()[0][:200]})
                continue
            checks.append(
                {
                    "check": name,
                    "ours": int(value or 0),
                    "expected": raw_uber_trips,
                    "status": (
                        "not_loaded" if not value else "match" if int(value) == raw_uber_trips else "mismatch"
                    ),
                }
            )
    finally:
        con.close()
    return checks
