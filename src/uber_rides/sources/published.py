"""Independently published TLC figures used to verify the pipeline's outputs.

* **TLC monthly data reports** (``data_reports_monthly.csv``, linked from the TLC "Aggregated
  Reports" page): one row per month and licence class. The ``FHV - High Volume`` class covers
  every HVFHS licensee (Uber *and* Lyft), so it is compared with all licensees in the raw file.
* **FHV Base Aggregate Report** (NYC Open Data ``2v9c-2k7f``, Socrata API, no key): dispatched
  trips per base per month, published about two months after the month ends. Uber dispatches
  through its own bases, so this verifies the Uber (HV0003) numbers base by base.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

from uber_rides.config import Settings
from uber_rides.logs import get_logger
from uber_rides.sources.http import land_file, session
from uber_rides.sources.tlc import parse_month

log = get_logger(__name__)

MONTHLY_FILE = "data_reports_monthly.csv"
HV_CLASS = "FHV - High Volume"


def _num(value: str | None) -> float | None:
    if value is None:
        return None
    v = value.strip().replace(",", "").replace("%", "")
    if v in {"", "-"}:
        return None
    return float(v)


def monthly_report_path(settings: Settings) -> Path:
    return settings.published_dir / MONTHLY_FILE


def fetch_monthly_report(settings: Settings, *, offline_dir: Path | None = None) -> Path:
    local = offline_dir / MONTHLY_FILE if offline_dir else None
    landed = land_file(
        settings, settings.monthly_report_url, monthly_report_path(settings), local_source=local
    )
    return Path(landed.path)


def hv_monthly_row(path: Path, month: str) -> dict | None:
    """The ``FHV - High Volume`` row of the monthly report for ``month`` (parsed numbers)."""
    with path.open(encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            if row.get("Month/Year") == month and row.get("License Class") == HV_CLASS:
                return {
                    "month": month,
                    "license_class": HV_CLASS,
                    "trips_per_day": _num(row.get("Trips Per Day")),
                    "trips_per_day_shared": _num(row.get("Trips Per Day Shared")),
                    "avg_minutes_per_trip": _num(row.get("Avg Minutes Per Trip")),
                    "unique_vehicles": _num(row.get("Unique Vehicles")),
                    "unique_drivers": _num(row.get("Unique Drivers")),
                }
    return None


def base_aggregate_path(settings: Settings, month: str) -> Path:
    return settings.published_dir / f"fhv_base_aggregate_{month}.json"


def fetch_base_aggregate(settings: Settings, month: str, *, offline_dir: Path | None = None) -> Path:
    """All bases' rows for ``month`` from the Socrata API (cached; empty list if not yet published)."""
    target = base_aggregate_path(settings, month)
    if offline_dir and (offline_dir / target.name).exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((offline_dir / target.name).read_bytes())
        return target
    year, mon = parse_month(month)
    params = {"$where": f"year='{year}' AND month='{mon}'", "$limit": "50000"}
    r = session(settings).get(settings.base_aggregate_url, params=params, timeout=settings.http_timeout)
    r.raise_for_status()
    rows = r.json()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(rows, indent=1), "utf-8")
    log.info("base aggregate landed", extra={"month": month, "rows": len(rows)})
    return target


def base_trips(path: Path) -> dict[str, dict]:
    rows = json.loads(path.read_text("utf-8"))
    return {
        r["base_license_number"].strip().upper(): {
            "base_name": r.get("base_name"),
            "total_dispatched_trips": int(float(r.get("total_dispatched_trips") or 0)),
            "total_dispatched_shared_trips": int(float(r.get("total_dispatched_shared_trips") or 0)),
        }
        for r in rows
    }
