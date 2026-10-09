"""NYC TLC High Volume For-Hire Vehicle (HVFHV) trip records and the taxi zone lookup.

Monthly files: ``<base>/fhvhv_tripdata_YYYY-MM.parquet`` (about 500 MB, all HVFHS licensees).
TLC publishes each month with a lag of roughly two months. This module lands the raw file
unchanged and writes a small per-licensee *source profile* used by verification.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb

from uber_rides.config import Settings
from uber_rides.logs import get_logger
from uber_rides.sources.http import LandedFile, land_file, session

log = get_logger(__name__)

MONTH_RE = re.compile(r"^(20\d\d)-(0[1-9]|1[0-2])$")
ZONES_FILE = "taxi_zone_lookup.csv"

# The 25 columns of the HVFHV file (TLC data dictionary, March 2025 revision with
# cbd_congestion_fee). Values are the DuckDB types we read them as.
EXPECTED_COLUMNS: dict[str, str] = {
    "hvfhs_license_num": "VARCHAR",
    "dispatching_base_num": "VARCHAR",
    "originating_base_num": "VARCHAR",
    "request_datetime": "TIMESTAMP",
    "on_scene_datetime": "TIMESTAMP",
    "pickup_datetime": "TIMESTAMP",
    "dropoff_datetime": "TIMESTAMP",
    "PULocationID": "INTEGER",
    "DOLocationID": "INTEGER",
    "trip_miles": "DOUBLE",
    "trip_time": "BIGINT",
    "base_passenger_fare": "DOUBLE",
    "tolls": "DOUBLE",
    "bcf": "DOUBLE",
    "sales_tax": "DOUBLE",
    "congestion_surcharge": "DOUBLE",
    "airport_fee": "DOUBLE",
    "tips": "DOUBLE",
    "driver_pay": "DOUBLE",
    "shared_request_flag": "VARCHAR",
    "shared_match_flag": "VARCHAR",
    "access_a_ride_flag": "VARCHAR",
    "wav_request_flag": "VARCHAR",
    "wav_match_flag": "VARCHAR",
    "cbd_congestion_fee": "DOUBLE",
}

PROFILE_VERSION = 2
LICENSEES = {"HV0002": "Juno", "HV0003": "Uber", "HV0004": "Via", "HV0005": "Lyft"}


class SchemaError(ValueError):
    """The landed file does not have the expected HVFHV columns."""


def parse_month(month: str) -> tuple[int, int]:
    m = MONTH_RE.match(month)
    if not m:
        raise ValueError(f"month must look like YYYY-MM, got {month!r}")
    return int(m.group(1)), int(m.group(2))


def month_bounds(month: str) -> tuple[date, date]:
    """First day of ``month`` and first day of the next month."""
    year, mon = parse_month(month)
    start = date(year, mon, 1)
    end = date(year + (mon == 12), mon % 12 + 1, 1)
    return start, end


def previous_months(month: str, n: int) -> list[str]:
    year, mon = parse_month(month)
    out = []
    for _ in range(n):
        out.append(f"{year:04d}-{mon:02d}")
        year, mon = (year - 1, 12) if mon == 1 else (year, mon - 1)
    return out


def trip_file_name(month: str) -> str:
    parse_month(month)
    return f"fhvhv_tripdata_{month}.parquet"


def trip_url(settings: Settings, month: str) -> str:
    return f"{settings.tlc_base_url}/{trip_file_name(month)}"


def landed_trip_path(settings: Settings, month: str) -> Path:
    return settings.tlc_landing_dir / trip_file_name(month)


def landed_zones_path(settings: Settings) -> Path:
    return settings.tlc_landing_dir / ZONES_FILE


def latest_available_month(settings: Settings, today: date | None = None, lookback: int = 8) -> str:
    """Newest month whose HVFHV file exists (HEAD 200), searching back from last month."""
    today = today or date.today()
    start = f"{today.year:04d}-{today.month:02d}"
    s = session(settings)
    for month in previous_months(start, lookback + 1)[1:]:
        if settings.tlc_source_dir and (settings.tlc_source_dir / trip_file_name(month)).exists():
            return month
        r = s.head(trip_url(settings, month), timeout=settings.http_timeout, allow_redirects=True)
        if r.status_code == 200:
            return month
    raise LookupError(f"no HVFHV file found in the {lookback} months before {start}")


def check_schema(path: Path) -> dict[str, str]:
    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet(?))",
            [str(path)],
        ).fetchall()
    finally:
        con.close()
    found = dict(rows)
    missing = sorted(set(EXPECTED_COLUMNS) - set(found))
    if missing:
        raise SchemaError(f"{path.name}: missing columns {missing}")
    wrong = {
        c: found[c]
        for c, t in EXPECTED_COLUMNS.items()
        if found[c] != t and not (t == "BIGINT" and found[c] in {"INTEGER", "BIGINT"})
    }
    if wrong:
        raise SchemaError(f"{path.name}: unexpected column types {wrong}")
    return found


@dataclass
class SourceProfile:
    """Per-licensee totals straight from the raw file, before any cleaning."""

    month: str
    rows: list[dict]

    def to_dict(self) -> dict:
        return {"version": PROFILE_VERSION, "month": self.month, "licensees": self.rows}


def profile_month(path: Path, month: str) -> SourceProfile:
    start, end = month_bounds(month)
    con = duckdb.connect()
    try:
        cur = con.execute(
            """
            SELECT hvfhs_license_num AS license_num,
                   count(*) AS trips,
                   count(*) FILTER (WHERE pickup_datetime >= ?::DATE AND pickup_datetime < ?::DATE)
                       AS trips_in_month,
                   count(*) FILTER (WHERE shared_request_flag = 'Y') AS shared_requested,
                   count(*) FILTER (WHERE shared_match_flag = 'Y') AS shared_matched,
                   sum(trip_time) AS trip_seconds,
                   sum(trip_miles) AS trip_miles,
                   sum(base_passenger_fare) AS base_passenger_fare,
                   sum(driver_pay) AS driver_pay,
                   count(DISTINCT CAST(pickup_datetime AS DATE)) AS days
            FROM read_parquet(?)
            GROUP BY 1 ORDER BY 1
            """,
            [start.isoformat(), end.isoformat(), str(path)],
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
        bases = con.execute(
            """
            SELECT hvfhs_license_num, dispatching_base_num, count(*) AS trips,
                   count(*) FILTER (WHERE shared_match_flag = 'Y') AS shared_matched
            FROM read_parquet(?) GROUP BY 1, 2 ORDER BY 1, 2
            """,
            [str(path)],
        ).fetchall()
    finally:
        con.close()
    for row in rows:
        row["licensee"] = LICENSEES.get(row["license_num"], "unknown")
        for k, v in list(row.items()):
            if hasattr(v, "__float__") and not isinstance(v, int | float):
                row[k] = float(v)
    by_license: dict[str, list[dict]] = {}
    for lic, base, trips, shared in bases:
        by_license.setdefault(lic, []).append(
            {"dispatching_base_num": base, "trips": trips, "shared_matched": shared}
        )
    for row in rows:
        row["bases"] = by_license.get(row["license_num"], [])
    return SourceProfile(month=month, rows=rows)


def profile_path(settings: Settings, month: str) -> Path:
    return settings.ops_dir / "source_profile" / f"{month}.json"


def ingest_month(settings: Settings, month: str, *, force: bool = False) -> dict:
    """Land the month's HVFHV file + zone lookup, check the schema, write the source profile."""
    src_dir = settings.tlc_source_dir
    trips = land_file(
        settings,
        trip_url(settings, month),
        landed_trip_path(settings, month),
        local_source=src_dir / trip_file_name(month) if src_dir else None,
        force=force,
    )
    zones = ingest_zones(settings, force=force)
    columns = check_schema(Path(trips.path))
    prof_path = profile_path(settings, month)
    stale = (
        not prof_path.exists() or json.loads(prof_path.read_text("utf-8")).get("version") != PROFILE_VERSION
    )
    if trips.changed or stale:
        profile = profile_month(Path(trips.path), month)
        prof_path.parent.mkdir(parents=True, exist_ok=True)
        prof_path.write_text(json.dumps(profile.to_dict(), indent=2, default=str), "utf-8")
    profile = json.loads(prof_path.read_text("utf-8"))
    log.info("ingested month", extra={"month": month, "changed": trips.changed})
    return {
        "month": month,
        "trips_file": trips.to_dict(),
        "zones_file": zones.to_dict(),
        "columns": len(columns),
        "profile": profile,
    }


def ingest_zones(settings: Settings, *, force: bool = False) -> LandedFile:
    src_dir = settings.tlc_source_dir
    return land_file(
        settings,
        settings.zones_url,
        landed_zones_path(settings),
        local_source=src_dir / ZONES_FILE if src_dir else None,
        force=force,
    )
