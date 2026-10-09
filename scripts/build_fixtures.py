"""Rebuild the committed test fixtures from real downloads (run once; output is committed).

    uv run python scripts/build_fixtures.py --tlc ~/projects/_data/tlc/fhvhv_tripdata_2026-06.parquet

Fixtures (all real records, deterministic selection, each file < 300 KB):

* ``tests/fixtures/tlc/fhvhv_tripdata_2026-06.parquet``: raw 25-column TLC rows:
  every Uber trip requested 2026-06-12 18:00-18:02 (a dense window for the streaming replay),
  plus every 10,000th Uber row and every 10,000th Lyft row of the month (file order).
* ``tests/fixtures/tlc/taxi_zone_lookup.csv``: the TLC zone lookup, unchanged.
* ``tests/fixtures/weather/open_meteo_hourly_2026-06.json``: Open-Meteo hourly archive, June 2026.
* ``tests/fixtures/published/``: the TLC monthly report rows for HVFHS classes and the
  FHV Base Aggregate rows for UBER/LYFT, for offline verification tests.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
from pathlib import Path

import duckdb
import requests

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "tests" / "fixtures"
UA = {"User-Agent": "Mozilla/5.0 uber-rides-streaming-aws/0.1"}
MONTH = "2026-06"


def build_trips(src: Path) -> None:
    out = FIX / "tlc" / f"fhvhv_tripdata_{MONTH}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(
        f"""
        COPY (
            WITH src AS (SELECT * EXCLUDE (file_row_number), file_row_number AS rn
                         FROM read_parquet('{src.as_posix()}', file_row_number = true))
            SELECT * EXCLUDE (rn) FROM src
            WHERE (hvfhs_license_num = 'HV0003'
                   AND request_datetime >= TIMESTAMP '2026-06-12 18:00:00'
                   AND request_datetime <  TIMESTAMP '2026-06-12 18:02:00')
               OR (hvfhs_license_num IN ('HV0003', 'HV0005') AND rn % 10000 = 0)
            ORDER BY rn
        ) TO '{out.as_posix()}' (FORMAT parquet, COMPRESSION zstd)
        """
    )
    n = duckdb.sql(
        f"SELECT hvfhs_license_num, count(*) FROM '{out.as_posix()}' GROUP BY 1 ORDER BY 1"
    ).fetchall()
    print(out.name, out.stat().st_size, n)  # noqa: T201


def build_zones() -> None:
    r = requests.get(
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv", headers=UA, timeout=60
    )
    r.raise_for_status()
    (FIX / "tlc" / "taxi_zone_lookup.csv").write_bytes(r.content)


def build_weather() -> None:
    params = {
        "latitude": 40.7128,
        "longitude": -74.0060,
        "start_date": "2026-06-01",
        "end_date": "2026-06-30",
        "hourly": "temperature_2m,relative_humidity_2m,precipitation,rain,snowfall,cloud_cover,wind_speed_10m,weather_code",
        "timezone": "America/New_York",
    }
    r = requests.get("https://archive-api.open-meteo.com/v1/archive", params=params, headers=UA, timeout=60)
    r.raise_for_status()
    payload = r.json()
    payload["_request"] = params
    out = FIX / "weather" / f"open_meteo_hourly_{MONTH}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload), "utf-8")


def build_published() -> None:
    out = FIX / "published"
    out.mkdir(parents=True, exist_ok=True)
    r = requests.get(
        "https://www.nyc.gov/assets/tlc/downloads/csv/data_reports_monthly.csv", headers=UA, timeout=60
    )
    r.raise_for_status()
    rows = list(csv.reader(io.StringIO(r.content.decode("utf-8-sig"))))
    keep = [rows[0]] + [
        row for row in rows[1:] if row[1].startswith("FHV - High Volume") and row[0] >= "2025-06"
    ]
    buf = io.StringIO()
    csv.writer(buf, lineterminator="\n").writerows(keep)
    (out / "data_reports_monthly.csv").write_text(buf.getvalue(), "utf-8")
    for month in ("2026-05", MONTH):
        year, mon = month.split("-")
        q = {"$where": f"year='{year}' AND month='{int(mon)}' AND base_license_number in ('UBER','LYFT')"}
        r = requests.get(
            "https://data.cityofnewyork.us/resource/2v9c-2k7f.json", params=q, headers=UA, timeout=60
        )
        r.raise_for_status()
        (out / f"fhv_base_aggregate_{month}.json").write_text(json.dumps(r.json(), indent=1), "utf-8")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tlc", type=Path, required=True, help="path to the real fhvhv_tripdata_2026-06.parquet")
    args = ap.parse_args()
    build_trips(args.tlc.expanduser())
    build_zones()
    build_weather()
    build_published()


if __name__ == "__main__":
    main()
