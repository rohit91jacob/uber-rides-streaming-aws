"""Batch loads into Iceberg: TLC month -> ``trips`` (+ ``trips_quarantine``), zones, weather.

Each load is idempotent. A month is replaced inside one Iceberg transaction (delete
``source_month = M`` then append), so a rerun produces exactly the same rows and readers never
see a half-loaded month.
"""

from __future__ import annotations

import warnings
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.expressions import EqualTo

from uber_rides.config import Settings
from uber_rides.lake.catalog import get_catalog
from uber_rides.lake.tables import TAXI_ZONES, TRIPS, TRIPS_QUARANTINE, WEATHER_HOURLY, ensure_table, to_arrow
from uber_rides.logs import get_logger
from uber_rides.sources import tlc, weather

log = get_logger(__name__)

BATCH_ROWS = 1_000_000
MAX_ZONE_ID = 265

# Hard validity rules: a trip failing any of these cannot be placed in time or space and goes
# to the quarantine table. Everything else (zero fares, unknown zones, reservations) is kept and
# flagged, because those are real trips.
REJECT_RULES: tuple[tuple[str, str], ...] = (
    ("missing_timestamp", "request_datetime IS NULL OR pickup_datetime IS NULL OR dropoff_datetime IS NULL"),
    ("dropoff_before_pickup", "dropoff_datetime < pickup_datetime"),
    ("pickup_outside_file_month", "pickup_datetime < $lo OR pickup_datetime >= $hi"),
    ("missing_zone", "PULocationID IS NULL OR DOLocationID IS NULL"),
    (
        "zone_out_of_range",
        f"PULocationID NOT BETWEEN 1 AND {MAX_ZONE_ID} OR DOLocationID NOT BETWEEN 1 AND {MAX_ZONE_ID}",
    ),
    (
        "negative_distance_or_duration",
        "trip_miles IS NULL OR trip_time IS NULL OR trip_miles < 0 OR trip_time < 0",
    ),
    ("missing_fare_or_pay", "base_passenger_fare IS NULL OR driver_pay IS NULL"),
)


def _reject_case() -> str:
    whens = "\n".join(f"WHEN {cond} THEN '{name}'" for name, cond in REJECT_RULES)
    return f"CASE {whens} ELSE NULL END"


def _trip_select() -> str:
    flag = "coalesce({c} = 'Y', false)"
    return f"""
        printf('%s-%s-%09d', hvfhs_license_num, $month, file_row_number) AS trip_id,
        $month AS source_month,
        hvfhs_license_num, dispatching_base_num, originating_base_num,
        request_datetime, on_scene_datetime, pickup_datetime, dropoff_datetime,
        PULocationID AS pu_location_id, DOLocationID AS do_location_id,
        trip_miles, CAST(trip_time AS BIGINT) AS trip_time_s,
        base_passenger_fare, tolls, bcf, sales_tax, congestion_surcharge, airport_fee, tips,
        driver_pay, cbd_congestion_fee,
        {flag.format(c="shared_request_flag")} AS shared_request,
        {flag.format(c="shared_match_flag")} AS shared_match,
        {flag.format(c="access_a_ride_flag")} AS access_a_ride,
        {flag.format(c="wav_request_flag")} AS wav_request,
        {flag.format(c="wav_match_flag")} AS wav_match,
        coalesce(request_datetime > on_scene_datetime, false) AS is_reservation_like,
        (PULocationID > 263 OR DOLocationID > 263) AS is_unknown_zone,
        coalesce(base_passenger_fare <= 0, false) AS is_non_positive_fare,
        $loaded_at::TIMESTAMP AS loaded_at
    """


def staging_paths(settings: Settings, month: str) -> tuple[Path, Path]:
    base = settings.data_dir / "staging" / "trips"
    return base / f"trips_{month}.parquet", base / f"quarantine_{month}.parquet"


def stage_month(settings: Settings, month: str) -> dict:
    """Filter the landed TLC file to the licensee, derive ids and flags, split clean/rejected."""
    src = tlc.landed_trip_path(settings, month)
    if not src.exists():
        raise FileNotFoundError(f"{src} not landed; run `uber-rides ingest --month {month}` first")
    clean_path, reject_path = staging_paths(settings, month)
    clean_path.parent.mkdir(parents=True, exist_ok=True)
    start, end = tlc.month_bounds(month)
    params = {
        "path": str(src),
        "lic": settings.license_num,
        "month": month,
        # one day of slack either side: trips spanning midnight at the month boundary
        "lo": datetime.combine(start - timedelta(days=1), datetime.min.time()),
        "hi": datetime.combine(end + timedelta(days=1), datetime.min.time()),
        "loaded_at": datetime.now(UTC).replace(tzinfo=None, microsecond=0),
    }
    con = duckdb.connect()
    try:
        con.execute(f"SET memory_limit = '{settings.duckdb_memory_limit}'")
        con.execute("SET preserve_insertion_order = false")
        base = f"""
            SELECT {_trip_select()}, {_reject_case()} AS reject_reason
            FROM read_parquet($path, file_row_number = true)
            WHERE hvfhs_license_num = $lic
        """
        cols = ", ".join(TRIPS.column_names)
        con.execute(
            f"COPY (SELECT {cols} FROM ({base}) WHERE reject_reason IS NULL ORDER BY pickup_datetime, trip_id) "
            f"TO '{clean_path.as_posix()}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 250000)",
            params,
        )
        con.execute(
            f"COPY (SELECT {cols}, reject_reason FROM ({base}) WHERE reject_reason IS NOT NULL ORDER BY trip_id) "
            f"TO '{reject_path.as_posix()}' (FORMAT parquet, COMPRESSION zstd)",
            params,
        )
        clean = con.execute("SELECT count(*) FROM read_parquet(?)", [str(clean_path)]).fetchone()[0]
        rejected = dict(
            con.execute(
                "SELECT reject_reason, count(*) FROM read_parquet(?) GROUP BY 1 ORDER BY 1",
                [str(reject_path)],
            ).fetchall()
        )
    finally:
        con.close()
    log.info("staged month", extra={"month": month, "clean": clean, "rejected": sum(rejected.values())})
    return {
        "month": month,
        "clean_rows": clean,
        "rejected_rows": sum(rejected.values()),
        "rejected_by_rule": rejected,
    }


def _replace_month(table_def, settings: Settings, month: str, parquet_path: Path) -> int:
    catalog = get_catalog(settings)
    table = ensure_table(catalog, settings, table_def)
    pf = pq.ParquetFile(parquet_path)
    written = 0
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Delete operation did not match any records")
        with table.transaction() as tx:
            tx.delete(EqualTo("source_month", month))
            for batch in pf.iter_batches(batch_size=BATCH_ROWS):
                data = to_arrow(table_def, pa.Table.from_batches([batch]))
                tx.append(data)
                written += data.num_rows
    return written


def load_trips(settings: Settings, month: str) -> dict:
    """Stage then replace ``source_month = month`` in ``trips`` and ``trips_quarantine``."""
    staged = stage_month(settings, month)
    clean_path, reject_path = staging_paths(settings, month)
    clean = _replace_month(TRIPS, settings, month, clean_path)
    rejected = _replace_month(TRIPS_QUARANTINE, settings, month, reject_path)
    log.info("loaded trips", extra={"month": month, "rows": clean, "quarantined": rejected})
    return {**staged, "trips_written": clean, "quarantine_written": rejected}


def load_zones(settings: Settings) -> dict:
    path = tlc.landed_zones_path(settings)
    con = duckdb.connect()
    try:
        data = con.execute(
            """SELECT CAST("LocationID" AS INTEGER) AS location_id, "Borough" AS borough,
                      "Zone" AS zone, service_zone
               FROM read_csv(?, header = true, all_varchar = true) ORDER BY 1""",
            [str(path)],
        ).to_arrow_table()
    finally:
        con.close()
    table = ensure_table(get_catalog(settings), settings, TAXI_ZONES)
    table.overwrite(to_arrow(TAXI_ZONES, data))
    return {"zones": data.num_rows}


def load_weather(settings: Settings, month: str) -> dict:
    rows = weather.hourly_rows(weather.weather_file(settings, month))
    for r in rows:
        r["source_month"] = month
    data = to_arrow(WEATHER_HOURLY, pa.Table.from_pylist(rows))
    table = ensure_table(get_catalog(settings), settings, WEATHER_HOURLY)
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Delete operation did not match any records")
        table.overwrite(data, overwrite_filter=EqualTo("source_month", month))
    return {"weather_hours": data.num_rows}
