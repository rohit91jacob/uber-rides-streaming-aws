"""Landing zone (gzip JSON lines written by the Lambda) -> Iceberg ``ride_events``.

Incremental and idempotent: every row records the landing object it came from, objects that
are already in the table are skipped, and ``full_refresh`` rebuilds the table from every object.
The landing zone is at-least-once, so the table can hold the same ``event_id`` more than once
(e.g. a replay run twice); dbt staging deduplicates on ``event_id``.
"""

from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc

from uber_rides.config import Settings
from uber_rides.lake.catalog import get_catalog
from uber_rides.lake.tables import RIDE_EVENTS, ensure_table, to_arrow
from uber_rides.logs import get_logger

log = get_logger(__name__)

ATTRIBUTE_FIELDS = (
    "trip_miles",
    "trip_time_s",
    "base_passenger_fare",
    "driver_pay",
    "tips",
    "cbd_congestion_fee",
    "airport_fee",
    "shared_request",
    "shared_match",
)


def landing_objects(root: Path) -> list[Path]:
    return sorted(root.rglob("*.jsonl.gz"))


def object_key(root: Path, path: Path) -> str:
    return path.relative_to(root).as_posix()


def _rows(root: Path, path: Path, loaded_at: datetime) -> list[dict]:
    key = object_key(root, path)
    rows = []
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            ev = json.loads(line)
            attrs = ev.get("attributes") or {}
            kin = ev.get("_kinesis") or {}
            produced = ev.get("produced_at")
            rows.append(
                {
                    "event_id": ev["event_id"],
                    "event_type": ev["event_type"],
                    "event_time": datetime.fromisoformat(ev["event_time"]),
                    "trip_id": ev["trip_id"],
                    "zone_id": ev["zone_id"],
                    "pu_location_id": ev["pu_location_id"],
                    "do_location_id": ev["do_location_id"],
                    "schema_version": ev["schema_version"],
                    **{k: attrs.get(k) for k in ATTRIBUTE_FIELDS},
                    "produced_at": (
                        datetime.fromisoformat(produced).astimezone(UTC).replace(tzinfo=None)
                        if produced
                        else None
                    ),
                    "shard_id": kin.get("shard_id"),
                    "sequence_number": kin.get("sequence_number"),
                    "landing_object": key,
                    "loaded_at": loaded_at,
                }
            )
    return rows


def sync_landing_from_s3(settings: Settings) -> int:
    """On AWS the consumer lands objects in S3: mirror the ones not yet present locally."""
    from uber_rides.streaming.aws import client

    s3 = client("s3", settings)
    target = settings.events_landing_dir / "s3"
    fetched = 0
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=settings.landing_bucket, Prefix=f"{settings.landing_prefix}/"
    ):
        for obj in page.get("Contents", []):
            dest = target / obj["Key"]
            if dest.exists():
                continue
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(s3.get_object(Bucket=settings.landing_bucket, Key=obj["Key"])["Body"].read())
            fetched += 1
    return fetched


def load_events(settings: Settings, *, full_refresh: bool = False, root: Path | None = None) -> dict:
    root = root or settings.events_landing_dir
    table = ensure_table(get_catalog(settings), settings, RIDE_EVENTS)
    objects = landing_objects(root) if root.exists() else []
    if full_refresh:
        loaded: set[str] = set()
    else:
        existing = table.scan(selected_fields=("landing_object",)).to_arrow()
        loaded = set(pc.unique(existing.column("landing_object")).to_pylist()) if existing.num_rows else set()
    new = [p for p in objects if object_key(root, p) not in loaded]
    loaded_at = datetime.now(UTC).replace(tzinfo=None, microsecond=0)
    rows: list[dict] = []
    for p in new:
        rows.extend(_rows(root, p, loaded_at))
    data = to_arrow(RIDE_EVENTS, pa.Table.from_pylist(rows)) if rows else None
    if full_refresh:
        table.overwrite(data if data is not None else RIDE_EVENTS.arrow_schema.empty_table())
    elif data is not None:
        table.append(data)
    log.info("events loaded", extra={"objects": len(new), "rows": len(rows)})
    return {
        "landing_objects_total": len(objects),
        "landing_objects_loaded": len(new),
        "rows_loaded": len(rows),
        "full_refresh": full_refresh,
    }
