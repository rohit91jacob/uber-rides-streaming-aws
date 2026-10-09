"""Ride lifecycle events derived from TLC trip records, and their validation.

Each completed trip becomes four events, timestamped with the real TLC times:

    requested (request_datetime, pickup zone)
    driver_on_scene (on_scene_datetime, pickup zone)
    picked_up (pickup_datetime, pickup zone)
    dropped_off (dropoff_datetime, dropoff zone, with distance, fare and pay)

The *events* are therefore real; what is simulated is only their *delivery* as a live feed
(the replay producer). Imports are stdlib-only so this module ships inside the Lambda zip.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any

SCHEMA_VERSION = 1
EVENT_TYPES = ("requested", "driver_on_scene", "picked_up", "dropped_off")
EVENT_RANK = {t: i for i, t in enumerate(EVENT_TYPES)}
TIMESTAMP_COLUMN = {
    "requested": "request_datetime",
    "driver_on_scene": "on_scene_datetime",
    "picked_up": "pickup_datetime",
    "dropped_off": "dropoff_datetime",
}
MIN_ZONE, MAX_ZONE = 1, 265
DROPOFF_NUMERIC = (
    "trip_miles",
    "trip_time_s",
    "base_passenger_fare",
    "tolls",
    "bcf",
    "sales_tax",
    "congestion_surcharge",
    "airport_fee",
    "tips",
    "driver_pay",
    "cbd_congestion_fee",
)
MAX_RAW_BYTES = 4096


def event_id(trip_id: str, event_type: str) -> str:
    return hashlib.sha256(f"{trip_id}|{event_type}".encode()).hexdigest()[:32]


def _iso(ts: datetime) -> str:
    return ts.isoformat(timespec="seconds")


def events_for_trip(trip: dict[str, Any], produced_at: str | None = None) -> list[dict[str, Any]]:
    """The lifecycle events of one trip (a row of the ``trips`` table), in lifecycle order.

    A missing timestamp (``on_scene_datetime`` can be null in older months) skips that event.
    """
    out = []
    for etype in EVENT_TYPES:
        ts = trip.get(TIMESTAMP_COLUMN[etype])
        if ts is None:
            continue
        zone = trip["do_location_id"] if etype == "dropped_off" else trip["pu_location_id"]
        attributes: dict[str, Any] = {}
        if etype == "requested":
            attributes = {
                "shared_request": bool(trip.get("shared_request")),
                "wav_request": bool(trip.get("wav_request")),
                "access_a_ride": bool(trip.get("access_a_ride")),
            }
        elif etype == "dropped_off":
            attributes = {k: trip.get(k) for k in DROPOFF_NUMERIC}
            attributes["shared_match"] = bool(trip.get("shared_match"))
            attributes["wav_match"] = bool(trip.get("wav_match"))
        out.append(
            {
                "schema_version": SCHEMA_VERSION,
                "event_id": event_id(trip["trip_id"], etype),
                "event_type": etype,
                "event_time": _iso(ts),
                "trip_id": trip["trip_id"],
                "license": trip.get("hvfhs_license_num"),
                "zone_id": int(zone),
                "pu_location_id": int(trip["pu_location_id"]),
                "do_location_id": int(trip["do_location_id"]),
                "produced_at": produced_at,
                "attributes": attributes,
            }
        )
    return out


def sort_key(event: dict[str, Any]) -> tuple[str, str, int]:
    """Event-time order; ties broken deterministically by trip then lifecycle rank."""
    return (event["event_time"], event["trip_id"], EVENT_RANK[event["event_type"]])


def encode(event: dict[str, Any]) -> bytes:
    return json.dumps(event, separators=(",", ":"), sort_keys=True).encode()


# --------------------------------------------------------------------------- validation
@dataclass(frozen=True)
class Invalid:
    reason: str
    error: str


def _is_number(v: Any) -> bool:
    return isinstance(v, int | float) and not isinstance(v, bool) and math.isfinite(v)


def validate(raw: bytes) -> dict[str, Any] | Invalid:
    """Parse and validate one record. Returns the event dict or an :class:`Invalid`."""
    try:
        event = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        return Invalid("malformed_json", str(exc)[:200])
    if not isinstance(event, dict):
        return Invalid("malformed_json", "payload is not a JSON object")
    if event.get("schema_version") != SCHEMA_VERSION:
        return Invalid("unsupported_schema_version", f"schema_version={event.get('schema_version')!r}")
    missing = [
        k
        for k in (
            "event_id",
            "event_type",
            "event_time",
            "trip_id",
            "zone_id",
            "pu_location_id",
            "do_location_id",
        )
        if event.get(k) is None
    ]
    if missing:
        return Invalid("schema_violation", f"missing fields {missing}")
    etype = event["event_type"]
    if etype not in EVENT_RANK:
        return Invalid("schema_violation", f"unknown event_type {etype!r}")
    if not isinstance(event["trip_id"], str) or not event["trip_id"]:
        return Invalid("schema_violation", "trip_id must be a non-empty string")
    try:
        datetime.fromisoformat(event["event_time"])
    except (TypeError, ValueError):
        return Invalid("schema_violation", f"event_time {event['event_time']!r} is not ISO-8601")
    for k in ("zone_id", "pu_location_id", "do_location_id"):
        v = event[k]
        if not isinstance(v, int) or isinstance(v, bool):
            return Invalid("schema_violation", f"{k} must be an integer")
        if not MIN_ZONE <= v <= MAX_ZONE:
            return Invalid("zone_out_of_range", f"{k}={v}")
    if event["event_id"] != event_id(event["trip_id"], etype):
        return Invalid("event_id_mismatch", "event_id is not sha256(trip_id|event_type)[:32]")
    attributes = event.get("attributes") or {}
    if not isinstance(attributes, dict):
        return Invalid("schema_violation", "attributes must be an object")
    if etype == "dropped_off":
        bad = [k for k in DROPOFF_NUMERIC if attributes.get(k) is not None and not _is_number(attributes[k])]
        if bad:
            return Invalid("schema_violation", f"non-numeric dropoff fields {bad}")
        for k in ("trip_miles", "trip_time_s"):
            if attributes.get(k) is None or attributes[k] < 0:
                return Invalid("schema_violation", f"{k} must be >= 0")
    return event


def truncate_raw(raw: bytes) -> str:
    return raw[:MAX_RAW_BYTES].decode("utf-8", errors="replace")


def window_start(event_time: str, window_minutes: int) -> str:
    ts = datetime.fromisoformat(event_time)
    minute = ts.minute - ts.minute % window_minutes
    return _iso(ts.replace(minute=minute, second=0, microsecond=0))
