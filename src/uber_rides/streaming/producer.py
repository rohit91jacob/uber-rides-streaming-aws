"""Replay producer: real TLC trips -> ride lifecycle events -> Kinesis, in event-time order.

``speedup`` compresses time: with ``speedup=60`` one hour of real events is sent in one minute;
``0`` sends as fast as possible. Records are keyed by ``trip_id`` so all events of a trip land
on the same shard, in order.

Fault-injection drills (all **simulated**, deterministic for a given ``seed``):

* ``duplicate_every=N``: every Nth event is sent twice, as a producer retry would.
* ``malformed_every=N``: every Nth event is followed by a corrupted copy (bad JSON, missing
  field, out-of-range zone, unknown event type, or a tampered event_id, chosen by a seeded RNG).
  These must end up in the dead-letter queue.
"""

from __future__ import annotations

import json
import random
import time
import zlib
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

from uber_rides.config import Settings
from uber_rides.lake.catalog import get_catalog
from uber_rides.lake.tables import TRIPS
from uber_rides.logs import get_logger
from uber_rides.streaming.aws import client
from uber_rides.streaming.events import encode, events_for_trip, sort_key

log = get_logger(__name__)

PUT_BATCH = 500
PUT_BATCH_BYTES = 4_500_000
MAX_PUT_ATTEMPTS = 8
TRIP_FIELDS = (
    "trip_id",
    "hvfhs_license_num",
    "request_datetime",
    "on_scene_datetime",
    "pickup_datetime",
    "dropoff_datetime",
    "pu_location_id",
    "do_location_id",
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
    "shared_request",
    "shared_match",
    "wav_request",
    "wav_match",
    "access_a_ride",
)
MALFORMATIONS = ("bad_json", "missing_field", "zone_out_of_range", "unknown_event_type", "tampered_id")


@dataclass
class ReplayStats:
    trips: int = 0
    events: int = 0
    sent: int = 0
    duplicates_injected: int = 0
    malformed_injected: int = 0
    put_calls: int = 0
    put_retries: int = 0
    first_event_time: str | None = None
    last_event_time: str | None = None
    elapsed_s: float = 0.0
    malformed_by_kind: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


def in_sample(trip_id: str, sample_rate: float) -> bool:
    """Deterministic sampling on a stable hash of the trip id."""
    if sample_rate >= 1:
        return True
    return zlib.crc32(trip_id.encode()) % 1_000_000 < sample_rate * 1_000_000


def load_window(settings: Settings, start: datetime, end: datetime, sample_rate: float = 1.0) -> pa.Table:
    """Trips whose ``request_datetime`` falls in [start, end) from the Iceberg ``trips`` table."""
    table = get_catalog(settings).load_table(TRIPS.identifier(settings))
    scan = table.scan(
        row_filter=And(
            GreaterThanOrEqual("request_datetime", start.isoformat()),
            LessThan("request_datetime", end.isoformat()),
        ),
        selected_fields=TRIP_FIELDS,
    )
    data = scan.to_arrow()
    if sample_rate < 1:
        keep = [in_sample(t, sample_rate) for t in data.column("trip_id").to_pylist()]
        data = data.filter(pa.array(keep))
    return data.sort_by("trip_id")


def build_events(trips: pa.Table, produced_at: str | None = None) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for trip in trips.to_pylist():
        events.extend(events_for_trip(trip, produced_at))
    events.sort(key=sort_key)
    return events


def _malform(event: dict[str, Any], kind: str) -> bytes:
    bad = json.loads(json.dumps(event))
    if kind == "bad_json":
        return encode(event)[:-7] + b"#trunc"
    if kind == "missing_field":
        bad.pop("trip_id")
    elif kind == "zone_out_of_range":
        bad["zone_id"] = 999
    elif kind == "unknown_event_type":
        bad["event_type"] = "teleported"
    elif kind == "tampered_id":
        bad["event_id"] = "0" * 32
    return encode(bad)


def with_drills(
    events: Iterable[dict[str, Any]],
    *,
    duplicate_every: int = 0,
    malformed_every: int = 0,
    seed: int = 7,
    stats: ReplayStats | None = None,
) -> Iterator[tuple[dict[str, Any], bytes]]:
    """Yield (event, payload) pairs, injecting simulated duplicates and malformed copies."""
    rng = random.Random(seed)
    for i, ev in enumerate(events, start=1):
        payload = encode(ev)
        yield ev, payload
        if duplicate_every and i % duplicate_every == 0:
            if stats:
                stats.duplicates_injected += 1
            yield ev, payload
        if malformed_every and i % malformed_every == 0:
            kind = rng.choice(MALFORMATIONS)
            if stats:
                stats.malformed_injected += 1
                stats.malformed_by_kind[kind] = stats.malformed_by_kind.get(kind, 0) + 1
            yield ev, _malform(ev, kind)


class KinesisSender:
    def __init__(self, settings: Settings, stats: ReplayStats) -> None:
        self.kinesis = client("kinesis", settings)
        self.stream = settings.kinesis_stream
        self.stats = stats
        self.buffer: list[dict[str, Any]] = []
        self.buffer_bytes = 0

    def add(self, partition_key: str, payload: bytes) -> None:
        self.buffer.append({"Data": payload, "PartitionKey": partition_key})
        self.buffer_bytes += len(payload) + len(partition_key)
        if len(self.buffer) >= PUT_BATCH or self.buffer_bytes >= PUT_BATCH_BYTES:
            self.flush()

    def flush(self) -> None:
        records, self.buffer, self.buffer_bytes = self.buffer, [], 0
        attempt = 0
        while records:
            resp = self.kinesis.put_records(StreamName=self.stream, Records=records)
            self.stats.put_calls += 1
            failed = [r for r, res in zip(records, resp["Records"], strict=True) if "ErrorCode" in res]
            self.stats.sent += len(records) - len(failed)
            if not failed:
                return
            attempt += 1
            if attempt >= MAX_PUT_ATTEMPTS:
                raise RuntimeError(f"{len(failed)} records still failing after {attempt} PutRecords attempts")
            self.stats.put_retries += len(failed)
            time.sleep(min(5.0, 0.1 * 2**attempt))
            records = failed


def replay(
    settings: Settings,
    events: list[dict[str, Any]],
    *,
    speedup: float | None = None,
    duplicate_every: int = 0,
    malformed_every: int = 0,
    seed: int = 7,
    trips: int = 0,
) -> ReplayStats:
    speedup = settings.replay_speedup if speedup is None else speedup
    stats = ReplayStats(trips=trips, events=len(events))
    if events:
        stats.first_event_time = events[0]["event_time"]
        stats.last_event_time = events[-1]["event_time"]
    sender = KinesisSender(settings, stats)
    t0_wall = time.monotonic()
    t0_event = datetime.fromisoformat(events[0]["event_time"]) if events else None
    for ev, payload in with_drills(
        events, duplicate_every=duplicate_every, malformed_every=malformed_every, seed=seed, stats=stats
    ):
        if speedup and t0_event is not None:
            due = (datetime.fromisoformat(ev["event_time"]) - t0_event).total_seconds() / speedup
            wait = due - (time.monotonic() - t0_wall)
            if wait > 0:
                sender.flush()  # do not hold events back while sleeping
                time.sleep(wait)
        sender.add(ev["trip_id"], payload)
    sender.flush()
    stats.elapsed_s = round(time.monotonic() - t0_wall, 3)
    log.info("replay finished", extra=stats.to_dict())
    return stats


def produced_at_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")
