"""Iceberg table definitions: schema, partition spec and properties for every lake table.

All fields are optional (nullable) at the Iceberg level; non-null rules are enforced by the
data-quality gates instead, so a bad batch is reported rather than half-written.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC, PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IcebergType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

from uber_rides.config import Settings

TABLE_PROPERTIES = {
    "format-version": "2",
    "write.parquet.compression-codec": "zstd",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "20",
}


@dataclass(frozen=True)
class TableDef:
    name: str
    namespace: str
    columns: tuple[tuple[str, IcebergType, str], ...]
    partition: tuple[tuple[str, str], ...] = ()  # (source column, "day" | "identity")
    description: str = ""
    properties: dict[str, str] = field(default_factory=dict)

    @property
    def schema(self) -> Schema:
        return Schema(
            *(
                NestedField(i, name, typ, required=False, doc=doc)
                for i, (name, typ, doc) in enumerate(self.columns, start=1)
            )
        )

    @property
    def arrow_schema(self) -> pa.Schema:
        return self.schema.as_arrow()

    @property
    def column_names(self) -> list[str]:
        return [c[0] for c in self.columns]

    def partition_spec(self) -> PartitionSpec:
        if not self.partition:
            return UNPARTITIONED_PARTITION_SPEC
        schema = self.schema
        fields = []
        for i, (col, transform) in enumerate(self.partition, start=1000):
            source_id = schema.find_field(col).field_id
            if transform == "day":
                fields.append(PartitionField(source_id, i, DayTransform(), f"{col}_day"))
            else:
                fields.append(PartitionField(source_id, i, IdentityTransform(), col))
        return PartitionSpec(*fields)

    def identifier(self, settings: Settings) -> str:
        ns = settings.namespace if self.namespace == "lake" else f"{settings.namespace}_{self.namespace}"
        return f"{ns}.{self.name}"


S, TS, D, INT, L, B = StringType(), TimestampType(), DoubleType(), IntegerType(), LongType(), BooleanType()

_TRIP_COLUMNS: tuple[tuple[str, IcebergType, str], ...] = (
    ("trip_id", S, "Deterministic id: <license>-<YYYY-MM>-<row number in the TLC file>"),
    ("source_month", S, "TLC file month (YYYY-MM); the idempotency key of a load"),
    ("hvfhs_license_num", S, "HVFHS licence: HV0003 = Uber"),
    ("dispatching_base_num", S, "TLC base that dispatched the trip"),
    ("originating_base_num", S, "TLC base that received the request"),
    ("request_datetime", TS, "Rider request time (NYC local); 5-min marks suggest reservations"),
    ("on_scene_datetime", TS, "Driver arrival at pickup (NYC local)"),
    ("pickup_datetime", TS, "Trip start (NYC local)"),
    ("dropoff_datetime", TS, "Trip end (NYC local)"),
    ("pu_location_id", INT, "TLC taxi zone of pickup (1-263; 264/265 unknown/outside NYC)"),
    ("do_location_id", INT, "TLC taxi zone of dropoff"),
    ("trip_miles", D, "Passenger trip miles"),
    ("trip_time_s", L, "Passenger trip seconds"),
    ("base_passenger_fare", D, "Base fare before tolls, tips, taxes and fees (USD)"),
    ("tolls", D, "Tolls (USD)"),
    ("bcf", D, "Black Car Fund (USD)"),
    ("sales_tax", D, "NYS sales tax (USD)"),
    ("congestion_surcharge", D, "NYS congestion surcharge (USD)"),
    ("airport_fee", D, "Airport fee (USD)"),
    ("tips", D, "Tips (USD)"),
    ("driver_pay", D, "Driver pay excluding tolls and tips (USD)"),
    ("cbd_congestion_fee", D, "MTA Congestion Relief Zone toll (USD), since 2025-01-05"),
    ("shared_request", B, "Rider agreed to a shared ride"),
    ("shared_match", B, "Rider actually shared the vehicle"),
    ("access_a_ride", B, "Trip administered for the MTA Access-A-Ride programme"),
    ("wav_request", B, "Wheelchair-accessible vehicle requested"),
    ("wav_match", B, "Trip took place in a wheelchair-accessible vehicle"),
    ("is_reservation_like", B, "request_datetime later than on_scene_datetime (scheduled ride)"),
    ("is_unknown_zone", B, "Pickup or dropoff zone is 264/265"),
    ("is_non_positive_fare", B, "base_passenger_fare <= 0"),
    ("loaded_at", TS, "UTC load time"),
)

TRIPS = TableDef(
    name="trips",
    namespace="lake",
    columns=_TRIP_COLUMNS,
    partition=(("pickup_datetime", "day"),),
    description="Cleaned Uber (HV0003) trips, one row per TLC trip record.",
)

TRIPS_QUARANTINE = TableDef(
    name="trips_quarantine",
    namespace="lake",
    columns=(*_TRIP_COLUMNS, ("reject_reason", S, "Which hard validity rule failed")),
    description="Uber trips rejected by hard validity rules; kept for audit.",
)

TAXI_ZONES = TableDef(
    name="taxi_zones",
    namespace="lake",
    columns=(
        ("location_id", INT, "TLC taxi zone id"),
        ("borough", S, "Borough (EWR, Unknown, N/A for special ids)"),
        ("zone", S, "Zone name"),
        ("service_zone", S, "Yellow Zone, Boro Zone, Airports, EWR"),
    ),
    description="TLC taxi zone lookup.",
)

WEATHER_HOURLY = TableDef(
    name="weather_hourly",
    namespace="lake",
    columns=(
        ("observed_hour", TS, "Hour start, NYC local time"),
        ("source_month", S, "Month the hour was fetched for (YYYY-MM)"),
        ("temperature_2m", D, "Air temperature at 2 m (deg C)"),
        ("relative_humidity_2m", D, "Relative humidity at 2 m (%)"),
        ("precipitation", D, "Total precipitation in the preceding hour (mm)"),
        ("rain", D, "Rain in the preceding hour (mm)"),
        ("snowfall", D, "Snowfall in the preceding hour (cm)"),
        ("cloud_cover", D, "Total cloud cover (%)"),
        ("wind_speed_10m", D, "Wind speed at 10 m (km/h)"),
        ("weather_code", INT, "WMO weather code"),
    ),
    description="Open-Meteo hourly reanalysis for central NYC.",
)

RIDE_EVENTS = TableDef(
    name="ride_events",
    namespace="lake",
    columns=(
        ("event_id", S, "sha256(trip_id|event_type)[:32]; the dedupe key"),
        ("event_type", S, "requested | driver_on_scene | picked_up | dropped_off"),
        ("event_time", TS, "Event time (NYC local), from the TLC timestamps"),
        ("trip_id", S, "Trip the event belongs to"),
        ("zone_id", INT, "Zone the event happened in (pickup zone, dropoff zone for dropped_off)"),
        ("pu_location_id", INT, "Pickup zone"),
        ("do_location_id", INT, "Dropoff zone"),
        ("schema_version", INT, "Envelope schema version"),
        ("trip_miles", D, "dropped_off only"),
        ("trip_time_s", L, "dropped_off only"),
        ("base_passenger_fare", D, "dropped_off only"),
        ("driver_pay", D, "dropped_off only"),
        ("tips", D, "dropped_off only"),
        ("cbd_congestion_fee", D, "dropped_off only"),
        ("airport_fee", D, "dropped_off only"),
        ("shared_request", B, "requested only"),
        ("shared_match", B, "dropped_off only"),
        ("produced_at", TS, "Producer wall-clock time (UTC)"),
        ("shard_id", S, "Kinesis shard"),
        ("sequence_number", S, "Kinesis sequence number"),
        ("landing_object", S, "Landing object key the event was loaded from (idempotency key)"),
        ("loaded_at", TS, "UTC load time"),
    ),
    partition=(("event_time", "day"),),
    description="Accepted ride lifecycle events from the streaming path (at-least-once landing).",
)

ZONE_WINDOW_COUNTERS = TableDef(
    name="zone_window_counters",
    namespace="lake",
    columns=(
        ("zone_id", INT, "TLC taxi zone"),
        ("window_start", TS, "Window start (NYC local)"),
        ("window_minutes", INT, "Window length"),
        ("requested", L, "requested events"),
        ("driver_on_scene", L, "driver_on_scene events"),
        ("picked_up", L, "picked_up events"),
        ("dropped_off", L, "dropped_off events (drivers freed here: the supply proxy)"),
        ("surge_proxy", D, "requested / max(dropped_off, 1)"),
        ("snapshot_at", TS, "UTC time the DynamoDB table was exported"),
    ),
    description="Snapshot of the real-time DynamoDB zone supply/demand counters.",
)

DQ_RESULTS = TableDef(
    name="dq_results",
    namespace="ops",
    columns=(
        ("run_id", S, "Gate run id"),
        ("checked_at", TS, "UTC"),
        ("layer", S, "landing | lake | stream | marts"),
        ("check_name", S, "Check id"),
        ("severity", S, "error | warn"),
        ("status", S, "pass | fail"),
        ("failing_rows", L, "Rows violating the rule"),
        ("total_rows", L, "Rows checked"),
        ("threshold", D, "Allowed failing fraction"),
        ("detail", S, "Human-readable explanation"),
    ),
    description="Data-quality gate results.",
)

STREAM_DLQ = TableDef(
    name="stream_dlq",
    namespace="ops",
    columns=(
        ("message_id", S, "SQS message id"),
        ("reason", S, "Why the record was rejected"),
        ("error", S, "Validation error detail"),
        ("shard_id", S, "Kinesis shard"),
        ("sequence_number", S, "Kinesis sequence number"),
        ("raw", S, "Original record payload (truncated to 4 KB)"),
        ("received_at", TS, "UTC"),
    ),
    description="Records the consumer sent to the SQS dead-letter queue.",
)

ALL_TABLES = (
    TRIPS,
    TRIPS_QUARANTINE,
    TAXI_ZONES,
    WEATHER_HOURLY,
    RIDE_EVENTS,
    ZONE_WINDOW_COUNTERS,
    DQ_RESULTS,
    STREAM_DLQ,
)


def ensure_table(catalog: Catalog, settings: Settings, table: TableDef) -> Table:
    ident = table.identifier(settings)
    namespace = ident.rsplit(".", 1)[0]
    catalog.create_namespace_if_not_exists(namespace)
    try:
        return catalog.load_table(ident)
    except NoSuchTableError:
        return catalog.create_table(
            ident,
            schema=table.schema,
            partition_spec=table.partition_spec(),
            properties={**TABLE_PROPERTIES, "comment": table.description, **table.properties},
        )


def to_arrow(table: TableDef, data: pa.Table) -> pa.Table:
    """Select + cast ``data`` to the table's Arrow schema (missing columns become nulls)."""
    target = table.arrow_schema
    arrays = []
    for f in target:
        if f.name in data.column_names:
            arrays.append(data.column(f.name).cast(f.type))
        else:
            arrays.append(pa.nulls(data.num_rows, f.type))
    return pa.Table.from_arrays(arrays, schema=target)
