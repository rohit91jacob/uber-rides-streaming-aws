# Data dictionary

All timestamps from the TLC are **New York local wall-clock time** (no zone in the source).
`loaded_at`, `produced_at`, `checked_at` and `snapshot_at` are UTC. Money is USD.

## Raw source: TLC HVFHV trip record (25 columns)

The landed file is kept byte-for-byte (`<data>/landing/tlc/fhvhv_tripdata_YYYY-MM.parquet`).
Its schema is checked on every ingest (`sources/tlc.py: EXPECTED_COLUMNS`); the TLC's own
dictionary is at <https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_hvfhs.pdf>.
Only `hvfhs_license_num = 'HV0003'` (Uber) rows enter the lake; the per-licensee profile of the
raw file (all licensees) is kept in `<data>/ops/source_profile/YYYY-MM.json` for verification.

## Ride lifecycle event (Kinesis record, JSON)

One completed trip becomes four events, each carrying the trip's *real* TLC timestamp:

| Field | Type | Notes |
|---|---|---|
| `schema_version` | int | `1` |
| `event_id` | string | `sha256(trip_id + "|" + event_type)[:32]`; the idempotency key |
| `event_type` | string | `requested` (request_datetime), `driver_on_scene` (on_scene_datetime), `picked_up` (pickup_datetime), `dropped_off` (dropoff_datetime) |
| `event_time` | ISO-8601 | NYC local |
| `trip_id` | string | Kinesis partition key, so a trip's events stay ordered on one shard |
| `zone_id` | int | pickup zone; dropoff zone for `dropped_off` |
| `pu_location_id`, `do_location_id` | int | 1-265 |
| `license` | string | `HV0003` |
| `produced_at` | ISO-8601 UTC | producer wall clock (**simulated delivery time**) |
| `attributes` | object | `requested`: shared/WAV/Access-A-Ride request flags. `dropped_off`: miles, seconds, fare components, tips, driver pay, CBD fee, shared/WAV match |

Rejected records go to SQS with `reason` in `malformed_json`, `unsupported_schema_version`,
`schema_violation`, `zone_out_of_range`, `event_id_mismatch` (consumer) or
`batch_retries_exhausted` (event source on-failure).

## DynamoDB tables (real-time state)

| Table | Key | Attributes |
|---|---|---|
| `uber-ride-event-dedupe` | `event_id` (S) | `trip_id`, `event_type`, `shard_id`, `sequence_number`, `first_seen_at`, `expires_at` (TTL, 7 days) |
| `uber-zone-supply-demand` | `pk` = `zone#<id>` (S), `sk` = window start ISO (S) | `requested`, `driver_on_scene`, `picked_up`, `dropped_off` (N, atomic `ADD`), `expires_at` (TTL, 35 days) |

## Iceberg tables

Format v2, zstd Parquet. Locally: SQLite SQL catalog + filesystem warehouse; on AWS: Glue
Data Catalog + `s3://<lake-bucket>/warehouse`.

### `uber_lake.trips`

Cleaned Uber (HV0003) trips, one row per TLC trip record. Partitioned by: pickup_datetime (day).

| Column | Type | Description |
|---|---|---|
| `trip_id` | string | Deterministic id: <license>-<YYYY-MM>-<row number in the TLC file> |
| `source_month` | string | TLC file month (YYYY-MM); the idempotency key of a load |
| `hvfhs_license_num` | string | HVFHS licence: HV0003 = Uber |
| `dispatching_base_num` | string | TLC base that dispatched the trip |
| `originating_base_num` | string | TLC base that received the request |
| `request_datetime` | timestamp | Rider request time (NYC local); 5-min marks suggest reservations |
| `on_scene_datetime` | timestamp | Driver arrival at pickup (NYC local) |
| `pickup_datetime` | timestamp | Trip start (NYC local) |
| `dropoff_datetime` | timestamp | Trip end (NYC local) |
| `pu_location_id` | int | TLC taxi zone of pickup (1-263; 264/265 unknown/outside NYC) |
| `do_location_id` | int | TLC taxi zone of dropoff |
| `trip_miles` | double | Passenger trip miles |
| `trip_time_s` | long | Passenger trip seconds |
| `base_passenger_fare` | double | Base fare before tolls, tips, taxes and fees (USD) |
| `tolls` | double | Tolls (USD) |
| `bcf` | double | Black Car Fund (USD) |
| `sales_tax` | double | NYS sales tax (USD) |
| `congestion_surcharge` | double | NYS congestion surcharge (USD) |
| `airport_fee` | double | Airport fee (USD) |
| `tips` | double | Tips (USD) |
| `driver_pay` | double | Driver pay excluding tolls and tips (USD) |
| `cbd_congestion_fee` | double | MTA Congestion Relief Zone toll (USD), since 2025-01-05 |
| `shared_request` | boolean | Rider agreed to a shared ride |
| `shared_match` | boolean | Rider actually shared the vehicle |
| `access_a_ride` | boolean | Trip administered for the MTA Access-A-Ride programme |
| `wav_request` | boolean | Wheelchair-accessible vehicle requested |
| `wav_match` | boolean | Trip took place in a wheelchair-accessible vehicle |
| `is_reservation_like` | boolean | request_datetime later than on_scene_datetime (scheduled ride) |
| `is_unknown_zone` | boolean | Pickup or dropoff zone is 264/265 |
| `is_non_positive_fare` | boolean | base_passenger_fare <= 0 |
| `loaded_at` | timestamp | UTC load time |

### `uber_lake.trips_quarantine`

Uber trips rejected by hard validity rules; kept for audit. Partitioned by: none.

| Column | Type | Description |
|---|---|---|
| `trip_id` | string | Deterministic id: <license>-<YYYY-MM>-<row number in the TLC file> |
| `source_month` | string | TLC file month (YYYY-MM); the idempotency key of a load |
| `hvfhs_license_num` | string | HVFHS licence: HV0003 = Uber |
| `dispatching_base_num` | string | TLC base that dispatched the trip |
| `originating_base_num` | string | TLC base that received the request |
| `request_datetime` | timestamp | Rider request time (NYC local); 5-min marks suggest reservations |
| `on_scene_datetime` | timestamp | Driver arrival at pickup (NYC local) |
| `pickup_datetime` | timestamp | Trip start (NYC local) |
| `dropoff_datetime` | timestamp | Trip end (NYC local) |
| `pu_location_id` | int | TLC taxi zone of pickup (1-263; 264/265 unknown/outside NYC) |
| `do_location_id` | int | TLC taxi zone of dropoff |
| `trip_miles` | double | Passenger trip miles |
| `trip_time_s` | long | Passenger trip seconds |
| `base_passenger_fare` | double | Base fare before tolls, tips, taxes and fees (USD) |
| `tolls` | double | Tolls (USD) |
| `bcf` | double | Black Car Fund (USD) |
| `sales_tax` | double | NYS sales tax (USD) |
| `congestion_surcharge` | double | NYS congestion surcharge (USD) |
| `airport_fee` | double | Airport fee (USD) |
| `tips` | double | Tips (USD) |
| `driver_pay` | double | Driver pay excluding tolls and tips (USD) |
| `cbd_congestion_fee` | double | MTA Congestion Relief Zone toll (USD), since 2025-01-05 |
| `shared_request` | boolean | Rider agreed to a shared ride |
| `shared_match` | boolean | Rider actually shared the vehicle |
| `access_a_ride` | boolean | Trip administered for the MTA Access-A-Ride programme |
| `wav_request` | boolean | Wheelchair-accessible vehicle requested |
| `wav_match` | boolean | Trip took place in a wheelchair-accessible vehicle |
| `is_reservation_like` | boolean | request_datetime later than on_scene_datetime (scheduled ride) |
| `is_unknown_zone` | boolean | Pickup or dropoff zone is 264/265 |
| `is_non_positive_fare` | boolean | base_passenger_fare <= 0 |
| `loaded_at` | timestamp | UTC load time |
| `reject_reason` | string | Which hard validity rule failed |

### `uber_lake.taxi_zones`

TLC taxi zone lookup. Partitioned by: none.

| Column | Type | Description |
|---|---|---|
| `location_id` | int | TLC taxi zone id |
| `borough` | string | Borough (EWR, Unknown, N/A for special ids) |
| `zone` | string | Zone name |
| `service_zone` | string | Yellow Zone, Boro Zone, Airports, EWR |

### `uber_lake.weather_hourly`

Open-Meteo hourly reanalysis for one grid cell (requested 40.713, -74.006; returned cell centre 40.738, -74.043). Partitioned by: none.

| Column | Type | Description |
|---|---|---|
| `observed_hour` | timestamp | Hour start, NYC local time |
| `source_month` | string | Month the hour was fetched for (YYYY-MM) |
| `temperature_2m` | double | Air temperature at 2 m (deg C) |
| `relative_humidity_2m` | double | Relative humidity at 2 m (%) |
| `precipitation` | double | Total precipitation in the preceding hour (mm) |
| `rain` | double | Rain in the preceding hour (mm) |
| `snowfall` | double | Snowfall in the preceding hour (cm) |
| `cloud_cover` | double | Total cloud cover (%) |
| `wind_speed_10m` | double | Wind speed at 10 m (km/h) |
| `weather_code` | int | WMO weather code |

### `uber_lake.ride_events`

Accepted ride lifecycle events from the streaming path (at-least-once landing). Partitioned by: event_time (day).

| Column | Type | Description |
|---|---|---|
| `event_id` | string | sha256(trip_id|event_type)[:32]; the dedupe key |
| `event_type` | string | requested | driver_on_scene | picked_up | dropped_off |
| `event_time` | timestamp | Event time (NYC local), from the TLC timestamps |
| `trip_id` | string | Trip the event belongs to |
| `zone_id` | int | Zone the event happened in (pickup zone, dropoff zone for dropped_off) |
| `pu_location_id` | int | Pickup zone |
| `do_location_id` | int | Dropoff zone |
| `schema_version` | int | Envelope schema version |
| `trip_miles` | double | dropped_off only |
| `trip_time_s` | long | dropped_off only |
| `base_passenger_fare` | double | dropped_off only |
| `driver_pay` | double | dropped_off only |
| `tips` | double | dropped_off only |
| `cbd_congestion_fee` | double | dropped_off only |
| `airport_fee` | double | dropped_off only |
| `shared_request` | boolean | requested only |
| `shared_match` | boolean | dropped_off only |
| `produced_at` | timestamp | Producer wall-clock time (UTC) |
| `shard_id` | string | Kinesis shard |
| `sequence_number` | string | Kinesis sequence number |
| `landing_object` | string | Landing object key the event was loaded from (idempotency key) |
| `loaded_at` | timestamp | UTC load time |

### `uber_lake.zone_window_counters`

Snapshot of the real-time DynamoDB zone supply/demand counters. Partitioned by: none.

| Column | Type | Description |
|---|---|---|
| `zone_id` | int | TLC taxi zone |
| `window_start` | timestamp | Window start (NYC local) |
| `window_minutes` | int | Window length |
| `requested` | long | requested events |
| `driver_on_scene` | long | driver_on_scene events |
| `picked_up` | long | picked_up events |
| `dropped_off` | long | dropped_off events (drivers freed here: the supply proxy) |
| `surge_proxy` | double | requested / max(dropped_off, 1) |
| `snapshot_at` | timestamp | UTC time the DynamoDB table was exported |

### `uber_lake_ops.dq_results`

Data-quality gate results. Partitioned by: none.

| Column | Type | Description |
|---|---|---|
| `run_id` | string | Gate run id |
| `checked_at` | timestamp | UTC |
| `layer` | string | landing | lake | stream | marts |
| `check_name` | string | Check id |
| `severity` | string | error | warn |
| `status` | string | pass | fail |
| `failing_rows` | long | Rows violating the rule |
| `total_rows` | long | Rows checked |
| `threshold` | double | Allowed failing fraction |
| `detail` | string | Human-readable explanation |

### `uber_lake_ops.stream_dlq`

Records the consumer sent to the SQS dead-letter queue. Partitioned by: none.

| Column | Type | Description |
|---|---|---|
| `message_id` | string | SQS message id |
| `reason` | string | Why the record was rejected |
| `error` | string | Validation error detail |
| `shard_id` | string | Kinesis shard |
| `sequence_number` | string | Kinesis sequence number |
| `raw` | string | Original record payload (truncated to 4 KB) |
| `received_at` | timestamp | UTC |

## dbt models (DuckDB locally, Athena on AWS)

Schemas: `staging` (views), `intermediate` and `marts` (tables). On Athena they are the Glue
databases `uber_staging`, `uber_intermediate`, `uber_marts` (Iceberg tables).

| Model | Grain | Key columns |
|---|---|---|
| `stg_trips` | trip | `trip_id`, `request_at`, `pickup_at`, `wait_seconds` (request to pickup; negative for scheduled rides), `driver_arrival_seconds`, `rider_total_ex_tips` (base + tolls + BCF + sales tax + congestion surcharge + airport fee + CBD fee), flags |
| `stg_taxi_zones` | zone | `zone_id`, `borough`, `zone_name`, `service_zone`, `airport_code` (1 = EWR, 132 = JFK, 138 = LGA) |
| `stg_weather_hourly` | hour | `observed_hour`, `temperature_c`, `precipitation_mm`, `precipitation_band` (dry / light < 1 / moderate 1-4 / heavy >= 4 mm/h) |
| `stg_ride_events` | event | first-loaded copy of each `event_id` (the landing zone is at-least-once) |
| `stg_zone_window_counters` | zone x window | counter snapshot from DynamoDB |
| `int_trips_enriched` | trip | + pickup/dropoff borough and zone, `time_band`, `wait_bucket`, `airport_direction`, `is_cbd_charged`, `speed_mph`, weather at the request hour |
| `mart_hourly_zone_demand` | request hour x pickup zone | `requests`, `picked_up_within_target` (<= 10 min), `pct_pickup_within_10_min`, `avg/p50/p90_wait_minutes`, `dropoffs_in_zone` (supply proxy), `demand_supply_ratio` |
| `mart_wait_time_distribution` | borough x time band x wait bucket | `trips`, `share_of_cell` |
| `mart_wait_time_percentiles` | borough x time band | p50/p75/p90/p95 wait, avg driver arrival |
| `mart_driver_pay_share` | request date x borough | `driver_pay_share_of_fare` (driver_pay / base fare), `driver_share_of_rider_spend` ((pay + tips) / (rider total + tips)), `driver_pay_per_trip_hour`, `pct_trips_tipped` |
| `mart_airport_trips` | date x airport x direction | trips, fares, miles, minutes, airport fees, wait |
| `mart_cbd_congestion_fee` | hour of day x weekend x charged | trips, fees, fare, speed, fee share of rider total (descriptive) |
| `mart_shared_rides` | date x borough | `shared_requests`, `shared_matches`, `match_rate` |
| `mart_weather_demand` | request hour | requests, weather, `demand_index` = requests / mean of the same weekday-hour |
| `mart_weather_demand_summary` | precipitation band | hours, `avg_demand_index`, `avg_wait_minutes` |
| `mart_borough_flows` | pickup borough x dropoff borough | trips, `share_of_trips`, miles, minutes, fare, pay |
| `mart_stream_zone_supply_demand` | window x zone | streaming counters with zone names, `surge_proxy` = requested / max(dropped_off, 1) |
| `mart_stream_event_summary` | event type | distinct landed events, trips, event-time span |
| `dim_zone` | zone | zone attributes |
| `fct_daily_summary` | request date | headline totals and averages |

Trips requested on the last evening of the previous month but picked up in this month belong to
this month's file, so `request_date` can fall on the previous month's last day.
