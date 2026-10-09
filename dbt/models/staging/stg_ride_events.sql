-- The landing zone is at-least-once (a replay run twice lands every event twice): keep the
-- first-loaded copy of each event_id.
with ranked as (
    select
        *,
        row_number() over (partition by event_id order by loaded_at, landing_object, sequence_number) as copy_rank
    from {{ source('lake', 'ride_events') }}
)

select
    event_id,
    event_type,
    event_time,
    trip_id,
    zone_id,
    pu_location_id as pu_zone_id,
    do_location_id as do_zone_id,
    trip_miles,
    trip_time_s as trip_seconds,
    base_passenger_fare as base_fare,
    driver_pay,
    tips,
    cbd_congestion_fee,
    shared_request as is_shared_request,
    shared_match as is_shared_match,
    produced_at,
    shard_id,
    sequence_number,
    landing_object,
    loaded_at
from ranked
where copy_rank = 1
