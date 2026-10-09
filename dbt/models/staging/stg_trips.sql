-- One row per Uber trip with friendly names, rider total and wait times.
with src as (
    select * from {{ source('lake', 'trips') }}
    where {{ license_filter() }}
)

select
    trip_id,
    source_month,
    dispatching_base_num,
    request_datetime as request_at,
    on_scene_datetime as on_scene_at,
    pickup_datetime as pickup_at,
    dropoff_datetime as dropoff_at,
    date_trunc('hour', request_datetime) as request_hour,
    cast(request_datetime as date) as request_date,
    pu_location_id as pu_zone_id,
    do_location_id as do_zone_id,
    trip_miles,
    trip_time_s as trip_seconds,
    base_passenger_fare as base_fare,
    tolls,
    bcf,
    sales_tax,
    congestion_surcharge,
    airport_fee,
    tips,
    driver_pay,
    coalesce(cbd_congestion_fee, 0) as cbd_congestion_fee,
    -- what the rider paid before tips (TLC fare components)
    base_passenger_fare + coalesce(tolls, 0) + coalesce(bcf, 0) + coalesce(sales_tax, 0)
        + coalesce(congestion_surcharge, 0) + coalesce(airport_fee, 0) + coalesce(cbd_congestion_fee, 0)
        as rider_total_ex_tips,
    {{ seconds_between('request_datetime', 'pickup_datetime') }} as wait_seconds,
    {{ seconds_between('request_datetime', 'on_scene_datetime') }} as driver_arrival_seconds,
    shared_request as is_shared_request,
    shared_match as is_shared_match,
    wav_request as is_wav_request,
    wav_match as is_wav_match,
    access_a_ride as is_access_a_ride,
    is_reservation_like,
    is_unknown_zone,
    is_non_positive_fare
from src
