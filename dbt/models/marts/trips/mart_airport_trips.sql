-- Trips to and from JFK, LaGuardia and Newark, per request date.
select
    request_date,
    airport_code,
    airport_direction,
    count(*) as trips,
    avg(base_fare) as avg_base_fare_usd,
    avg(rider_total_ex_tips) as avg_rider_total_ex_tips_usd,
    avg(trip_miles) as avg_trip_miles,
    avg(trip_seconds) / 60.0 as avg_trip_minutes,
    sum(airport_fee) as airport_fee_usd,
    avg(case when wait_seconds >= 0 then wait_seconds end) / 60.0 as avg_wait_minutes,
    avg(driver_pay) as avg_driver_pay_usd
from {{ ref('int_trips_enriched') }}
where airport_direction is not null
group by 1, 2, 3
