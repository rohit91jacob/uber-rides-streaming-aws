-- Headline numbers per request date (used by the results site and docs/verification.md).
select
    request_date,
    count(*) as trips,
    sum(trip_miles) as trip_miles,
    avg(trip_miles) as avg_trip_miles,
    avg(base_fare) as avg_base_fare_usd,
    avg(driver_pay) as avg_driver_pay_usd,
    sum(base_fare) as base_fare_usd,
    sum(driver_pay) as driver_pay_usd,
    sum(tips) as tips_usd,
    avg(trip_seconds) / 60.0 as avg_trip_minutes,
    avg(case when wait_seconds >= 0 then wait_seconds end) / 60.0 as avg_wait_minutes,
    count(*) filter (where is_shared_match) as shared_trips,
    count(*) filter (where is_cbd_charged) as cbd_charged_trips,
    count(*) filter (where airport_direction is not null) as airport_trips
from {{ ref('int_trips_enriched') }}
group by 1
