-- Trips that paid the MTA Congestion Relief Zone fee (cbd_congestion_fee > 0) versus the rest,
-- by request hour of day and weekend flag. Descriptive only: charged trips differ from the rest
-- in where they go, so the differences are not a causal estimate of the fee's effect.
select
    request_hour_of_day,
    is_weekend,
    is_cbd_charged,
    count(*) as trips,
    sum(cbd_congestion_fee) as cbd_fee_usd,
    avg(case when is_cbd_charged then cbd_congestion_fee end) as avg_fee_when_charged_usd,
    avg(base_fare) as avg_base_fare_usd,
    avg(rider_total_ex_tips) as avg_rider_total_ex_tips_usd,
    {{ safe_divide('sum(cbd_congestion_fee)', 'sum(rider_total_ex_tips)') }} as fee_share_of_rider_total,
    avg(speed_mph) as avg_speed_mph,
    avg(trip_miles) as avg_trip_miles,
    avg(case when wait_seconds >= 0 then wait_seconds end) / 60.0 as avg_wait_minutes
from {{ ref('int_trips_enriched') }}
group by 1, 2, 3
