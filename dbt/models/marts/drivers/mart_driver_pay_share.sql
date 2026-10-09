-- How much of the fare reaches the driver, per request date and pickup borough.
-- driver_pay excludes tips and tolls (TLC definition); trips with a non-positive base fare are
-- excluded from the ratios.
select
    request_date,
    pu_borough as borough,
    count(*) as trips,
    sum(base_fare) as base_fare_usd,
    sum(rider_total_ex_tips) as rider_total_ex_tips_usd,
    sum(driver_pay) as driver_pay_usd,
    sum(tips) as tips_usd,
    {{ safe_divide('sum(driver_pay)', 'sum(base_fare)') }} as driver_pay_share_of_fare,
    {{ safe_divide('sum(driver_pay) + sum(tips)', 'sum(rider_total_ex_tips) + sum(tips)') }} as driver_share_of_rider_spend,
    {{ safe_divide('sum(driver_pay)', 'sum(trip_seconds) / 3600.0') }} as driver_pay_per_trip_hour,
    {{ safe_divide('count(*) filter (where tips > 0)', 'count(*)') }} as pct_trips_tipped
from {{ ref('int_trips_enriched') }}
where base_fare > 0
group by 1, 2
