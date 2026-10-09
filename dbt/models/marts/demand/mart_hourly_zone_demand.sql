-- Demand and fulfilment per pickup zone and request hour.
-- TLC files contain completed trips only (no cancellations), so "fulfilment" is measured as
-- the share of requests picked up within var('fast_pickup_minutes') minutes. Dropoffs in the
-- same zone and hour are the supply proxy: each one frees a driver there.
with trips as (
    select * from {{ ref('int_trips_enriched') }}
),

demand as (
    select
        request_hour,
        pu_zone_id as zone_id,
        min(pu_zone_name) as zone_name,
        min(pu_borough) as borough,
        count(*) as requests,
        count(*) filter (where is_reservation_like) as reservation_like_requests,
        count(*) filter (
            where wait_seconds between 0 and {{ var('fast_pickup_minutes') }} * 60
        ) as picked_up_within_target,
        avg(case when wait_seconds >= 0 then wait_seconds end) / 60.0 as avg_wait_minutes,
        {{ percentile('case when wait_seconds >= 0 then wait_seconds end', 0.5) }} / 60.0 as p50_wait_minutes,
        {{ percentile('case when wait_seconds >= 0 then wait_seconds end', 0.9) }} / 60.0 as p90_wait_minutes,
        sum(base_fare) as base_fare_usd
    from trips
    group by 1, 2
),

supply as (
    select
        date_trunc('hour', dropoff_at) as dropoff_hour,
        do_zone_id as zone_id,
        count(*) as dropoffs
    from trips
    group by 1, 2
)

select
    d.request_hour,
    d.zone_id,
    d.zone_name,
    d.borough,
    d.requests,
    d.reservation_like_requests,
    d.picked_up_within_target,
    {{ safe_divide('d.picked_up_within_target', 'd.requests') }} as pct_pickup_within_10_min,
    d.avg_wait_minutes,
    d.p50_wait_minutes,
    d.p90_wait_minutes,
    d.base_fare_usd,
    coalesce(s.dropoffs, 0) as dropoffs_in_zone,
    {{ safe_divide('d.requests', 's.dropoffs') }} as demand_supply_ratio
from demand d
left join supply s on d.request_hour = s.dropoff_hour and d.zone_id = s.zone_id
