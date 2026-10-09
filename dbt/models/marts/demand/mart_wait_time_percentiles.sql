-- Wait-time percentiles (minutes) by pickup borough and time band.
select
    pu_borough as borough,
    time_band,
    count(*) as trips,
    avg(wait_seconds) / 60.0 as avg_wait_minutes,
    {{ percentile('wait_seconds', 0.5) }} / 60.0 as p50_wait_minutes,
    {{ percentile('wait_seconds', 0.75) }} / 60.0 as p75_wait_minutes,
    {{ percentile('wait_seconds', 0.9) }} / 60.0 as p90_wait_minutes,
    {{ percentile('wait_seconds', 0.95) }} / 60.0 as p95_wait_minutes,
    avg(case when driver_arrival_seconds >= 0 then driver_arrival_seconds end) / 60.0 as avg_driver_arrival_minutes
from {{ ref('int_trips_enriched') }}
where wait_seconds >= 0
group by 1, 2
