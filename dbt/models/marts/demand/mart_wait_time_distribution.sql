-- Histogram of request -> pickup wait by pickup borough and time band.
-- Reservation-like trips (request logged after driver arrival) have negative waits and are excluded.
with cells as (
    select pu_borough as borough, time_band, wait_bucket, count(*) as trips
    from {{ ref('int_trips_enriched') }}
    where wait_seconds >= 0
    group by 1, 2, 3
)

select
    borough,
    time_band,
    wait_bucket,
    trips,
    {{ safe_divide('trips', 'sum(trips) over (partition by borough, time_band)') }} as share_of_cell
from cells
