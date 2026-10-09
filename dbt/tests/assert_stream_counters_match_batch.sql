-- The real-time DynamoDB "requested" counters must equal the batch count of requests in the
-- same zone and 15-minute window, for every window the latest replay covered (exactly-once
-- counting). The counters table holds the latest run only, so batch rows are restricted to its
-- window range.
{% set window_minutes = 15 %}
with counters as (
    select zone_id, window_start, requested from {{ ref('stg_zone_window_counters') }}
),

replayed_trips as (
    select distinct e.trip_id
    from {{ ref('stg_ride_events') }} e
    where e.event_type = 'requested'
),

batch as (
    select
        t.pu_zone_id as zone_id,
        date_trunc('hour', t.request_at)
            + (cast(floor(extract(minute from t.request_at) / {{ window_minutes }}) as integer) * {{ window_minutes }}) * interval '1' minute
            as window_start,
        count(*) as requested
    from {{ ref('stg_trips') }} t
    inner join replayed_trips r on t.trip_id = r.trip_id
    group by 1, 2
),

batch_in_range as (
    select b.* from batch b
    cross join (select min(window_start) as lo, max(window_start) as hi from counters) r
    where b.window_start between r.lo and r.hi
)

select coalesce(c.zone_id, b.zone_id) as zone_id, coalesce(c.window_start, b.window_start) as window_start,
       c.requested as counted, b.requested as expected
from (select * from counters where requested > 0) c
full outer join batch_in_range b on c.zone_id = b.zone_id and c.window_start = b.window_start
where coalesce(c.requested, 0) <> coalesce(b.requested, 0)
