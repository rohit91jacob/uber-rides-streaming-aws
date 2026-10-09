-- Streaming path health per event type: distinct events landed (after dedupe), the trips they
-- cover, their event-time span and the number of landing objects they came from.
select
    event_type,
    count(*) as events,
    count(distinct trip_id) as trips,
    min(event_time) as first_event_time,
    max(event_time) as last_event_time,
    count(distinct landing_object) as landing_objects
from {{ ref('stg_ride_events') }}
group by 1
