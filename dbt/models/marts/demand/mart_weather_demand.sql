-- City-wide requests per hour against the weather in that hour.
-- demand_index compares an hour with the average of the same weekday-and-hour in the data, so
-- the weekly cycle is removed before weather is looked at.
with hourly as (
    select
        request_hour,
        min(request_iso_dow) as iso_dow,
        min(request_hour_of_day) as hour_of_day,
        count(*) as requests,
        avg(case when wait_seconds >= 0 then wait_seconds end) / 60.0 as avg_wait_minutes,
        min(temperature_c) as temperature_c,
        min(precipitation_mm) as precipitation_mm,
        min(snowfall_cm) as snowfall_cm,
        min(precipitation_band) as precipitation_band
    from {{ ref('int_trips_enriched') }}
    group by 1
)

select
    request_hour,
    iso_dow,
    hour_of_day,
    requests,
    avg_wait_minutes,
    temperature_c,
    precipitation_mm,
    snowfall_cm,
    precipitation_band,
    avg(requests) over (partition by iso_dow, hour_of_day) as baseline_requests,
    {{ safe_divide('requests', 'avg(requests) over (partition by iso_dow, hour_of_day)') }} as demand_index,
    count(*) over (partition by iso_dow, hour_of_day) as baseline_hours
from hourly
