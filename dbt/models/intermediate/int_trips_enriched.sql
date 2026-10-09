-- Trips joined to pickup/dropoff zones and the weather at the request hour, with time features.
with trips as (
    select * from {{ ref('stg_trips') }}
),

zones as (
    select * from {{ ref('stg_taxi_zones') }}
),

weather as (
    select * from {{ ref('stg_weather_hourly') }}
)

select
    t.*,
    pu.borough as pu_borough,
    pu.zone_name as pu_zone_name,
    pu.service_zone as pu_service_zone,
    pu.airport_code as pu_airport_code,
    do_.borough as do_borough,
    do_.zone_name as do_zone_name,
    do_.airport_code as do_airport_code,
    extract(hour from t.request_at) as request_hour_of_day,
    {{ iso_dow('t.request_at') }} as request_iso_dow,
    {{ iso_dow('t.request_at') }} >= 6 as is_weekend,
    case
        when extract(hour from t.request_at) between 7 and 9 then '1 am peak (7-10)'
        when extract(hour from t.request_at) between 10 and 15 then '2 midday (10-16)'
        when extract(hour from t.request_at) between 16 and 19 then '3 pm peak (16-20)'
        when extract(hour from t.request_at) between 20 and 23 then '4 evening (20-24)'
        else '5 overnight (0-7)'
    end as time_band,
    case
        when t.wait_seconds < 0 then null
        when t.wait_seconds < 120 then '00-02 min'
        when t.wait_seconds < 300 then '02-05 min'
        when t.wait_seconds < 600 then '05-10 min'
        when t.wait_seconds < 900 then '10-15 min'
        when t.wait_seconds < 1200 then '15-20 min'
        when t.wait_seconds < 1800 then '20-30 min'
        else '30+ min'
    end as wait_bucket,
    case
        when pu.airport_code is not null and do_.airport_code is not null then 'airport_to_airport'
        when pu.airport_code is not null then 'from_airport'
        when do_.airport_code is not null then 'to_airport'
    end as airport_direction,
    coalesce(pu.airport_code, do_.airport_code) as airport_code,
    t.cbd_congestion_fee > 0 as is_cbd_charged,
    case when t.trip_seconds > 0 then t.trip_miles / (t.trip_seconds / 3600.0) end as speed_mph,
    w.temperature_c,
    w.precipitation_mm,
    w.snowfall_cm,
    w.precipitation_band
from trips t
left join zones pu on t.pu_zone_id = pu.zone_id
left join zones do_ on t.do_zone_id = do_.zone_id
left join weather w on t.request_hour = w.observed_hour
