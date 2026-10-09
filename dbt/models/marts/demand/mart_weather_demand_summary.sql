-- Demand index and wait by precipitation band (hours with a weather observation only).
select
    precipitation_band,
    count(*) as hours,
    sum(requests) as requests,
    avg(demand_index) as avg_demand_index,
    avg(avg_wait_minutes) as avg_wait_minutes,
    avg(temperature_c) as avg_temperature_c
from {{ ref('mart_weather_demand') }}
where precipitation_band is not null
group by 1
