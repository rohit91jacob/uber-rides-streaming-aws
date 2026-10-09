select
    observed_hour,
    temperature_2m as temperature_c,
    relative_humidity_2m as relative_humidity_pct,
    coalesce(precipitation, 0) as precipitation_mm,
    coalesce(rain, 0) as rain_mm,
    coalesce(snowfall, 0) as snowfall_cm,
    cloud_cover as cloud_cover_pct,
    wind_speed_10m as wind_speed_kmh,
    weather_code,
    case
        when coalesce(precipitation, 0) = 0 then 'dry'
        when precipitation < 1 then 'light (<1 mm/h)'
        when precipitation < 4 then 'moderate (1-4 mm/h)'
        else 'heavy (>=4 mm/h)'
    end as precipitation_band
from {{ source('lake', 'weather_hourly') }}
