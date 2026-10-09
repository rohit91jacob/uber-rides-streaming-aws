-- Origin-destination matrix between boroughs.
select
    pu_borough,
    do_borough,
    count(*) as trips,
    {{ safe_divide('count(*)', 'sum(count(*)) over ()') }} as share_of_trips,
    avg(trip_miles) as avg_trip_miles,
    avg(trip_seconds) / 60.0 as avg_trip_minutes,
    avg(base_fare) as avg_base_fare_usd,
    avg(driver_pay) as avg_driver_pay_usd
from {{ ref('int_trips_enriched') }}
group by 1, 2
