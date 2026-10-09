select
    location_id as zone_id,
    borough,
    zone as zone_name,
    service_zone,
    case location_id when 1 then 'EWR' when 132 then 'JFK' when 138 then 'LGA' end as airport_code,
    location_id in (1, 132, 138) as is_airport,
    location_id > 263 as is_unknown_zone
from {{ source('lake', 'taxi_zones') }}
