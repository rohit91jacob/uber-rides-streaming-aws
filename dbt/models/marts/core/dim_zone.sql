select zone_id, zone_name, borough, service_zone, airport_code, is_airport, is_unknown_zone
from {{ ref('stg_taxi_zones') }}
