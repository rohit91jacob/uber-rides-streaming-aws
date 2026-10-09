select
    zone_id,
    window_start,
    window_minutes,
    requested,
    driver_on_scene,
    picked_up,
    dropped_off,
    surge_proxy,
    snapshot_at
from {{ source('lake', 'zone_window_counters') }}
