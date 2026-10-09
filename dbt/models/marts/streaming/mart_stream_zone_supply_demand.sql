-- The real-time view: DynamoDB counters exported after a replay, per zone and window.
-- surge_proxy = requests / max(dropoffs, 1) in the window (each dropoff frees a driver there).
select
    c.window_start,
    c.window_minutes,
    c.zone_id,
    z.zone_name,
    z.borough,
    c.requested,
    c.driver_on_scene,
    c.picked_up,
    c.dropped_off,
    c.surge_proxy,
    c.requested - c.dropped_off as net_demand,
    c.snapshot_at
from {{ ref('stg_zone_window_counters') }} c
left join {{ ref('stg_taxi_zones') }} z on c.zone_id = z.zone_id
