-- Every trip is counted exactly once in the additive marts.
with expected as (select count(*) as n from {{ ref('stg_trips') }}),
checks as (
    select 'mart_hourly_zone_demand' as mart, sum(requests) as n from {{ ref('mart_hourly_zone_demand') }}
    union all
    select 'mart_borough_flows', sum(trips) from {{ ref('mart_borough_flows') }}
    union all
    select 'mart_shared_rides', sum(trips) from {{ ref('mart_shared_rides') }}
    union all
    select 'fct_daily_summary', sum(trips) from {{ ref('fct_daily_summary') }}
    union all
    select 'mart_cbd_congestion_fee', sum(trips) from {{ ref('mart_cbd_congestion_fee') }}
)

select c.mart, c.n as mart_trips, e.n as expected_trips
from checks c cross join expected e
where c.n <> e.n
