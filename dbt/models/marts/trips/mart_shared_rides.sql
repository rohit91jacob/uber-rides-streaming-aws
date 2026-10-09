-- Shared-ride requests and how many were actually matched with another rider.
select
    request_date,
    pu_borough as borough,
    count(*) as trips,
    count(*) filter (where is_shared_request) as shared_requests,
    count(*) filter (where is_shared_request and is_shared_match) as shared_matches,
    count(*) filter (where is_shared_match) as shared_trips,
    coalesce(
        {{ safe_divide('count(*) filter (where is_shared_request and is_shared_match)', 'count(*) filter (where is_shared_request)') }},
        0
    ) as match_rate,
    avg(case when is_shared_request then base_fare end) as avg_shared_request_fare_usd,
    avg(case when not is_shared_request then base_fare end) as avg_solo_fare_usd
from {{ ref('int_trips_enriched') }}
group by 1, 2
