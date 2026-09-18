{{ config(
    materialized='view'
) }}

select
    run_id,
    visit_id,
    mmsi,
    port_id,
    arrival_at,
    last_observed_at,
    departure_at,
    arrival_censored,
    end_reason,
    observation_count,
    observed_stay_seconds,
    updated_at

from {{ source('analytics', 'port_visits') }} final

where run_id = (
    select run_id
    from {{ source('analytics', 'port_visit_runs') }} final
    order by
        completed_at desc,
        run_id desc
    limit 1
)
