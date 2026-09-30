{{ config(
    materialized='view'
) }}

select
    v.run_id as run_id,
    v.activity_date as activity_date,
    v.visit_id as visit_id,
    v.mmsi as mmsi,
    v.port_id as port_id,
    v.arrival_at as arrival_at,
    v.last_observed_at as last_observed_at,
    v.departure_at as departure_at,
    v.arrival_censored as arrival_censored,
    v.end_reason as end_reason,
    v.observation_count as observation_count,
    v.observed_stay_seconds as observed_stay_seconds,
    v.updated_at as updated_at

from {{ source('analytics', 'port_visits') }} as v final

where v.is_deleted = 0
  and v.run_id = (
    select run_id
    from {{ source('analytics', 'port_visit_runs') }} final
    order by
        completed_at desc,
        run_id desc
    limit 1
)
