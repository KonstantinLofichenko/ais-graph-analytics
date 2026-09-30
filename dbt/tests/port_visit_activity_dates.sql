select v.run_id as run_id, v.visit_id as visit_id
from {{ source('analytics', 'port_visits') }} as v final
left join (
    select r.run_id as run_id, toDate(r.window_start, 'UTC') as activity_date, 1 as present
    from {{ source('analytics', 'port_visit_runs') }} as r final
) as r on v.run_id = r.run_id
where v.is_deleted = 0
  and (v.activity_date is null or r.present = 0 or v.activity_date != r.activity_date)
