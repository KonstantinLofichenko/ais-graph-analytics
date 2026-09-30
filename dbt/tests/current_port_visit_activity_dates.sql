select c.run_id as run_id, c.visit_id as visit_id
from {{ ref('current_port_visits') }} as c
left join (
    select v.run_id as run_id, v.visit_id as visit_id,
           v.activity_date as activity_date, 1 as present
    from {{ source('analytics', 'port_visits') }} as v final
    where v.is_deleted = 0
) as v on c.run_id = v.run_id and c.visit_id = v.visit_id
where v.present = 0 or c.activity_date is null or c.activity_date != v.activity_date
