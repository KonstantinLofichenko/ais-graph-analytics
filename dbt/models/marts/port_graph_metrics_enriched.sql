{{ config(
    materialized='view'
) }}

select
    g.run_id as run_id,
    g.window_start as window_start,
    g.window_end as window_end,
    g.snapshot_date as snapshot_date,
    g.snapshot_date as activity_date,

    g.port_id as port_id,
    p.name as port_name,

    p.country as country_code,
    c.country_name as country_name,
    c.iso3 as iso3,
    c.numeric_code as numeric_code,

    p.latitude as latitude,
    p.longitude as longitude,

    g.page_rank as page_rank,
    g.community_id as community_id,
    g.community_name as community_name,
    g.community_label as community_label,

    coalesce(v.unique_vessels, 0) as unique_vessels,
    coalesce(v.visit_count, 0) as visit_count,

    g.exported_at as exported_at

from
(
    select *
    from {{ source('analytics', 'port_graph_metrics') }} final
    where is_deleted = 0
) as g

left join {{ source('analytics', 'ports') }} as p final
    on g.port_id = p.port_id

left join {{ ref('countries') }} as c
    on p.country = c.country_code

left join
(
    select
        run_id,
        port_id,
        uniqExact(mmsi) as unique_vessels,
        count() as visit_count
    from {{ source('analytics', 'port_visits') }} final
    where is_deleted = 0
    group by
        run_id,
        port_id
) as v
    on g.run_id = v.run_id
   and g.port_id = v.port_id
