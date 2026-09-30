-- Validate one snapshot date per run without assuming one run per date.
with dates as (
    select
        m.run_id as run_id,
        m.activity_date as activity_date,
        m.snapshot_date as snapshot_date,
        toDate(m.window_start, 'UTC') as processing_date
    from {{ ref('port_graph_metrics_enriched') }} as m

    union all

    select
        c.run_id as run_id,
        c.activity_date as activity_date,
        c.activity_date as snapshot_date,
        c.activity_date as processing_date
    from {{ ref('port_graph_communities') }} as c
)
select d.run_id as run_id
from dates as d
group by d.run_id
having uniqExact(d.activity_date) != 1
    or countIf(d.activity_date != d.snapshot_date) > 0
    or countIf(d.snapshot_date != d.processing_date) > 0
