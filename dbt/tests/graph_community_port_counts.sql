-- Every active port must be represented exactly once in community totals.
with counts as (
    select
        c.run_id as run_id,
        sum(c.port_count) as community_ports,
        toUInt64(0) as metric_ports
    from {{ ref('port_graph_communities') }} as c
    group by c.run_id

    union all

    select
        m.run_id as run_id,
        toUInt64(0) as community_ports,
        count() as metric_ports
    from {{ ref('port_graph_metrics_enriched') }} as m
    group by m.run_id
)
select c.run_id as run_id
from counts as c
group by c.run_id
having sum(c.community_ports) != sum(c.metric_ports)
