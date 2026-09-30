{{ config(
    materialized='view'
) }}

-- Exported labels are shared by all ports in a snapshot community.
-- Nullable aggregates preserve NULL for historical communities without labels.
select
    c.run_id as run_id,
    min(c.activity_date) as activity_date,
    c.community_id as community_id,
    max(c.community_name) as community_name,
    max(c.community_label) as community_label,
    uniqExact(c.port_id) as port_count

from {{ ref('port_graph_metrics_enriched') }} as c

group by
    c.run_id,
    c.community_id
