-- Snapshot-specific Louvain labels belong with metric facts, not current dimensions.
-- Additive and rerunnable: existing historical rows retain NULL labels until explicitly re-exported.
ALTER TABLE analytics.port_graph_metrics
    ADD COLUMN IF NOT EXISTS community_name Nullable(String),
    ADD COLUMN IF NOT EXISTS community_label Nullable(String);
