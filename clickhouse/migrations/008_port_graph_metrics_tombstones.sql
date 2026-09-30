-- Additive: old rows remain active via DEFAULT 0, without rewriting history.
-- Keep ReplacingMergeTree(exported_at), monthly partitions, and (run_id, port_id).
ALTER TABLE analytics.port_graph_metrics
    ADD COLUMN IF NOT EXISTS is_deleted UInt8 DEFAULT 0;
-- Deploy active-row readers and rebuild the enriched dbt view before exporting.
