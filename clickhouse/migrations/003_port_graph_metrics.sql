-- Manual migration: persist already-computed Neo4j Port metrics in ClickHouse.
-- Apply before running ais_graph_metrics_export; no GDS algorithms run here.
CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.port_graph_metrics
(
    run_id String,
    window_start DateTime64(6, 'UTC'),
    window_end DateTime64(6, 'UTC'),
    snapshot_date Date,
    port_id String,
    page_rank Float64,
    community_id UInt64,
    exported_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(exported_at)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (run_id, port_id);

-- snapshot_date is the UTC window_end date, so retries of a run stay in one partition.
-- Repeated exports replace the logical (run_id, port_id) row by exported_at.
-- Physical versions can coexist before merges; query with FINAL for logical rows.
