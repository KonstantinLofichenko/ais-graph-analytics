-- Additive: leaves the live raw/Kafka pipeline unchanged.
CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.ports
(
    port_id String, name String, country String,
    latitude Float64, longitude Float64, radius_m Float64,
    dataset_hash String, updated_at DateTime64(6, 'UTC'),
    created_at DateTime64(6, 'UTC') DEFAULT updated_at
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY port_id;

-- Upgrade existing installations without resetting port data.
ALTER TABLE analytics.ports ADD COLUMN IF NOT EXISTS
    created_at DateTime64(6, 'UTC') DEFAULT updated_at;

-- New run_id values are canonical UTC window_start timestamps (seconds, trailing Z).
-- FINAL preserves the existing retry behavior; historical hash IDs remain valid keys.
CREATE TABLE IF NOT EXISTS analytics.port_visits
(
    run_id String, visit_id String, mmsi UInt32, port_id String,
    arrival_at DateTime64(6, 'UTC'), last_observed_at DateTime64(6, 'UTC'),
    departure_at Nullable(DateTime64(6, 'UTC')),
    arrival_censored UInt8, end_reason LowCardinality(String),
    observation_count UInt32, observed_stay_seconds Float64,
    updated_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (run_id, visit_id);

-- Written only after the graph transaction succeeds. Incomplete snapshots are not current.
CREATE TABLE IF NOT EXISTS analytics.port_visit_runs
(
    run_id String, window_start DateTime64(6, 'UTC'), window_end DateTime64(6, 'UTC'),
    dataset_hash String, parameters String, source_rows UInt64,
    visit_count UInt64, completed_at DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(completed_at)
ORDER BY run_id;

CREATE VIEW IF NOT EXISTS analytics.current_port_visits AS
SELECT * FROM analytics.port_visits FINAL
WHERE run_id = (SELECT run_id FROM analytics.port_visit_runs FINAL
                ORDER BY completed_at DESC, run_id DESC LIMIT 1);
