-- Additive upgrade: existing versions stay active via DEFAULT 0; no data rewrite.
-- Keep ReplacingMergeTree(updated_at) and ORDER BY (run_id, visit_id) unchanged.
ALTER TABLE analytics.port_visits ADD COLUMN IF NOT EXISTS is_deleted UInt8 DEFAULT 0;

-- Upgrade the bootstrap view too; dbt maintains the same active-row predicate.
CREATE OR REPLACE VIEW analytics.current_port_visits AS
SELECT run_id, visit_id, mmsi, port_id, arrival_at, last_observed_at, departure_at,
       arrival_censored, end_reason, observation_count, observed_stay_seconds, updated_at
FROM analytics.port_visits FINAL
WHERE is_deleted = 0 AND run_id = (SELECT run_id FROM analytics.port_visit_runs FINAL
                ORDER BY completed_at DESC, run_id DESC LIMIT 1);
