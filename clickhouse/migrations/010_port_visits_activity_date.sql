-- Additive upgrade: unknown historical dates stay NULL until explicitly backfilled.
-- New writes supply the UTC processing window-start date, never arrival_at/run_id.
-- Apply with port-visit writers idle, then run:
-- python -m pipelines.port_visits.backfill_activity_dates --apply
-- The backfill publishes only missing logical versions (including tombstones),
-- using port_visit_runs.window_start. Old physical versions may remain NULL until
-- normal merges; logical FINAL rows must be non-null after the backfill.
ALTER TABLE analytics.port_visits
    ADD COLUMN IF NOT EXISTS activity_date Nullable(Date);
