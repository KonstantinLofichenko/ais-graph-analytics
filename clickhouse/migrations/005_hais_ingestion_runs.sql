-- Manual migration: HAIS file ingestion attempts and their outcomes.
CREATE DATABASE IF NOT EXISTS raw;

-- Practical file identity is (file_name, file_size); no content hash is stored.
-- Preserve historical attempts, including repeated attempts for the same file.
-- Expected statuses are running, success, and failed; no status constraint is enforced.
CREATE TABLE IF NOT EXISTS raw.hais_ingestion_runs
(
    file_name String,
    file_size UInt64,
    source_date Date,
    row_count Nullable(UInt64),
    status LowCardinality(String),
    started_at DateTime64(3, 'UTC'),
    completed_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
-- Group each file's attempts chronologically; the sorting key is not unique.
ORDER BY (file_name, file_size, started_at);
