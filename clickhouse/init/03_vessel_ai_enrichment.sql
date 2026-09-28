CREATE TABLE IF NOT EXISTS analytics.vessel_ai_enrichment
(
    mmsi UInt32,
    activity_date Date,

    activity_class LowCardinality(String),
    navigation_status_quality LowCardinality(String),

    summary String,
    notable_behavior String,
    data_quality_note Nullable(String),

    model LowCardinality(String),
    prompt_version LowCardinality(String),
    input_hash FixedString(64),

    openai_response_id String,
    input_tokens UInt32,
    output_tokens UInt32,

    created_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
ORDER BY (
    activity_date,
    mmsi,
    prompt_version,
    input_hash
);