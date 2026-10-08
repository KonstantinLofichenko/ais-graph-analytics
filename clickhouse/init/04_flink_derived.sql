-- Float64 JSON tokens are read as strings, then cast with precise_float_parsing
-- in the views: ClickHouse 26.3 JSON numeric-to-Float64 parsing can round by 1 ULP.
-- Persistent columns remain native Float64; no metrics are recalculated.
-- Additive derived-stream ingestion; the raw AIS pipeline is unchanged.
-- Reapplying this file creates missing objects without resetting data or offsets.
CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS analytics;


CREATE TABLE IF NOT EXISTS analytics.ais_vessel_features
(
    activity_date Date DEFAULT toDate(window_end),
    mmsi UInt32,
    name Nullable(String),
    window_minutes UInt16,
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    position_count UInt64,
    avg_speed Nullable(Float64),
    min_speed Nullable(Float64),
    max_speed Nullable(Float64),
    ship_type Nullable(UInt16),
    ship_type_name Nullable(String),
    ship_category Nullable(String),
    last_navigation_status Nullable(UInt8),
    last_navigation_status_name Nullable(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3),
    CONSTRAINT valid_mmsi CHECK mmsi BETWEEN 1 AND 999999999,
    CONSTRAINT valid_window CHECK window_minutes > 0 AND window_start < window_end,
    CONSTRAINT valid_count CHECK position_count > 0
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(window_start)
ORDER BY (mmsi, window_minutes, window_start);


-- DEFAULT expressions also derive dates for existing parts without rewriting history.
ALTER TABLE analytics.ais_vessel_features ADD COLUMN IF NOT EXISTS activity_date Date DEFAULT toDate(window_end) FIRST;

-- Existing history moved by 04_flink_derived.sh has no names; keep those NULL.
ALTER TABLE analytics.ais_vessel_features ADD COLUMN IF NOT EXISTS name Nullable(String) AFTER mmsi;

CREATE TABLE IF NOT EXISTS raw.ais_vessel_features_kafka
(
    mmsi UInt32,
    name Nullable(String),
    window_minutes UInt16,
    window_start String,
    window_end String,
    position_count UInt64,
    avg_speed Nullable(String),
    min_speed Nullable(String),
    max_speed Nullable(String),
    ship_type Nullable(UInt16),
    ship_type_name Nullable(String),
    ship_category Nullable(String),
    last_navigation_status Nullable(UInt8),
    last_navigation_status_name Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'ais-kafka:29092',
    kafka_topic_list = 'ais.vessel.features',
    kafka_group_name = 'clickhouse_ais_vessel_features',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 0,
    kafka_handle_error_mode = 'default',
    input_format_skip_unknown_fields = 0,
    input_format_null_as_default = 0,
    input_format_json_read_numbers_as_strings = 1;


CREATE MATERIALIZED VIEW IF NOT EXISTS raw.ais_vessel_features_mv
TO analytics.ais_vessel_features
AS
SELECT
    mmsi,
    name,
    window_minutes,
    parseDateTime64BestEffort(window_start, 3, 'UTC') AS window_start,
    parseDateTime64BestEffort(window_end, 3, 'UTC') AS window_end,
    position_count,
    if(isNull(avg_speed), NULL, toFloat64(ifNull(avg_speed, '0'))) AS avg_speed,
    if(isNull(min_speed), NULL, toFloat64(ifNull(min_speed, '0'))) AS min_speed,
    if(isNull(max_speed), NULL, toFloat64(ifNull(max_speed, '0'))) AS max_speed,
    ship_type,
    ship_type_name,
    ship_category,
    last_navigation_status,
    last_navigation_status_name,
    now64(3) AS ingested_at
FROM raw.ais_vessel_features_kafka
SETTINGS precise_float_parsing = 1;


CREATE TABLE IF NOT EXISTS analytics.ais_vessel_gap_events
(
    activity_date Nullable(Date) DEFAULT if(event_type = 'AIS_GAP_ENDED', toDate(gap_ended_at), toDate(gap_detected_at)),
    event_type LowCardinality(String),
    mmsi UInt32,
    name Nullable(String),
    last_event_msgtime Nullable(DateTime64(9, 'UTC')),
    previous_event_msgtime Nullable(DateTime64(9, 'UTC')),
    resumed_event_msgtime Nullable(DateTime64(9, 'UTC')),
    last_latitude Nullable(Float64),
    last_longitude Nullable(Float64),
    gap_detected_at DateTime64(3, 'UTC'),
    gap_ended_at Nullable(DateTime64(3, 'UTC')),
    gap_duration_seconds Nullable(Float64),
    gap_timeout_seconds Nullable(UInt32),
    ship_type Nullable(UInt16),
    ship_type_name Nullable(String),
    ship_category Nullable(String),
    navigation_status Nullable(UInt8),
    navigation_status_name Nullable(String),
    previous_navigation_status Nullable(UInt8),
    previous_navigation_status_name Nullable(String),
    resumed_navigation_status Nullable(UInt8),
    resumed_navigation_status_name Nullable(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3),
    CONSTRAINT valid_mmsi CHECK mmsi BETWEEN 1 AND 999999999,
    CONSTRAINT valid_event_type CHECK event_type IN ('AIS_GAP_DETECTED', 'AIS_GAP_ENDED')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(gap_detected_at)
ORDER BY (mmsi, gap_detected_at, event_type);


ALTER TABLE analytics.ais_vessel_gap_events ADD COLUMN IF NOT EXISTS activity_date Nullable(Date)
    DEFAULT if(event_type = 'AIS_GAP_ENDED', toDate(gap_ended_at), toDate(gap_detected_at)) FIRST;

CREATE TABLE IF NOT EXISTS raw.ais_vessel_gap_events_kafka
(
    event_type String,
    mmsi UInt32,
    name Nullable(String),
    last_event_msgtime Nullable(String),
    previous_event_msgtime Nullable(String),
    resumed_event_msgtime Nullable(String),
    last_latitude Nullable(String),
    last_longitude Nullable(String),
    gap_detected_at String,
    gap_ended_at Nullable(String),
    gap_duration_seconds Nullable(String),
    gap_timeout_seconds Nullable(UInt32),
    ship_type Nullable(UInt16),
    ship_type_name Nullable(String),
    ship_category Nullable(String),
    navigation_status Nullable(UInt8),
    navigation_status_name Nullable(String),
    previous_navigation_status Nullable(UInt8),
    previous_navigation_status_name Nullable(String),
    resumed_navigation_status Nullable(UInt8),
    resumed_navigation_status_name Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'ais-kafka:29092',
    kafka_topic_list = 'ais.vessel.gaps',
    kafka_group_name = 'clickhouse_ais_vessel_gaps',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 0,
    kafka_handle_error_mode = 'default',
    input_format_skip_unknown_fields = 0,
    input_format_null_as_default = 0,
    input_format_json_read_numbers_as_strings = 1;


-- The safe parser input prevents eager evaluation of a NULL String
-- as an empty string. The outer if preserves NULL; invalid non-null dates fail.
CREATE MATERIALIZED VIEW IF NOT EXISTS raw.ais_vessel_gap_events_mv
TO analytics.ais_vessel_gap_events
AS
SELECT
    event_type,
    mmsi,
    name,
    if(isNull(last_event_msgtime), NULL,
       parseDateTime64BestEffort(ifNull(last_event_msgtime, '1970-01-01T00:00:00Z'), 9, 'UTC')) AS last_event_msgtime,
    if(isNull(previous_event_msgtime), NULL,
       parseDateTime64BestEffort(ifNull(previous_event_msgtime, '1970-01-01T00:00:00Z'), 9, 'UTC')) AS previous_event_msgtime,
    if(isNull(resumed_event_msgtime), NULL,
       parseDateTime64BestEffort(ifNull(resumed_event_msgtime, '1970-01-01T00:00:00Z'), 9, 'UTC')) AS resumed_event_msgtime,
    if(isNull(last_latitude), NULL, toFloat64(ifNull(last_latitude, '0'))) AS last_latitude,
    if(isNull(last_longitude), NULL, toFloat64(ifNull(last_longitude, '0'))) AS last_longitude,
    parseDateTime64BestEffort(gap_detected_at, 3, 'UTC') AS gap_detected_at,
    if(isNull(gap_ended_at), NULL,
       parseDateTime64BestEffort(ifNull(gap_ended_at, '1970-01-01T00:00:00Z'), 3, 'UTC')) AS gap_ended_at,
    if(isNull(gap_duration_seconds), NULL, toFloat64(ifNull(gap_duration_seconds, '0'))) AS gap_duration_seconds,
    gap_timeout_seconds,
    ship_type,
    ship_type_name,
    ship_category,
    navigation_status,
    navigation_status_name,
    previous_navigation_status,
    previous_navigation_status_name,
    resumed_navigation_status,
    resumed_navigation_status_name,
    now64(3) AS ingested_at
FROM raw.ais_vessel_gap_events_kafka
SETTINGS precise_float_parsing = 1;
