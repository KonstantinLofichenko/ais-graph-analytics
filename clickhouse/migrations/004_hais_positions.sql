-- Manual migration: source-faithful HAIS historical AIS data from GeoParquet.
CREATE DATABASE IF NOT EXISTS raw;

-- Preserve all rows, including exact duplicates; deduplication belongs in staging.
-- (mmsi, date_time_utc) is not unique: simultaneous observations can differ.
-- geometry is omitted: longitude/latitude suffice and avoid GeoParquet type inference issues.
-- Keep source field names and AIS sentinel values unchanged.
CREATE TABLE IF NOT EXISTS raw.hais_positions
(
    date_time_utc DateTime64(6, 'UTC'),
    mmsi UInt32,
    longitude Float64,
    latitude Float64,
    status Nullable(Int32),
    course_over_ground Nullable(Float64),
    true_heading Nullable(Int32),
    speed_over_ground Nullable(Float64),
    rate_of_turn Nullable(Float64),
    maneuvre Nullable(Int32),
    data_source Nullable(String),
    ais_class Nullable(String),
    msg_type Nullable(Int32),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date_time_utc)
-- Sort vessel observations chronologically; this is not a uniqueness constraint.
ORDER BY (mmsi, date_time_utc);
