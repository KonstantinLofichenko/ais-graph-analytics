-- Run manually after migrations/004_hais_positions.sql.
-- The file must already be in the ClickHouse server's user_files directory.
-- Explicit schema bypasses inference of the unsupported GeoParquet geometry type.
-- Preserve source values and duplicates; each execution appends the sample again.
INSERT INTO raw.hais_positions
(
    date_time_utc,
    mmsi,
    longitude,
    latitude,
    status,
    course_over_ground,
    true_heading,
    speed_over_ground,
    rate_of_turn,
    maneuvre,
    data_source,
    ais_class,
    msg_type
)
SELECT
    date_time_utc,
    mmsi,
    longitude,
    latitude,
    status,
    course_over_ground,
    true_heading,
    speed_over_ground,
    rate_of_turn,
    maneuvre,
    data_source,
    ais_class,
    msg_type
FROM file(
    'hais_2026-09-01.snappy.parquet',
    'Parquet',
    'date_time_utc DateTime64(6, \'UTC\'),
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
     msg_type Nullable(Int32)'
);
