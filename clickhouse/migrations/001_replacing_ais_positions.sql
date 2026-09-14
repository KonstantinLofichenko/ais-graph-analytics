-- Manual migration: stop ais-producer manually before running this script.
-- Drop the materialized view first to stop ClickHouse Kafka ingestion before copying.
-- This script intentionally does not run from Docker startup.

-- Freeze the source table before inspecting or copying its rows.
DROP VIEW raw.ais_positions_mv;

-- Inspect the source before copying. The two counts should explain the duplicate set.
SELECT
    count() AS source_rows,
    uniqExact((mmsi, msgtime)) AS logical_events
FROM raw.ais_positions;

-- Create a replacement with logical deduplication by (mmsi, msgtime).
-- argMax keeps every payload field from the row with the greatest ingested_at.
CREATE TABLE raw.ais_positions_replacing
(
    mmsi UInt32,
    msgtime DateTime64(9, 'UTC'),
    latitude Float64,
    longitude Float64,
    speed_over_ground Nullable(Float32),
    course_over_ground Nullable(Float32),
    true_heading Nullable(UInt16),
    rate_of_turn Nullable(Float32),
    navigational_status Nullable(UInt8),
    ship_type Nullable(UInt16),
    name Nullable(String),
    stream Nullable(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(msgtime)
ORDER BY (mmsi, msgtime);

INSERT INTO raw.ais_positions_replacing
(
    mmsi,
    msgtime,
    latitude,
    longitude,
    speed_over_ground,
    course_over_ground,
    true_heading,
    rate_of_turn,
    navigational_status,
    ship_type,
    name,
    stream,
    ingested_at
)
SELECT
    mmsi,
    msgtime,
    tupleElement(winning, 1),
    tupleElement(winning, 2),
    tupleElement(winning, 3),
    tupleElement(winning, 4),
    tupleElement(winning, 5),
    tupleElement(winning, 6),
    tupleElement(winning, 7),
    tupleElement(winning, 8),
    tupleElement(winning, 9),
    tupleElement(winning, 10),
    tupleElement(winning, 11)
FROM
(
    SELECT
        mmsi,
        msgtime,
        argMax(
            tuple(
                latitude,
                longitude,
                speed_over_ground,
                course_over_ground,
                true_heading,
                rate_of_turn,
                navigational_status,
                ship_type,
                name,
                stream,
                ingested_at
            ),
            ingested_at
        ) AS winning
    FROM raw.ais_positions
    GROUP BY mmsi, msgtime
);

-- Confirm the replacement has one row per logical event before swapping names.
SELECT
    count() AS replacement_physical_rows,
    uniqExact((mmsi, msgtime)) AS replacement_logical_events
FROM raw.ais_positions_replacing FINAL;

-- Keep the original table for inspection or rollback; do not drop it automatically.
RENAME TABLE
    raw.ais_positions TO raw.ais_positions_merge_backup,
    raw.ais_positions_replacing TO raw.ais_positions;

-- Recreate the view so it writes to the new ReplacingMergeTree table.
CREATE MATERIALIZED VIEW raw.ais_positions_mv
TO raw.ais_positions
AS
SELECT
    mmsi,
    parseDateTime64BestEffort(msgtime, 9, 'UTC') AS msgtime,
    latitude,
    longitude,
    speedOverGround AS speed_over_ground,
    courseOverGround AS course_over_ground,
    trueHeading AS true_heading,
    rateOfTurn AS rate_of_turn,
    navigationalStatus AS navigational_status,
    shipType AS ship_type,
    name,
    stream
FROM raw.ais_positions_kafka;

-- Physical duplicates may remain before background merges; FINAL shows logical rows.
SELECT
    count() AS physical_rows,
    uniqExact((mmsi, msgtime)) AS logical_events
FROM raw.ais_positions FINAL;

SELECT
    mmsi,
    msgtime,
    count()
FROM raw.ais_positions
GROUP BY mmsi, msgtime
HAVING count() > 1
ORDER BY count() DESC
LIMIT 20;
