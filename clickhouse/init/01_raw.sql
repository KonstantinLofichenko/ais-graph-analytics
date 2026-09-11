CREATE DATABASE IF NOT EXISTS raw;


CREATE TABLE IF NOT EXISTS raw.ais_positions_kafka
(
    mmsi UInt32,
    msgtime String,

    latitude Float64,
    longitude Float64,

    speedOverGround Nullable(Float32),
    courseOverGround Nullable(Float32),
    trueHeading Nullable(UInt16),
    rateOfTurn Nullable(Float32),

    navigationalStatus Nullable(UInt8),
    shipType Nullable(UInt16),

    name Nullable(String),
    stream Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'ais-kafka:29092',
    kafka_topic_list = 'ais.positions',
    kafka_group_name = 'clickhouse_ais_positions',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;


CREATE MATERIALIZED VIEW IF NOT EXISTS raw.ais_positions_mv
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


CREATE TABLE IF NOT EXISTS raw.ais_positions
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
ENGINE = MergeTree
PARTITION BY toYYYYMM(msgtime)
ORDER BY (mmsi, msgtime);
