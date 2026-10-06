CREATE TABLE ais_positions (
    mmsi BIGINT,
    msgtime STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    speedOverGround DOUBLE,
    `stream` STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'ais.positions',
    'properties.bootstrap.servers' = 'ais-kafka:29092',
    'properties.group.id' = 'ais-flink-transform',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE ais_positions_flink (
    mmsi BIGINT,
    msgtime STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    speedOverGround DOUBLE,
    stream_upper STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'ais.positions.flink',
    'properties.bootstrap.servers' = 'ais-kafka:29092',
    'format' = 'json'
);

INSERT INTO ais_positions_flink
SELECT
    mmsi,
    msgtime,
    latitude,
    longitude,
    speedOverGround,
    UPPER(`stream`) AS stream_upper
FROM ais_positions
WHERE mmsi IS NOT NULL;