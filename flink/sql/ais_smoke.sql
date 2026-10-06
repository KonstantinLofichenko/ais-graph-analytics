SET 'sql-client.execution.result-mode' = 'TABLEAU';

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
    'properties.group.id' = 'ais-flink-smoke',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

SELECT
    mmsi,
    msgtime,
    latitude,
    longitude,
    speedOverGround,
    UPPER(`stream`) AS stream_upper
FROM ais_positions
WHERE mmsi IS NOT NULL;