CREATE DATABASE IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.countries
(
    `_airbyte_raw_id` String,
    `_airbyte_extracted_at` DateTime64(3),
    `_airbyte_meta` String,
    `_airbyte_generation_id` UInt32,
    `_meta` Nullable(String),
    `codes` Nullable(String),
    `names` Nullable(String),
    `region` Nullable(String),
    `capitals` Nullable(String),
    `contry_code` Nullable(String)
)
ENGINE = MergeTree
ORDER BY _airbyte_raw_id
SETTINGS index_granularity = 8192;

TRUNCATE TABLE raw.countries;

INSERT INTO raw.countries
(
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta,
    _airbyte_generation_id,
    _meta,
    codes,
    names,
    region,
    capitals,
    contry_code
)
VALUES
(
    'ci-norway',
    toDateTime64('2026-01-01 00:00:00', 3),
    '{}',
    1,
    NULL,
    '{"alpha_2":"NO","alpha_3":"NOR","ccn3":"578"}',
    '{"common":"Norway"}',
    'Europe',
    '[{"attributes":{"administrative":false,"constitutional":false,"executive":false,"judicial":false,"legislative":false,"primary":true},"coordinates":{"lat":59.92,"lng":10.75},"name":"Oslo"}]',
    'NO'
);
