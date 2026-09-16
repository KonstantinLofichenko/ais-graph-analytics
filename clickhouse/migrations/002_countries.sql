-- Manual migration: add the country reference dimension for analytics.ports.country.
-- This script does not run automatically from Docker startup.
CREATE DATABASE IF NOT EXISTS analytics;

-- country_code is ISO 3166-1 alpha-2; iso3 is ISO 3166-1 alpha-3.
-- numeric_code uses FixedString(3) because ISO numeric codes can contain leading zeroes.
CREATE TABLE IF NOT EXISTS analytics.countries
(
    country_code String,
    iso3 String,
    numeric_code FixedString(3),
    country_name String,
    region Nullable(String),
    capital_city Nullable(String),
    updated_at DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY country_code;

-- The manual Norway seed is temporary bootstrap/reference data.
-- Planned production flow:
-- public country API -> Airbyte raw ingestion -> dbt transformation -> analytics.countries.
-- Insert only if Norway is absent, including before background merges (FINAL).
-- Reruns preserve an existing Norway row instead of inserting another version.
INSERT INTO analytics.countries
(
    country_code,
    iso3,
    numeric_code,
    country_name,
    region,
    capital_city
)
SELECT
    'NO',
    'NOR',
    '578',
    'Norway',
    'Europe',
    'Oslo'
WHERE NOT EXISTS
(
    SELECT 1
    FROM analytics.countries FINAL
    WHERE country_code = 'NO'
);
