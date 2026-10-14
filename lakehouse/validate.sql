-- Disposable fixture: rerunning replaces only this validation table.
CREATE SCHEMA IF NOT EXISTS iceberg.validation
WITH (location = 's3://warehouse/validation/');
DROP TABLE IF EXISTS iceberg.validation.foundation_smoke;
CREATE TABLE iceberg.validation.foundation_smoke (
    mmsi BIGINT,
    vessel_name VARCHAR
) WITH (format = 'PARQUET');
INSERT INTO iceberg.validation.foundation_smoke
VALUES (257000001, 'Iceberg One'), (257000002, 'Iceberg Two');
SELECT * FROM iceberg.validation.foundation_smoke ORDER BY mmsi;
SELECT count(*) AS row_count FROM iceberg.validation.foundation_smoke;
SELECT file_path, record_count
FROM iceberg.validation."foundation_smoke$files";
