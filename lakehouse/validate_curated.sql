-- Read-only validation for the full-month milestone dataset.
SELECT 'raw' AS layer, count(*) AS rows FROM iceberg.hais_raw.ais_positions
UNION ALL
SELECT 'curated', count(*) FROM iceberg.hais_curated.ais_positions;

SELECT count_if(date_time IS NULL OR mmsi IS NULL OR longitude IS NULL OR
    latitude IS NULL OR navigational_status IS NULL OR maneuver IS NULL OR
    data_source IS NULL OR ais_class IS NULL OR message_type IS NULL OR
    geometry IS NULL) AS required_null_rows,
    count_if(course_over_ground IS NULL) AS course_nulls,
    count_if(true_heading IS NULL) AS heading_nulls,
    count_if(speed_over_ground IS NULL) AS speed_nulls,
    count_if(rate_of_turn IS NULL) AS turn_nulls,
    count_if(course_over_ground < 0 OR course_over_ground >= 360) AS invalid_course,
    count_if(true_heading = 511) AS heading_sentinels,
    count_if(speed_over_ground = DECIMAL '102.30') AS speed_sentinels,
    count_if(rate_of_turn = -128) AS turn_sentinels
FROM iceberg.hais_curated.ais_positions;

SHOW CREATE TABLE iceberg.hais_curated.ais_positions;
SELECT partition, record_count
FROM iceberg.hais_curated."ais_positions$partitions" ORDER BY partition;

SELECT CAST(date_time AS DATE) AS activity_date, count(*) AS positions,
    count(DISTINCT mmsi) AS vessels, avg(speed_over_ground) AS average_speed
FROM iceberg.hais_curated.ais_positions GROUP BY 1 ORDER BY 1;
