{{ config(materialized='view') }}

-- This is the existing Python input SELECT, shared with presentation freshness.
with candidates as (
SELECT
            mmsi AS mmsi,
            activity_date AS activity_date,
            vessel_name AS vessel_name,
            ship_type_name AS ship_type_name,
            ship_category AS ship_category,

            ais_points AS ais_points,

            round(
                toFloat64(observation_hours),
                2
            ) AS observation_hours,

            round(
                toFloat64(avg_speed_kn),
                2
            ) AS avg_speed_kn,

            round(
                toFloat64(max_speed_kn),
                2
            ) AS max_speed_kn,

            round(
                toFloat64(stationary_observation_pct),
                1
            ) AS stationary_observation_pct,

            dominant_navigational_status_name
                AS dominant_navigational_status_name,

            round(
                toFloat64(dominant_status_pct),
                1
            ) AS dominant_status_pct,

            round(
                toFloat64(nav_status_change_rate_pct),
                1
            ) AS nav_status_change_rate_pct,

            baseline_days AS baseline_days,
            baseline_avg_speed_kn AS baseline_avg_speed_kn,
            baseline_sd_speed_kn AS baseline_sd_speed_kn,
            speed_deviation_kn AS speed_deviation_kn,
            speed_anomaly_threshold_kn AS speed_anomaly_threshold_kn,
            speed_anomaly AS speed_anomaly,

            baseline_stationary_pct AS baseline_stationary_pct,
            baseline_sd_stationary_pct AS baseline_sd_stationary_pct,
            stationary_deviation_pct AS stationary_deviation_pct,
            stationary_anomaly_threshold_pct AS stationary_anomaly_threshold_pct,
            stationary_anomaly AS stationary_anomaly,

            status_speed_mismatch AS status_speed_mismatch,
            navigation_status_inconsistent AS navigation_status_inconsistent,
            anomaly_reason AS anomaly_reason,
            anomaly_score AS anomaly_score,
            anomaly_rank AS anomaly_rank

        FROM {{ ref('vessel_daily_anomalies') }}
)
select
    {% for field in ai_input_fields() %}
    c.{{ field }} AS {{ field }},
    {% endfor %}
    -- Exact typed bytes, not a second hash algorithm or Python JSON serializer.
    -- Include field names and types so schema changes cannot reuse an old mapping.
    toJSONString(tuple(
        {% for field in ai_input_fields() %}
        tuple('{{ field }}', toTypeName(c.{{ field }}), hex(c.{{ field }})){{ ',' if not loop.last }}
        {% endfor %}
    )) AS input_values
from candidates AS c
where c.anomaly_rank <= 100
