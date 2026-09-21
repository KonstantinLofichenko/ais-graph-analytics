{% macro load_hais_to_ais_positions(start_date, end_date) %}

    {% set sql %}

        INSERT INTO {{ source('raw', 'ais_positions') }}
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
            stream
        )

        SELECT
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
            stream

        FROM {{ ref('stg_hais_positions') }}

        WHERE msgtime >= toDateTime64(
            '{{ start_date }} 00:00:00',
            9,
            'UTC'
        )
        AND msgtime < toDateTime64(
            '{{ end_date }} 00:00:00',
            9,
            'UTC'
        )

    {% endset %}

    {% do run_query(sql) %}

{% endmacro %}