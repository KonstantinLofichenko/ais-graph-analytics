select
    mmsi,

    argMaxIf(
        trim(name),
        msgtime,
        name is not null and trim(name) != ''
    ) as vessel_name,

    argMaxIf(
        ship_type,
        msgtime,
        ship_type is not null
    ) as ship_type,

    min(msgtime) as first_seen,
    max(msgtime) as last_seen,

    uniqExactIf(
        trim(name),
        name is not null and trim(name) != ''
    ) as name_count,

    count() as ais_message_count,

    avgIf(
        speed_over_ground,
        speed_over_ground is not null
        and speed_over_ground < {{ var('ais_max_valid_speed_kn') }}
    ) as avg_speed_kn,

    maxIf(
        speed_over_ground,
        speed_over_ground is not null
        and speed_over_ground < {{ var('ais_max_valid_speed_kn') }}
    ) as max_speed_kn,

    argMaxIf(
        speed_over_ground,
        msgtime,
        speed_over_ground is not null
        and speed_over_ground < {{ var('ais_max_valid_speed_kn') }}
    ) as last_speed_kn,

    argMax(latitude, msgtime) as last_latitude,
    argMax(longitude, msgtime) as last_longitude,

    argMaxIf(
        course_over_ground,
        msgtime,
        course_over_ground is not null
    ) as last_course,

    argMaxIf(
        true_heading,
        msgtime,
        true_heading is not null
    ) as last_heading,

    argMaxIf(
        navigational_status,
        msgtime,
        navigational_status is not null
    ) as last_navigational_status

from {{ source('raw', 'ais_positions') }} final

where msgtime >= toDateTime64(
    '{{ var("ais_analysis_start_date") }}',
    9,
    'UTC'
)

group by mmsi