{{ config(
    materialized='view'
) }}

with ranked as (

    select
        mmsi,
        date_time_utc as msgtime,
        latitude,
        longitude,

        cast(speed_over_ground as Nullable(Float32))
            as speed_over_ground,

        cast(
            nullIf(course_over_ground, 360)
            as Nullable(Float32)
        ) as course_over_ground,

        cast(
            nullIf(true_heading, 511)
            as Nullable(UInt16)
        ) as true_heading,

        cast(
            if(rate_of_turn in (-128, -127, 127), null, rate_of_turn)
            as Nullable(Float32)
        ) as rate_of_turn,

        cast(status as Nullable(UInt8))
            as navigational_status,

        cast(null as Nullable(UInt16))
            as ship_type,

        cast(null as Nullable(String))
            as name,

        cast('hais' as Nullable(String))
            as stream,

        row_number() over (
            partition by mmsi, date_time_utc
            order by
                (true_heading != 511) desc,
                (course_over_ground != 360) desc,
                (rate_of_turn not in (-128, -127, 127)) desc,
                data_source,
                ais_class,
                msg_type,
                longitude,
                latitude
        ) as rn

    from {{ source('raw', 'hais_positions') }}

)

select
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

from ranked
where rn = 1