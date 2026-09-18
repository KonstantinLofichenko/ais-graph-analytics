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
    ) as name_count

from {{ source('raw', 'ais_positions') }} final

group by mmsi
