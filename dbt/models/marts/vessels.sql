{{ config(
    engine='MergeTree()',
    order_by='mmsi'
) }}

with vessel_base as (

select
    assumeNotNull(v.mmsi) as mmsi,

    v.vessel_name as vessel_name,

    v.ship_type as ship_type,
    coalesce(
        nullIf(trim(st.ship_type_name), ''),
        'Unknown'
    ) as ship_type_name,

    coalesce(
        nullIf(trim(st.ship_category), ''),
        'Unknown'
    ) as ship_category,

    v.first_seen as first_seen,
    v.last_seen as last_seen,

    v.name_count as name_count,
    v.ais_message_count as ais_message_count,

    v.avg_speed_kn as avg_speed_kn,
    v.max_speed_kn as max_speed_kn,

    v.last_latitude as last_latitude,
    v.last_longitude as last_longitude,
    v.last_speed_kn as last_speed_kn,
    v.last_course as last_course,
    v.last_heading as last_heading,

    v.last_navigational_status as last_navigational_status,
    coalesce(
        ns.navigational_status_name,
        'Unknown'
    ) as last_navigational_status_name,

    assumeNotNull(length(toString(mmsi)) = 9) as is_valid_mmsi,

    assumeNotNull(name_count > 1) as has_identity_conflict,

    (
        v.ship_type is not null
        and empty(trim(coalesce(st.ship_type_name, '')))
    ) as has_unknown_ship_type,

    assumeNotNull(
        positionCaseInsensitive(vessel_name, 'test') > 0
    ) as is_test_record,

    (
        length(toString(v.mmsi)) = 9
        and positionCaseInsensitive(
            coalesce(v.vessel_name, ''),
            'test'
        ) = 0
    ) as is_dashboard_eligible

from {{ ref('stg_vessels') }} v

left join {{ ref('ais_ship_types') }} st
    on v.ship_type = st.ship_type

left join {{ ref('ais_navigational_status') }} ns
    on v.last_navigational_status = ns.navigational_status
)

, vessel_classified as (

    select
        vb.*,

        {{ mmsi_format_class('vb.mmsi') }}
            as mmsi_format_class

    from vessel_base as vb
)

select
    vc.*,

    (
        vc.is_dashboard_eligible
        and vc.mmsi_format_class = 'ship'
    ) as is_ai_enrichment_eligible

from vessel_classified as vc