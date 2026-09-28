{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(activity_date, mmsi)'
) }}

with source_points as (

    select
        mmsi,
        msgtime,
        ingested_at,
        stream,

        toDate(msgtime) as activity_date,

        speed_over_ground,
        navigational_status,

        lagInFrame(
            navigational_status,
            1,
            cast(null as Nullable(UInt8))
        ) over (
            partition by
                mmsi,
                toDate(msgtime)
            order by
                msgtime,
                ingested_at,
                stream
            rows between unbounded preceding and current row
        ) as previous_navigational_status

    from {{ source('raw', 'ais_positions') }} final

    where msgtime >= toDateTime64(
        '{{ var("ais_analysis_start_date") }}',
        9,
        'UTC'
    )
),

/*
    Count how often each navigational status was reported
    for each vessel/day.
*/
status_counts as (

    select
        mmsi,
        activity_date,
        navigational_status,
        count() as status_observations

    from source_points

    where navigational_status is not null

    group by
        mmsi,
        activity_date,
        navigational_status
),

/*
    Determine the dominant navigational status for the day.
*/
dominant_status as (

    select
        mmsi,
        activity_date,

        argMax(
            navigational_status,
            status_observations
        ) as dominant_navigational_status,

        max(status_observations) as dominant_status_observations,

        sum(status_observations) as total_status_observations

    from status_counts

    group by
        mmsi,
        activity_date
),

daily as (

    select
        mmsi,
        activity_date,

        count() as ais_points,

        min(msgtime) as first_seen,
        max(msgtime) as last_seen,

        round(
            dateDiff(
                'second',
                min(msgtime),
                max(msgtime)
            ) / 3600.0,
            2
        ) as observation_hours,

        /*
            Speed statistics exclude AIS sentinel values
            102.2 knots and above.
        */
        round(
            avgIf(
                speed_over_ground,
                speed_over_ground is not null
                and speed_over_ground
                    < {{ var('ais_max_valid_speed_kn') }}
            ),
            2
        ) as avg_speed_kn,

        round(
            maxIf(
                speed_over_ground,
                speed_over_ground is not null
                and speed_over_ground
                    < {{ var('ais_max_valid_speed_kn') }}
            ),
            2
        ) as max_speed_kn,

        /*
            This is based on the percentage of AIS observations,
            not percentage of elapsed time.
        */
        round(
            100.0
            * countIf(
                speed_over_ground is not null
                and speed_over_ground
                    < {{ var('ais_max_valid_speed_kn') }}
                and speed_over_ground
                    < {{ var('ais_stationary_speed_kn') }}
            )
            / nullIf(
                countIf(
                    speed_over_ground is not null
                    and speed_over_ground
                        < {{ var('ais_max_valid_speed_kn') }}
                ),
                0
            ),
            1
        ) as stationary_observation_pct,

        /*
            Last reported navigational status for the vessel/day.
        */
        argMaxIf(
            navigational_status,
            tuple(msgtime, ingested_at),
            navigational_status is not null
        ) as last_navigational_status,

        /*
            Number of adjacent AIS observations for which
            both statuses are known.
        */
        countIf(
            navigational_status is not null
            and previous_navigational_status is not null
        ) as nav_status_comparable_pairs,

        /*
            Number of adjacent observations whose status changed.
            This is a data signal, not necessarily a real
            operational vessel-state transition.
        */
        countIf(
            navigational_status is not null
            and previous_navigational_status is not null
            and navigational_status
                != previous_navigational_status
        ) as nav_status_changes

    from source_points

    group by
        mmsi,
        activity_date
),

daily_with_status_metrics as (

    select
        d.*,

        ds.dominant_navigational_status,

        round(
            100.0
            * ds.dominant_status_observations
            / nullIf(ds.total_status_observations, 0),
            1
        ) as dominant_status_pct,

        round(
            100.0
            * d.nav_status_changes
            / nullIf(d.nav_status_comparable_pairs, 0),
            1
        ) as nav_status_change_rate_pct

    from daily d

    left join dominant_status ds
        on d.mmsi = ds.mmsi
        and d.activity_date = ds.activity_date
)

select
    d.mmsi as mmsi,
    d.activity_date as activity_date,

    v.vessel_name as vessel_name,
    v.ship_type_name as ship_type_name,
    v.ship_category as ship_category,

    d.ais_points as ais_points,
    d.first_seen as first_seen,
    d.last_seen as last_seen,
    d.observation_hours as observation_hours,

    d.avg_speed_kn as avg_speed_kn,
    d.max_speed_kn as max_speed_kn,
    d.stationary_observation_pct as stationary_observation_pct,

    d.last_navigational_status as last_navigational_status,

    coalesce(
        nullIf(last_status.navigational_status_name, ''),
        'Unknown'
    ) as last_navigational_status_name,

    d.dominant_navigational_status as dominant_navigational_status,

    coalesce(
        nullIf(dominant_status_lookup.navigational_status_name, ''),
        'Unknown'
    ) as dominant_navigational_status_name,

    d.dominant_status_pct as dominant_status_pct,

    d.nav_status_changes as nav_status_changes,
    d.nav_status_change_rate_pct as nav_status_change_rate_pct

from daily_with_status_metrics d

inner join {{ ref('vessels') }} v
    on d.mmsi = v.mmsi

left join {{ ref('ais_navigational_status') }} last_status
    on d.last_navigational_status
        = last_status.navigational_status

left join {{ ref('ais_navigational_status') }} dominant_status_lookup
    on d.dominant_navigational_status
        = dominant_status_lookup.navigational_status

where v.is_ai_enrichment_eligible = 1