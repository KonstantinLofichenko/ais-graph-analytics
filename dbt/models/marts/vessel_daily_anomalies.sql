{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(activity_date, anomaly_rank, mmsi)'
) }}

with features as (

    select
        mmsi as mmsi,
        activity_date as activity_date,

        vessel_name as vessel_name,
        ship_type_name as ship_type_name,
        ship_category as ship_category,

        ais_points as ais_points,
        observation_hours as observation_hours,

        avg_speed_kn as avg_speed_kn,
        max_speed_kn as max_speed_kn,
        stationary_observation_pct as stationary_observation_pct,

        dominant_navigational_status_name
            as dominant_navigational_status_name,

        dominant_status_pct as dominant_status_pct,

        nav_status_change_rate_pct
            as nav_status_change_rate_pct

    from {{ ref('vessel_daily_features') }}
),

/*
    Only sufficiently observed vessel-days should be used
    as anomaly targets.
*/
target_features as (

    select
        f.mmsi as mmsi,
        f.activity_date as activity_date,

        f.vessel_name as vessel_name,
        f.ship_type_name as ship_type_name,
        f.ship_category as ship_category,

        f.ais_points as ais_points,
        f.observation_hours as observation_hours,

        f.avg_speed_kn as avg_speed_kn,
        f.max_speed_kn as max_speed_kn,

        f.stationary_observation_pct
            as stationary_observation_pct,

        f.dominant_navigational_status_name
            as dominant_navigational_status_name,

        f.dominant_status_pct
            as dominant_status_pct,

        f.nav_status_change_rate_pct
            as nav_status_change_rate_pct

    from features as f

    where f.observation_hours >= 12
      and f.ais_points >= 100
),

/*
    Use only good-quality historical days in the baseline.
*/
baseline_source as (

    select
        f.mmsi as mmsi,
        f.activity_date as activity_date,

        f.avg_speed_kn as avg_speed_kn,

        f.stationary_observation_pct
            as stationary_observation_pct

    from features as f

    where f.observation_hours >= 12
      and f.ais_points >= 100
),

/*
    Each historical vessel-day contributes to the next
    seven calendar days.

    This avoids a non-equality range JOIN and works well
    with ClickHouse 26.3.
*/
baseline_expanded as (

    select
        b.mmsi as mmsi,

        addDays(
            b.activity_date,
            toInt32(day_offset)
        ) as target_activity_date,

        b.avg_speed_kn as avg_speed_kn,

        b.stationary_observation_pct
            as stationary_observation_pct

    from baseline_source as b

    array join range(1, 8) as day_offset
),

baseline as (

    select
        be.mmsi as mmsi,

        be.target_activity_date
            as activity_date,

        count() as baseline_days,

        countIf(
            be.avg_speed_kn is not null
        ) as baseline_speed_days,

        countIf(
            be.stationary_observation_pct is not null
        ) as baseline_stationary_days,

        avg(
            toFloat64(be.avg_speed_kn)
        ) as baseline_avg_speed_kn,

        stddevPop(
            toFloat64(be.avg_speed_kn)
        ) as baseline_sd_speed_kn,

        avg(
            toFloat64(be.stationary_observation_pct)
        ) as baseline_stationary_pct,

        stddevPop(
            toFloat64(be.stationary_observation_pct)
        ) as baseline_sd_stationary_pct

    from baseline_expanded as be

    group by
        be.mmsi,
        be.target_activity_date
),

with_baseline as (

    select
        t.mmsi as mmsi,
        t.activity_date as activity_date,

        t.vessel_name as vessel_name,
        t.ship_type_name as ship_type_name,
        t.ship_category as ship_category,

        t.ais_points as ais_points,
        t.observation_hours as observation_hours,

        t.avg_speed_kn as avg_speed_kn,
        t.max_speed_kn as max_speed_kn,

        t.stationary_observation_pct
            as stationary_observation_pct,

        t.dominant_navigational_status_name
            as dominant_navigational_status_name,

        t.dominant_status_pct
            as dominant_status_pct,

        t.nav_status_change_rate_pct
            as nav_status_change_rate_pct,

        b.baseline_days as baseline_days,
        b.baseline_speed_days as baseline_speed_days,
        b.baseline_stationary_days
            as baseline_stationary_days,

        b.baseline_avg_speed_kn
            as baseline_avg_speed_kn,

        b.baseline_sd_speed_kn
            as baseline_sd_speed_kn,

        b.baseline_stationary_pct
            as baseline_stationary_pct,

        b.baseline_sd_stationary_pct
            as baseline_sd_stationary_pct

    from target_features as t

    left join baseline as b
        on t.mmsi = b.mmsi
        and t.activity_date = b.activity_date
),

deviations as (

    select
        wb.*,

        abs(
            toFloat64(wb.avg_speed_kn)
            - wb.baseline_avg_speed_kn
        ) as speed_deviation_kn,

        greatest(
            3.0,
            2.0 * wb.baseline_sd_speed_kn
        ) as speed_anomaly_threshold_kn,

        abs(
            toFloat64(wb.stationary_observation_pct)
            - wb.baseline_stationary_pct
        ) as stationary_deviation_pct,

        greatest(
            30.0,
            2.0 * wb.baseline_sd_stationary_pct
        ) as stationary_anomaly_threshold_pct

    from with_baseline as wb
),

flags as (

    select
        d.*,

        (
            d.baseline_speed_days >= 3
            and d.avg_speed_kn is not null
            and d.speed_deviation_kn
                >= d.speed_anomaly_threshold_kn
        ) as speed_anomaly,

        (
            d.baseline_stationary_days >= 3
            and d.stationary_observation_pct is not null
            and d.stationary_deviation_pct
                >= d.stationary_anomaly_threshold_pct
        ) as stationary_anomaly,

    (
        d.dominant_status_pct >= 85
        and
            (
                (
                    d.dominant_navigational_status_name
                        in ('At anchor', 'Moored')
                    and d.stationary_observation_pct < 40
                    and d.avg_speed_kn >= 3
                )
                or
                (
                    d.dominant_navigational_status_name
                        = 'Under way using engine'
                    and d.stationary_observation_pct >= 90
                    and d.avg_speed_kn < 1
                )
            )
        ) as status_speed_mismatch,

        (
            d.nav_status_change_rate_pct >= 30
            and d.dominant_status_pct <= 70
        ) as navigation_status_inconsistent

    from deviations as d
),

severity as (

    select
        f.*,

        if(
            f.speed_anomaly,
            least(
                2.0,
                f.speed_deviation_kn
                / nullIf(
                    f.speed_anomaly_threshold_kn,
                    0
                )
            ),
            0.0
        ) as speed_anomaly_severity,

        if(
            f.stationary_anomaly,
            least(
                2.0,
                f.stationary_deviation_pct
                / nullIf(
                    f.stationary_anomaly_threshold_pct,
                    0
                )
            ),
            0.0
        ) as stationary_anomaly_severity

    from flags as f
),

scored as (

    select
        s.*,

        (
            s.speed_anomaly
            or s.stationary_anomaly
        ) as is_behavior_anomaly,

        multiIf(
            s.speed_anomaly
                and s.stationary_anomaly,
            'speed_and_stationary',

            s.speed_anomaly,
            'speed',

            s.stationary_anomaly,
            'stationary',

            'none'
        ) as anomaly_reason,

        round(
            (
                if(
                    s.speed_anomaly,
                    s.speed_deviation_kn
                    / nullIf(
                        s.speed_anomaly_threshold_kn,
                        0
                    ),
                    0.0
                )

                +

                if(
                    s.stationary_anomaly,
                    s.stationary_deviation_pct
                    / nullIf(
                        s.stationary_anomaly_threshold_pct,
                        0
                    ),
                    0.0
                )

                +

                if(
                    s.status_speed_mismatch,
                    0.25,
                    0.0
                )

                +

                if(
                    s.navigation_status_inconsistent,
                    0.10,
                    0.0
                )
            ),
            3
        ) as anomaly_score

    from severity as s
),

anomalies_only as (

    select
        s.mmsi as mmsi,
        s.activity_date as activity_date,

        s.vessel_name as vessel_name,
        s.ship_type_name as ship_type_name,
        s.ship_category as ship_category,

        s.ais_points as ais_points,
        s.observation_hours as observation_hours,

        s.avg_speed_kn as avg_speed_kn,
        s.max_speed_kn as max_speed_kn,

        s.stationary_observation_pct
            as stationary_observation_pct,

        s.dominant_navigational_status_name
            as dominant_navigational_status_name,

        s.dominant_status_pct
            as dominant_status_pct,

        s.nav_status_change_rate_pct
            as nav_status_change_rate_pct,

        s.baseline_days as baseline_days,

        round(
            s.baseline_avg_speed_kn,
            2
        ) as baseline_avg_speed_kn,

        round(
            s.baseline_sd_speed_kn,
            2
        ) as baseline_sd_speed_kn,

        round(
            s.speed_deviation_kn,
            2
        ) as speed_deviation_kn,

        round(
            s.speed_anomaly_threshold_kn,
            2
        ) as speed_anomaly_threshold_kn,

        s.speed_anomaly as speed_anomaly,

        round(
            s.baseline_stationary_pct,
            1
        ) as baseline_stationary_pct,

        round(
            s.baseline_sd_stationary_pct,
            1
        ) as baseline_sd_stationary_pct,

        round(
            s.stationary_deviation_pct,
            1
        ) as stationary_deviation_pct,

        round(
            s.stationary_anomaly_threshold_pct,
            1
        ) as stationary_anomaly_threshold_pct,

        s.stationary_anomaly
            as stationary_anomaly,

        s.status_speed_mismatch
            as status_speed_mismatch,

        s.navigation_status_inconsistent
            as navigation_status_inconsistent,

        s.anomaly_reason as anomaly_reason,
        s.anomaly_score as anomaly_score

    from scored as s

    where s.is_behavior_anomaly
),

ranked as (

    select
        a.*,

        row_number() over (
            partition by a.activity_date
            order by
                a.anomaly_score desc,
                a.mmsi asc
        ) as anomaly_rank

    from anomalies_only as a
)

select
    r.mmsi as mmsi,
    r.activity_date as activity_date,

    r.vessel_name as vessel_name,
    r.ship_type_name as ship_type_name,
    r.ship_category as ship_category,

    r.ais_points as ais_points,
    r.observation_hours as observation_hours,

    r.avg_speed_kn as avg_speed_kn,
    r.max_speed_kn as max_speed_kn,

    r.stationary_observation_pct
        as stationary_observation_pct,

    r.dominant_navigational_status_name
        as dominant_navigational_status_name,

    r.dominant_status_pct
        as dominant_status_pct,

    r.nav_status_change_rate_pct
        as nav_status_change_rate_pct,

    r.baseline_days as baseline_days,

    r.baseline_avg_speed_kn
        as baseline_avg_speed_kn,

    r.baseline_sd_speed_kn
        as baseline_sd_speed_kn,

    r.speed_deviation_kn
        as speed_deviation_kn,

    r.speed_anomaly_threshold_kn
        as speed_anomaly_threshold_kn,

    r.speed_anomaly as speed_anomaly,

    r.baseline_stationary_pct
        as baseline_stationary_pct,

    r.baseline_sd_stationary_pct
        as baseline_sd_stationary_pct,

    r.stationary_deviation_pct
        as stationary_deviation_pct,

    r.stationary_anomaly_threshold_pct
        as stationary_anomaly_threshold_pct,

    r.stationary_anomaly
        as stationary_anomaly,

    r.status_speed_mismatch
        as status_speed_mismatch,

    r.navigation_status_inconsistent
        as navigation_status_inconsistent,

    r.anomaly_reason as anomaly_reason,
    r.anomaly_score as anomaly_score,

    toUInt32(r.anomaly_rank)
        as anomaly_rank

from ranked as r