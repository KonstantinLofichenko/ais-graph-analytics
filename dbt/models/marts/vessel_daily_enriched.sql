{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(activity_date, mmsi)'
) }}

with ai_filtered as (

    select
        mmsi as mmsi,
        activity_date as activity_date,
        activity_class as activity_class,
        navigation_status_quality as navigation_status_quality,
        summary as summary,
        notable_behavior as notable_behavior,
        data_quality_note as data_quality_note,
        model as model,
        prompt_version as prompt_version,
        input_hash as input_hash,
        openai_response_id as openai_response_id,
        input_tokens as input_tokens,
        output_tokens as output_tokens,
        created_at as created_at

    from {{ source('analytics', 'vessel_ai_enrichment') }}

    where model = '{{ var("ai_model") }}'
      and prompt_version = '{{ var("ai_prompt_version") }}'
),

ai_latest as (

    select
        mmsi as mmsi,
        activity_date as activity_date,

        argMax(
            activity_class,
            created_at
        ) as activity_class,

        argMax(
            navigation_status_quality,
            created_at
        ) as navigation_status_quality,

        argMax(
            summary,
            created_at
        ) as ai_summary,

        argMax(
            notable_behavior,
            created_at
        ) as ai_notable_behavior,

        argMax(
            data_quality_note,
            created_at
        ) as ai_data_quality_note,

        argMax(
            model,
            created_at
        ) as ai_model,

        argMax(
            prompt_version,
            created_at
        ) as ai_prompt_version,

        argMax(
            input_hash,
            created_at
        ) as ai_input_hash,

        argMax(
            openai_response_id,
            created_at
        ) as openai_response_id,

        argMax(
            input_tokens,
            created_at
        ) as ai_input_tokens,

        argMax(
            output_tokens,
            created_at
        ) as ai_output_tokens,

        max(created_at) as ai_created_at,

        toUInt8(1) as ai_enrichment_exists

    from ai_filtered

    group by
        mmsi,
        activity_date
)

select
    f.mmsi as mmsi,
    f.activity_date as activity_date,

    f.vessel_name as vessel_name,
    f.ship_type_name as ship_type_name,
    f.ship_category as ship_category,

    f.ais_points as ais_points,
    f.first_seen as first_seen,
    f.last_seen as last_seen,
    f.observation_hours as observation_hours,

    f.avg_speed_kn as avg_speed_kn,
    f.max_speed_kn as max_speed_kn,
    f.stationary_observation_pct as stationary_observation_pct,

    f.last_navigational_status as last_navigational_status,
    f.last_navigational_status_name as last_navigational_status_name,

    f.dominant_navigational_status as dominant_navigational_status,
    f.dominant_navigational_status_name
        as dominant_navigational_status_name,

    f.dominant_status_pct as dominant_status_pct,
    f.nav_status_changes as nav_status_changes,
    f.nav_status_change_rate_pct as nav_status_change_rate_pct,

    ai.activity_class as activity_class,
    ai.navigation_status_quality as navigation_status_quality,

    ai.ai_summary as ai_summary,
    ai.ai_notable_behavior as ai_notable_behavior,
    ai.ai_data_quality_note as ai_data_quality_note,

    ai.ai_model as ai_model,
    ai.ai_prompt_version as ai_prompt_version,
    ai.ai_input_hash as ai_input_hash,

    ai.openai_response_id as openai_response_id,
    ai.ai_input_tokens as ai_input_tokens,
    ai.ai_output_tokens as ai_output_tokens,
    ai.ai_created_at as ai_created_at,

    toUInt8(
        ai.ai_enrichment_exists = 1
    ) as has_ai_enrichment

from {{ ref('vessel_daily_features') }} as f

left join ai_latest as ai
    on f.mmsi = ai.mmsi
    and f.activity_date = ai.activity_date