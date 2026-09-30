{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(activity_date, mmsi)'
) }}

with matching_responses as (
    select
        h.mmsi AS mmsi,
        h.activity_date AS activity_date,
        toNullable(h.activity_class) AS activity_class,
        toNullable(h.navigation_status_quality) AS navigation_status_quality,
        toNullable(h.summary) AS ai_summary,
        toNullable(h.notable_behavior) AS ai_notable_behavior,
        toNullable(h.data_quality_note) AS ai_data_quality_note,
        toNullable(h.model) AS ai_model,
        toNullable(h.prompt_version) AS ai_prompt_version,
        toNullable(h.input_hash) AS ai_input_hash,
        toNullable(h.openai_response_id) AS openai_response_id,
        toNullable(h.input_tokens) AS ai_input_tokens,
        toNullable(h.output_tokens) AS ai_output_tokens,
        toNullable(h.created_at) AS ai_created_at,
        toUInt8(1) AS ai_enrichment_exists,
        row_number() over (
            partition by h.activity_date, h.mmsi
            order by h.created_at desc, h.openai_response_id desc
        ) AS response_rank
    from {{ source('analytics', 'vessel_ai_enrichment') }} AS h
    inner join {{ ref('int_vessel_ai_current_inputs') }} AS c
        on h.mmsi = c.mmsi
       and h.activity_date = c.activity_date
       and h.input_hash = c.current_ai_input_hash
    where h.model = '{{ var("ai_model") }}'
      and h.prompt_version = '{{ var("ai_prompt_version") }}'
      and c.anomaly_rank <= 100
),
ai_latest as (
    -- Choose one coherent response only within the exact current cache key.
    select * from matching_responses where response_rank = 1
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
        coalesce(ai.ai_enrichment_exists, 0) = 1
    ) as has_ai_enrichment

from {{ ref('vessel_daily_features') }} as f

left join ai_latest as ai
    on f.mmsi = ai.mmsi
    and f.activity_date = ai.activity_date