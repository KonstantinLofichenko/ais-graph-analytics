{% test valid_current_ai(model) %}
select
    e.mmsi AS mmsi,
    e.activity_date AS activity_date
from {{ model }} AS e
left join {{ ref('int_vessel_ai_current_inputs') }} AS c
    on e.mmsi = c.mmsi and e.activity_date = c.activity_date
where
    (e.has_ai_enrichment = 1 and (
        c.current_ai_input_hash is null
        or c.current_ai_input_hash = ''
        or e.ai_input_hash is null
        or e.ai_input_hash != c.current_ai_input_hash
        or c.anomaly_rank > 100
        or c.anomaly_rank = 0
        or e.ai_model is null
        or e.ai_prompt_version is null
        or e.ai_model != '{{ var("ai_model") }}'
        or e.ai_prompt_version != '{{ var("ai_prompt_version") }}'
    ))
    or (e.has_ai_enrichment = 0 and (
        e.activity_class is not null or e.navigation_status_quality is not null
        or e.ai_summary is not null or e.ai_notable_behavior is not null
        or e.ai_data_quality_note is not null or e.ai_model is not null
        or e.ai_prompt_version is not null or e.ai_input_hash is not null
        or e.openai_response_id is not null or e.ai_input_tokens is not null
        or e.ai_output_tokens is not null or e.ai_created_at is not null
    ))
{% endtest %}
