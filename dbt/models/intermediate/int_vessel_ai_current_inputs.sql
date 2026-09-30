{{ config(materialized='view') }}

select
    c.mmsi AS mmsi,
    c.activity_date AS activity_date,
    c.anomaly_rank AS anomaly_rank,
    h.input_hash AS current_ai_input_hash
from {{ ref('int_vessel_ai_candidates') }} AS c
inner join {{ source('analytics', 'vessel_ai_input_hashes') }} AS h FINAL
    on c.input_values = h.input_values
