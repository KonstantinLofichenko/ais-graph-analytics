{{ config(
    engine='MergeTree()',
    order_by='mmsi'
) }}

select
    assumeNotNull(mmsi) as mmsi,
    vessel_name,
    ship_type,
    first_seen,
    last_seen,
    name_count
from {{ ref('stg_vessels') }}
