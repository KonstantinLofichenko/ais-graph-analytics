{{ config(
    engine='MergeTree()',
    order_by='country_code'
) }}

select
    assumeNotNull(country_code) as country_code,
    assumeNotNull(iso3) as iso3,
    numeric_code,
    assumeNotNull(country_name) as country_name,
    region,
    capital_city
from {{ ref('stg_countries') }}