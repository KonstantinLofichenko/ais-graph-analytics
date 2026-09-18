with source_data as (
    select
        codes,
        names,
        region,
        JSONExtractArrayRaw(ifNull(capitals, '[]')) as capital_entries
    from {{ source('raw', 'countries') }}
),

normalized as (
    select
        nullIf(trimBoth(JSONExtractString(ifNull(codes, '{}'), 'alpha_2')), '') as country_code,
        nullIf(trimBoth(JSONExtractString(ifNull(codes, '{}'), 'alpha_3')), '') as iso3,
        -- Keep numeric codes as strings to preserve leading zeroes (e.g. '004').
        nullIf(trimBoth(JSONExtractString(ifNull(codes, '{}'), 'ccn3')), '') as numeric_code,
        nullIf(trimBoth(JSONExtractString(ifNull(names, '{}'), 'common')), '') as country_name,
        nullIf(trimBoth(region), '') as region,
        coalesce(
            nullIf(trimBoth(JSONExtractString(
                arrayFirst(capital -> JSONExtractBool(capital, 'attributes', 'primary'), capital_entries),
                'name'
            )), ''),
            nullIf(trimBoth(JSONExtractString(capital_entries[1], 'name')), '')
        ) as capital_city
    from source_data
)

select
    country_code,
    iso3,
    numeric_code,
    country_name,
    region,
    capital_city
from normalized
where match(country_code, '^[A-Z]{2}$')
