with source as (
    select * from {{ source('raw', 'companies') }}
)

select
    ticker,
    asset_type,
    trim(company_name) as company_name,
    coalesce(nullif(trim(sector), ''), 'Unknown') as sector,
    coalesce(nullif(trim(industry), ''), 'Unknown') as industry,
    market_cap,
    dividend_yield,
    currency
from source
where ticker is not null
