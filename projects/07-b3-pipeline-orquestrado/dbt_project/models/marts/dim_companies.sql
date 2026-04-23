select
    ticker,
    company_name,
    sector,
    industry,
    market_cap,
    currency
from {{ ref('stg_companies') }}
