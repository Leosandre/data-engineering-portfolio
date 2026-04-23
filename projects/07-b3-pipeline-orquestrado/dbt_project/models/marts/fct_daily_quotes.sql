{{
    config(
        materialized='incremental',
        unique_key=['ticker', 'trade_date'],
        incremental_strategy='merge'
    )
}}

with enriched as (
    select * from {{ ref('int_quotes_enriched') }}
)

select
    trade_date,
    ticker,
    asset_type,
    company_name,
    sector,
    industry,
    dividend_yield,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    daily_return,
    avg(close_price) over (
        partition by ticker order by trade_date rows between 19 preceding and current row
    ) as moving_avg_20d,
    stddev(daily_return) over (
        partition by ticker order by trade_date rows between 19 preceding and current row
    ) as volatility_20d,
    current_timestamp as loaded_at
from enriched

{% if is_incremental() %}
where trade_date > (select max(trade_date) from {{ this }})
{% endif %}
