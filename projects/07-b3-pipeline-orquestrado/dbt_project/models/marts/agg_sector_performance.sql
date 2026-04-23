select
    trade_date,
    asset_type,
    sector,
    count(distinct ticker) as num_assets,
    round(avg(daily_return), 6) as avg_return,
    round(sum(volume), 0) as total_volume,
    round(avg(close_price), 2) as avg_price
from {{ ref('fct_daily_quotes') }}
where sector != 'Unknown'
group by trade_date, asset_type, sector
