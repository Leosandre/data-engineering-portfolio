with source as (
    select * from {{ source('raw', 'quotes') }}
)

select
    trade_date,
    ticker,
    asset_type,
    round(open_price, 2) as open_price,
    round(high_price, 2) as high_price,
    round(low_price, 2) as low_price,
    round(close_price, 2) as close_price,
    volume
from source
where close_price > 0
  and volume > 0
