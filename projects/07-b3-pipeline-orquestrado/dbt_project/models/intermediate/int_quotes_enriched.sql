with quotes as (
    select * from {{ ref('stg_quotes') }}
),

companies as (
    select * from {{ ref('stg_companies') }}
),

with_prev as (
    select
        q.*,
        c.company_name,
        c.sector,
        c.industry,
        c.dividend_yield,
        lag(q.close_price) over (
            partition by q.ticker order by q.trade_date
        ) as prev_close
    from quotes q
    left join companies c on q.ticker = c.ticker
)

select
    *,
    case
        when prev_close > 0
        then round((close_price - prev_close) / prev_close, 6)
    end as daily_return
from with_prev
