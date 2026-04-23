select
    ticker,
    date_trunc('month', trade_date) as month,
    count(*) as trading_days,
    round((last_value(close_price) over (
        partition by ticker, date_trunc('month', trade_date)
        order by trade_date rows between unbounded preceding and unbounded following
    ) - first_value(close_price) over (
        partition by ticker, date_trunc('month', trade_date)
        order by trade_date rows between unbounded preceding and unbounded following
    )) / nullif(first_value(close_price) over (
        partition by ticker, date_trunc('month', trade_date)
        order by trade_date rows between unbounded preceding and unbounded following
    ), 0), 6) as monthly_return,
    round(avg(volume), 0) as avg_daily_volume
from {{ ref('fct_daily_quotes') }}
group by ticker, date_trunc('month', trade_date), close_price, trade_date
