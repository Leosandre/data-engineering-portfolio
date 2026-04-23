-- Testa que não há duplicatas de ticker+trade_date em fct_daily_quotes
select ticker, trade_date, count(*) as cnt
from {{ ref('fct_daily_quotes') }}
group by ticker, trade_date
having count(*) > 1
