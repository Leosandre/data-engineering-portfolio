/*
  Teste: cotação não pode variar mais de 30% em um dia.
  O circuit breaker da B3 é ~15%, então 30% é margem generosa.
*/

select
    trade_date,
    ticker,
    daily_return
from {{ ref('fct_daily_quotes') }}
where abs(daily_return) > {{ var('price_variation_limit') }}
  and daily_return is not null
