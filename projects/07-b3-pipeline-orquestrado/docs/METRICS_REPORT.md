# Relatório de Métricas — Pipeline B3 Orquestrado

## Resumo da Execução

| Métrica | Valor |
|---------|-------|
| Total de registros processados | 1.197 cotações |
| Ativos monitorados | 63 (48 ações + 15 FIIs) |
| Dias de pregão processados | 19 (dezembro/2024) |
| Período | 2024-12-02 a 2024-12-30 |
| Objetos no S3 | 20 (19 cotações + 1 empresas) |
| Modelos dbt executados | 6 (2 views + 3 tables + 1 incremental) |
| Snapshot SCD2 | 63 empresas rastreadas |
| Testes dbt | 17/17 passando |
| Tempo de ingestão (19 dias) | ~4 minutos |
| Tempo dbt run | ~8 segundos |
| Tempo dbt test | ~2 segundos |

## Dados por Tipo de Ativo

| Tipo | Registros | Ativos | Preço Médio (R$) | Volume Total (MM) |
|------|-----------|--------|------------------|--------------------|
| STOCK | 912 | 48 | 21.34 | 12.747 |
| FII | 285 | 15 | 63.14 | 96 |

## Top 5 Retornos — Dezembro 2024

| Ticker | Tipo | Setor | Retorno (%) |
|--------|------|-------|-------------|
| B3SA3 | STOCK | Financial Services | +10.15% |
| KNRI11 | FII | Real Estate | +6.44% |
| IRBR3 | STOCK | Financial Services | +5.96% |
| HGLG11 | FII | Real Estate | +5.49% |
| KLBN11 | STOCK | Basic Materials | +4.69% |

## Bottom 5 Retornos — Dezembro 2024

| Ticker | Tipo | Setor | Retorno (%) |
|--------|------|-------|-------------|
| MGLU3 | STOCK | Consumer Cyclical | -29.38% |
| CSNA3 | STOCK | Basic Materials | -20.75% |
| CSAN3 | STOCK | Energy | -18.32% |
| HAPV3 | STOCK | Financial Services | -16.48% |
| BEEF3 | STOCK | Consumer Defensive | -15.38% |

## Performance por Setor (retorno médio diário)

| Tipo | Setor | Ativos | Retorno Médio Diário (%) |
|------|-------|--------|--------------------------|
| FII | Real Estate | 12 | +0.149% |
| FII | Financial Services | 2 | +0.051% |
| STOCK | Technology | 2 | -0.161% |
| STOCK | Financial Services | 8 | -0.186% |
| STOCK | Industrials | 3 | -0.318% |
| STOCK | Energy | 5 | -0.335% |
| STOCK | Utilities | 7 | -0.339% |
| STOCK | Basic Materials | 8 | -0.355% |
| STOCK | Consumer Cyclical | 5 | -0.623% |

**Insight**: FIIs tiveram retorno positivo em dezembro/2024 enquanto ações caíram em todos os setores. Consumer Cyclical foi o pior setor (-0.62%/dia), puxado por MGLU3 (-29%).

## Testes de Qualidade (dbt)

```
PASS  dbt_utils_accepted_range_fct_daily_quotes_volume__0
PASS  not_null_agg_sector_performance_sector
PASS  not_null_agg_sector_performance_trade_date
PASS  not_null_dim_companies_sector
PASS  not_null_dim_companies_ticker
PASS  not_null_fct_daily_quotes_close_price
PASS  not_null_fct_daily_quotes_ticker
PASS  not_null_fct_daily_quotes_trade_date
PASS  not_null_fct_daily_quotes_volume
PASS  source_not_null_raw_companies_ticker
PASS  source_not_null_raw_quotes_close_price
PASS  source_not_null_raw_quotes_ticker
PASS  source_not_null_raw_quotes_trade_date
PASS  source_unique_raw_companies_ticker
PASS  test_price_variation_limit (nenhuma cotação variou > 30%)
PASS  test_unique_ticker_date (sem duplicatas ticker+data)
PASS  unique_dim_companies_ticker

Total: 17 PASS, 0 WARN, 0 ERROR
```

## Infraestrutura

| Recurso | Tipo | Status |
|---------|------|--------|
| VPC | CloudFormation | CREATE_COMPLETE |
| EC2 (Airflow) | t3.medium | running |
| Redshift Serverless | 8 RPU base | AVAILABLE |
| S3 | b3-pipeline-data-* | 20 objetos, 163 KB |
| SNS | b3-pipeline-alerts | Ativo |
| CloudFormation | 3 stacks | Todas CREATE_COMPLETE |

## Custo Estimado

| Serviço | Uso | Custo |
|---------|-----|-------|
| EC2 t3.medium | ~20h | ~$0.84 |
| Redshift Serverless | ~5 RPU-h | ~$1.80 |
| S3 | 163 KB | ~$0.00 |
| Data transfer | mínimo | ~$0.01 |
| **Total estimado** | | **~$2.65** |

*Nota: custo real será confirmado no Cost Explorer após 24h.*
