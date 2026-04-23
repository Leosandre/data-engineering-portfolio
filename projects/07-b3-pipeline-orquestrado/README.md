# Pipeline Incremental Orquestrado — Dados B3

Pipeline de dados financeiros da B3 (bolsa brasileira) com orquestração Airflow, modelagem incremental dbt, validação de qualidade e observabilidade. Processa cotações diárias de 63 ativos (48 ações do Ibovespa + 15 FIIs) com backfill automatizado.

## Arquitetura

```
┌─────────────┐     ┌─────────┐     ┌───────────────────┐     ┌──────────┐
│  Yahoo       │────▶│   S3    │────▶│ Redshift Serverless│────▶│  dbt     │
│  Finance API │     │  (raw)  │     │   (COPY Parquet)   │     │ (models) │
└─────────────┘     └─────────┘     └───────────────────┘     └──────────┘
       │                                                            │
       │              ┌──────────┐                                  │
       └─────────────▶│ Airflow  │◀─────────────────────────────────┘
                      │  (EC2)   │──────▶ SNS (alertas)
                      └──────────┘
```

**Fluxo do DAG:**
```
check_trading_day → [ingest_quotes, ingest_companies] → load_to_redshift → dbt_run → dbt_snapshot → dbt_test → notify
```

## Stack

| Serviço | Uso |
|---------|-----|
| **Airflow 2.9** | Orquestração (EC2 t3.medium) |
| **dbt 1.8** | Transformação incremental + SCD2 + testes |
| **Redshift Serverless** | Data warehouse (8 RPU) |
| **S3** | Data lake (raw Parquet) |
| **SNS** | Alertas de sucesso/falha |
| **CloudFormation** | IaC (3 stacks) |

## Modelos dbt

| Camada | Modelo | Materialização | Descrição |
|--------|--------|---------------|-----------|
| staging | `stg_quotes` | view | Cotações limpas e tipadas |
| staging | `stg_companies` | view | Dados cadastrais padronizados |
| intermediate | `int_quotes_enriched` | ephemeral | Join + retorno diário |
| marts | `fct_daily_quotes` | **incremental** | Fato principal com média móvel 20d e volatilidade |
| marts | `dim_companies` | table | Dimensão de empresas |
| marts | `agg_sector_performance` | table | Performance diária por setor e tipo de ativo |
| marts | `agg_monthly_returns` | table | Retornos mensais |
| snapshots | `snap_companies` | **SCD Type 2** | Rastreia mudanças em setor/indústria |

## Testes de Qualidade

17 testes automatizados, incluindo:
- `not_null` e `unique` em todas as PKs
- `accepted_range` em volume (> 0)
- `test_price_variation_limit` — nenhuma cotação pode variar > 30% em 1 dia (circuit breaker B3)
- `test_unique_ticker_date` — sem duplicatas ticker+data
- Source freshness em dados brutos

## Resultados

| Métrica | Valor |
|---------|-------|
| Cotações processadas | 1.197 |
| Ativos monitorados | 63 (48 ações + 15 FIIs) |
| Pregões | 19 (dezembro/2024) |
| Modelos dbt | 6 |
| Testes dbt | 17/17 passando |
| Tempo de ingestão (19 dias) | ~4 min |
| Tempo dbt run | ~8s |
| Custo total AWS | ~$2.65 |

### Top 5 Retornos — Dezembro 2024

| Ticker | Tipo | Setor | Retorno |
|--------|------|-------|---------|
| B3SA3 | Ação | Financial Services | +10.15% |
| KNRI11 | FII | Real Estate | +6.44% |
| IRBR3 | Ação | Financial Services | +5.96% |
| HGLG11 | FII | Real Estate | +5.49% |
| KLBN11 | Ação | Basic Materials | +4.69% |

### Bottom 5 Retornos

| Ticker | Tipo | Setor | Retorno |
|--------|------|-------|---------|
| MGLU3 | Ação | Consumer Cyclical | -29.38% |
| CSNA3 | Ação | Basic Materials | -20.75% |
| CSAN3 | Ação | Energy | -18.32% |
| HAPV3 | Ação | Financial Services | -16.48% |
| BEEF3 | Ação | Consumer Defensive | -15.38% |

**Insight**: FIIs tiveram retorno positivo em dezembro/2024 enquanto ações caíram em todos os setores. Consumer Cyclical foi o pior setor, puxado por MGLU3 (-29%).

## Como Reproduzir

```bash
# 1. Setup
make setup

# 2. Deploy (3 stacks CloudFormation)
make deploy

# 3. Backfill dezembro/2024
make backfill

# 4. Extrair evidências
make evidence

# 5. Teardown (destrói tudo)
make teardown
make verify-cleanup
```

## Estrutura

```
07-b3-pipeline-orquestrado/
├── cloudformation/
│   ├── 01-network.yaml          # VPC, subnets, security group
│   ├── 02-storage.yaml          # S3, SNS, IAM roles
│   └── 03-compute.yaml          # EC2 (Airflow), Redshift Serverless
├── dags/
│   └── b3_daily_pipeline.py     # DAG com 7 tasks, retry, skip feriados
├── dbt_project/
│   ├── models/staging/          # stg_quotes, stg_companies
│   ├── models/intermediate/     # int_quotes_enriched
│   ├── models/marts/            # fct_daily_quotes (incremental), dim, aggs
│   ├── snapshots/               # snap_companies (SCD2)
│   └── tests/                   # price_variation_limit, unique_ticker_date
├── src/
│   ├── tickers.py               # 63 ativos validados (48 ações + 15 FIIs)
│   ├── ingest_quotes.py         # yfinance → Parquet → S3
│   ├── ingest_companies.py      # Dados cadastrais → S3
│   ├── load_to_redshift.py      # COPY S3 → Redshift
│   └── generate_dashboard.py    # Gráficos com Plotly
├── scripts/
│   ├── deploy.sh                # Deploy 3 stacks
│   ├── teardown.sh              # Destrói tudo
│   └── verify_cleanup.sh        # Confirma 0 recursos
└── docs/
    └── METRICS_REPORT.md        # Métricas detalhadas da execução
```

## Lições Aprendidas

1. **MWAA é caro e frágil** — tentei 3 vezes, falhou em todas. Pivotei pra EC2 com Airflow standalone que subiu em 3 minutos por uma fração do custo
2. **`raw` é palavra reservada no Redshift** — precisa de aspas duplas em todo lugar
3. **Redshift COPY não suporta DEFAULT columns com Parquet** — removi `_loaded_at` da tabela
4. **dbt `delete+insert` não funciona no Redshift** — alias em DELETE não é suportado, troquei pra `merge`
5. **SequentialExecutor do Airflow não lida bem com backfill paralelo** — resolvi com loop bash sequencial
6. **FIIs no yfinance funcionam perfeitamente** — dados de cotação e cadastro disponíveis
