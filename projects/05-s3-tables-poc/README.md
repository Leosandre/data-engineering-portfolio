# 05 — POC: Amazon S3 Tables vs Iceberg Self-Managed

> Comparação prática entre **Amazon S3 Tables** (Iceberg gerenciado pela AWS) e **Iceberg self-managed** em S3 standard, com métricas reais de performance, manutenção e custo.

## 📊 Resultados

| Métrica | S3 Tables | Self-Managed |
|---------|-----------|-------------|
| Queries mais rápidas | **6/8** | 2/8 |
| Compaction | Automática ✅ | Manual (Glue Job, 78s) |
| Snapshot management | Automático ✅ | Manual |
| Orphan file removal | Automático ✅ | Manual |
| Small files após 12 batches | Otimizado automaticamente | 61 arquivos (média 10MB) |
| Código de manutenção | 0 linhas | ~40 linhas PySpark |
| Esforço operacional/mês | 0 horas | 4+ horas |
| Economia projetada (1TB/dia) | — | ~$31K/ano |

👉 **[Ver relatório completo com todas as métricas](report/index.html)**

## 🏗️ Arquitetura

```
                     NYC Taxi Parquet (S3 raw)
                              │
                 ┌────────────┴────────────┐
                 ▼                         ▼
        Pipeline A: S3 Tables        Pipeline B: Self-Managed
        (Table Bucket gerenciado)    (S3 Standard + Glue Catalog)
                 │                         │
            Glue Job A                Glue Job B
            (PySpark 5.0)             (PySpark 5.0)
                 │                         │
            Athena (WG-A)            Athena (WG-B)
                 │                         │
                 └────────────┬────────────┘
                              ▼
                    📊 Dashboard Comparativo
```

## 📋 Dataset

- **NYC Taxi Yellow Trip Data** — 2024 completo (12 meses)
- ~41 milhões de registros, ~661MB em Parquet
- 12 batches incrementais simulando cargas mensais de produção

## 🛠️ Stack

- **Infra**: CloudFormation (4 stacks YAML)
- **ETL**: AWS Glue 5.0 (PySpark, Iceberg)
- **Query**: Amazon Athena (8 tipos de queries × 3 runs cada)
- **S3 Tables**: REST endpoint do Iceberg
- **Automação**: Makefile + Bash + Python (boto3)

## 🚀 Como Reproduzir

### Pré-requisitos
- AWS CLI v2 configurado
- Python 3.9+
- Conta AWS com permissões para S3, S3 Tables, Glue, Athena, IAM, CloudFormation
- Região: `us-east-1`

### Execução

```bash
# 1. Baixar dataset (12 meses NYC Taxi 2024)
make download

# 2. Deployar infraestrutura
make deploy

# 3. Upload dataset + scripts Glue
make upload

# 4. Executar ETL (12 batches × 2 pipelines)
make etl

# 5. Schema evolution (add/drop column)
make schema-evolution

# 6. Time travel (query em versão anterior)
make time-travel

# 7. Queries comparativas (8 queries × 3 runs × 2 pipelines)
make queries

# 8. Métricas de manutenção + compaction manual
make maintenance

# 9. Gerar dashboard HTML
make dashboard

# Cleanup completo
make teardown
```

## 📂 Estrutura

```
05-s3-tables-poc/
├── cloudformation/          # 4 stacks (storage, iam, glue-catalog, glue-jobs)
├── glue-jobs/               # PySpark: ETL managed, ETL self-managed, compaction
├── scripts/                 # Automação: download, deploy, ETL, queries, métricas
├── docs/                    # Arquitetura e estimativa de custo
├── report/                  # Dashboard HTML com resultados
└── Makefile                 # Orquestração
```

## ⚠️ Lições Aprendidas

- S3 Tables exige **Glue 5.0** — Glue 4.0 não tem suporte
- Usar **REST endpoint** do Iceberg (não o JAR S3TablesCatalog)
- REST endpoint não suporta CTAS — criar tabela primeiro, inserir depois
- Databases no Glue Catalog **não aceitam hífens** quando usados com Iceberg
- Metadata tables (`$iceberg_snapshots`) não funcionam com table redirection no Athena

## 💰 Custo da POC

~$15-30 (Glue Jobs + Athena queries + S3 storage). Toda a infra é destruída com `make teardown`.

## Autor

[Leonardo Sandre](https://github.com/Leosandre) — [ST IT Cloud](https://stitcloud.com)
