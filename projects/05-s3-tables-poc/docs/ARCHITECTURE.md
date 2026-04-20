# Arquitetura — POC S3 Tables

## Visão Geral

Dois pipelines paralelos processam o mesmo dataset (NYC Taxi, ~30M registros) com configurações idênticas de Glue Job. A única diferença é o destino: S3 Tables (managed) vs S3 standard (self-managed).

## Componentes

### Storage
| Recurso | Tipo | Propósito |
|---------|------|-----------|
| `s3tables-poc-managed` | S3 Table Bucket | Pipeline A — Iceberg gerenciado |
| `s3tables-poc-raw-{account}` | S3 General Purpose | Dataset raw (Parquet) |
| `s3tables-poc-selfmanaged-{account}` | S3 General Purpose | Pipeline B — Iceberg self-managed |
| `s3tables-poc-athena-results-{account}` | S3 General Purpose | Resultados Athena |

### Compute
| Recurso | Config | Propósito |
|---------|--------|-----------|
| Glue Job A | G.1X, 2-10 workers, auto-scaling | ETL → S3 Tables |
| Glue Job B | G.1X, 2-10 workers, auto-scaling | ETL → S3 standard |
| Glue Job Compaction | G.1X, 2-10 workers | Compaction manual (pipeline B) |

### Query
| Recurso | Propósito |
|---------|-----------|
| Athena WG `managed` | Queries pipeline A (scan limit 100GB) |
| Athena WG `selfmanaged` | Queries pipeline B (scan limit 100GB) |

### Integração
- S3 Table Bucket ↔ Glue Data Catalog via catálogo federado `s3tablescatalog`
- Glue Database `s3tables-poc_selfmanaged` para pipeline B

## Fluxo de Dados

```
1. Download NYC Taxi Parquet → local
2. Upload → s3://raw-bucket/nyc-taxi/
3. Glue Job A lê raw → escreve Iceberg no Table Bucket (10 batches)
4. Glue Job B lê raw → escreve Iceberg no S3 standard (10 batches)
5. Schema evolution via Athena ALTER TABLE (ambos)
6. Time travel via Athena FOR VERSION AS OF (ambos)
7. Queries comparativas via Athena (4 queries × 3 runs × 2 pipelines)
8. Compaction: automática (A) vs manual via Glue Job (B)
9. Dashboard HTML com todas as métricas
```

## Segurança
- IAM role com least privilege (acesso apenas aos buckets da POC)
- Encryption at rest (SSE-S3) em todos os buckets
- Block Public Access habilitado em todos os buckets
- Athena workgroups com scan limit (100GB/query)
