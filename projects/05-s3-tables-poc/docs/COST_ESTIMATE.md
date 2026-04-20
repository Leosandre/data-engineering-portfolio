# Estimativa de Custo — POC S3 Tables

## Premissas
- Região: us-east-1
- Dataset: ~50GB (10 meses NYC Taxi Yellow)
- 20 Glue Job runs (10 por pipeline) + 1 compaction
- ~50 queries Athena
- Duração da POC: 1-2 dias

## Breakdown

| Serviço | Cálculo | Estimativa |
|---------|---------|-----------|
| **S3 Storage** | ~50GB × $0.023/GB | $1.15 |
| **S3 Tables Storage** | ~25GB × $0.023/GB | $0.58 |
| **S3 Tables Maintenance** | Compaction automática (~25GB rewrite) | $2-5 |
| **Glue Jobs (ETL)** | 20 runs × ~3min × 2 DPU × $0.44/DPU-hr | ~$8.80 |
| **Glue Job (Compaction)** | 1 run × ~5min × 2 DPU × $0.44/DPU-hr | ~$0.73 |
| **Athena Queries** | ~50 queries × ~10GB scan médio × $5/TB | ~$2.50 |
| **S3 Requests** | PUT/GET/LIST | ~$0.50 |
| | | |
| **Total Estimado** | | **$16-19** |

## Limites de Segurança
- Athena workgroups com scan limit: 100GB/query
- Glue auto-scaling: máximo 10 workers
- Lifecycle policy no bucket de resultados: 7 dias

## Custo em Produção (projeção)

Para um cenário de produção com 1TB/dia de ingestão:

| Cenário | S3 Tables | Self-Managed |
|---------|-----------|-------------|
| Storage (30TB/mês) | ~$690 | ~$690 |
| Manutenção | Incluído* | ~$200/mês (Glue) |
| Engenharia | 0h/mês | ~16h/mês (~$2,400) |
| **Total** | **~$690** | **~$3,290** |

*S3 Tables cobra por compaction, mas é significativamente menor que rodar Glue Jobs manualmente.
