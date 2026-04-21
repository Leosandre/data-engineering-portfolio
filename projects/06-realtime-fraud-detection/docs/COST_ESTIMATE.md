# Estimativa de Custo — Detecção de Fraude em Tempo Real

## Premissas
- Região: us-east-1
- Duração: 30 minutos de produção a 2.000 TPS
- Volume: ~3.6M transações
- Conta com free tier ativo (12 meses)

## Breakdown

### Sem Free Tier

| Serviço | Cálculo | Custo |
|---------|---------|-------|
| **Kinesis** (4 shards, 30min) | 4 × $0.015/shard-hour × 0.5h | $0.03 |
| Kinesis PUT units | 3.6M × $0.014/1M | $0.05 |
| **Lambda** (3.6M invocações) | 3.6M × $0.20/1M (requests) | $0.72 |
| Lambda compute | 3.6M × 0.2s × 0.25GB × $0.0000166667/GB-s | $3.00 |
| **DynamoDB** (on-demand) | 3.6M writes × $1.25/1M | $4.50 |
| | 3.6M reads × $0.25/1M | $0.90 |
| **Firehose** | ~3.5GB × $0.029/GB | $0.10 |
| **S3** | storage + requests | $0.25 |
| **SNS** | ~80K publishes × $0.50/1M | $0.04 |
| **SQS** | ~80K requests × $0.40/1M | $0.03 |
| **CloudWatch** | logs + métricas | $0.30 |
| **Total** | | **~$9.92** |

### Com Free Tier

| Serviço | Free Tier Aplicável | Custo Real |
|---------|-------------------|-----------|
| Kinesis | ❌ Sem free tier | $0.08 |
| Lambda | ✅ 1M requests + 400K GB-s/mês (always free) | $0.00 |
| DynamoDB | ❌ On-demand não tem free tier | $5.40 |
| Firehose | ❌ Sem free tier | $0.10 |
| S3 | ✅ 5GB + 20K GET/mês (12 meses) | $0.10 |
| SNS | ✅ 1M publishes/mês (always free) | $0.00 |
| SQS | ✅ 1M requests/mês (always free) | $0.00 |
| CloudWatch | ✅ 10 métricas + 5GB logs (always free) | $0.00 |
| **Total** | | **~$5.68** |

## Observações

- **DynamoDB é 55% do custo total.** Em produção, usaria DAX (cache in-memory) ou provisioned capacity com auto-scaling. DAX reduz reads em ~90% ($0.25/h por nó, mas compensa acima de 10K reads/s).
- **Lambda é grátis no free tier.** 3.6M invocações × 0.2s × 256MB = 180K GB-s, bem abaixo dos 400K GB-s gratuitos.
- **Kinesis provisioned é mais barato que on-demand** pra volume previsível e curta duração.

## Rastreio de Custo

Todos os recursos têm a tag `Project=fraud-detection-poc`. Pra ver no Cost Explorer:
1. Billing → Cost Explorer → Group by: Tag → Project
2. A tag é ativada como Cost Allocation Tag automaticamente pelo deploy
3. Dados aparecem com ~24h de atraso
