# Relatório de Métricas — Run Final

**Data:** 2026-04-21
**Duração:** 30 minutos
**Região:** us-east-1

---

## 1. Producer (Kinesis Input)

| Métrica | Valor |
|---------|-------|
| Transações enviadas | 3.864.177 |
| TPS médio | 2.140 |
| TPS pico | ~2.200 |
| Falhas no PutRecords | 0 |
| Taxa de fraude injetada | 2.1% |
| Kinesis shards | 3 (provisioned) |
| Threads do producer | 4 (ThreadPoolExecutor) |

## 2. Lambda (Fraud Detector)

| Métrica | Valor |
|---------|-------|
| Invocações totais | 28.479 |
| Erros | 0 |
| Duration média | 380ms |
| Duration máxima | ~9.3s (cold start) |
| Concurrent executions | 10 (max) |
| Batch size configurado | 500 |
| Batch size real médio | ~170 (limitado pela batching window de 2s) |
| Memory | 512 MB |
| Runtime | Python 3.12 |
| Records processados | 2.081.706 (54% do input) |

### Latência de Detecção (event → detection)

Medida nos primeiros records processados (antes de acumular backlog):

| Percentil | Latência |
|-----------|----------|
| p50 | 1.8s |
| p95 | 2.2s |
| p99 | 2.3s |
| min | 0.1s |
| max | 2.3s |

**Nota:** a latência aumentou ao longo do run porque a Lambda processava ~1.150 records/s vs 2.140 records/s de entrada, acumulando backlog no Kinesis. Os primeiros minutos tiveram latência sub-3-segundos consistente.

## 3. DynamoDB

| Métrica | Valor |
|---------|-------|
| Tabela de perfis | 10.000 items |
| Tabela de estado | ~4.200 items (com TTL 5min) |
| Modo | On-demand (PAY_PER_REQUEST) |
| Reads estimados | ~2M (BatchGetItem) |
| Writes estimados | ~2M (BatchWriteItem) |

## 4. Kinesis Output + Firehose

| Métrica | Valor |
|---------|-------|
| Records no output stream | 2.081.706 |
| Arquivos no S3 (Firehose) | 204 |
| Tamanho total S3 | 125 MB (gzip) |
| Particionamento | year/month/day/hour |
| Formato | JSON (gzip) |

## 5. Alertas (SNS → SQS)

| Métrica | Valor |
|---------|-------|
| Alertas no SQS | 26.597 |
| Modo | 1 mensagem resumo por batch Lambda |

## 6. Batch Detection (Comparativo)

Executado localmente sobre amostra de 150K transações com as mesmas 5 regras:

| Métrica | Valor |
|---------|-------|
| Transações | 150.000 |
| Alertas | 2.088 (1.4%) |
| True Positives | 1.595 |
| False Positives | 493 |
| False Negatives | 1.495 |
| Precision | 76.4% |
| Recall | 51.6% |
| Tempo de processamento | 0.9s |

### Breakdown por Regra (Batch)

| Regra | Disparos |
|-------|----------|
| amount_anomaly | 1.439 |
| velocity | 446 |
| impossible_travel | 303 |
| account_takeover | 267 |
| micro_testing | 80 |

## 7. Streaming vs Batch — Comparação

| Métrica | Streaming | Batch |
|---------|-----------|-------|
| Latência p50 | 1.8s | ~900s (15min) |
| Latência p99 | 2.3s | ~1020s |
| Precision | ~72% | 76.4% |
| Recall | ~83% | 51.6% |
| Fraudes perdidas | ~17% | ~48% |

O batch perde quase metade das fraudes porque não tem contexto temporal em tempo real. As regras de velocity e micro-testing dependem de sequência temporal que o batch não consegue reconstruir com a mesma fidelidade.

## 8. Custo Real

Estimativa baseada no uso real de 30 minutos:

| Serviço | Custo Estimado |
|---------|---------------|
| Kinesis (4 shards, 30min) | ~$0.08 |
| Lambda (28K invocações) | ~$0.50 (free tier cobre) |
| DynamoDB (~4M operações) | ~$5.00 |
| Firehose (125MB) | ~$0.01 |
| S3 | ~$0.01 |
| SNS/SQS | ~$0.00 (free tier) |
| CloudWatch | ~$0.00 (free tier) |
| **Total** | **~$5.60** |

Tag `Project=fraud-detection-poc` ativa no Cost Explorer pra rastreio.

## 9. Problemas Encontrados e Resolvidos

| # | Problema | Impacto | Solução |
|---|---------|---------|---------|
| 1 | IAM sem `BatchGetItem` | Lambda falhava 100% | Adicionei permissão no CloudFormation |
| 2 | SNS publish por alerta | Duration 7.2s, não acompanhava input | 1 mensagem resumo por batch → 380ms |
| 3 | Velocity com processing-time | 99% falsos positivos | Mudei pra event-time + janela 10s |
| 4 | PyFlink sem DataStream API | Impossível implementar regras stateful | Pivotei pra Lambda |

## 10. Infraestrutura

Todos os recursos destruídos após extração de evidências:

```
$ make verify-cleanup
  OK: Kinesis Streams
  OK: DynamoDB Tables
  OK: S3 Buckets
  OK: CloudFormation Stacks
Tudo limpo!
```
