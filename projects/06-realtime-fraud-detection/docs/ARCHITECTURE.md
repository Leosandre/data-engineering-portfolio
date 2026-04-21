# Arquitetura — Detecção de Fraude em Tempo Real

## Visão Geral

Dois pipelines paralelos processam as mesmas transações com as mesmas regras. A única diferença é o timing: streaming processa cada transação no momento que chega, batch processa tudo a cada 15 minutos.

## Componentes

### Storage
| Recurso | Tipo | Propósito |
|---------|------|-----------|
| `fraud-detection-poc-transactions` | Kinesis (3 shards) | Stream de entrada (2.000 TPS) |
| `fraud-detection-poc-alerts` | Kinesis (1 shard) | Stream de saída (transações + score) |
| `fraud-detection-poc-customer-profiles` | DynamoDB (on-demand) | Perfil financeiro de 10K clientes |
| `fraud-detection-poc-detection-state` | DynamoDB (on-demand, TTL 5min) | Estado das regras (velocity, localização) |
| `fraud-detection-poc-lake-*` | S3 | Data lake (Firehose entrega particionado por hora) |

### Compute
| Recurso | Config | Propósito |
|---------|--------|-----------|
| Lambda `fraud-detection-poc-detector` | Python 3.12, 256MB, timeout 60s | Detecção em tempo real |
| Event Source Mapping | batch 100, window 1s, parallelism 3 | Conecta Kinesis → Lambda |

### Messaging
| Recurso | Propósito |
|---------|-----------|
| SNS `fraud-detection-poc-fraud-alerts` | Alertas de fraude (score >= 0.7) |
| SQS `fraud-detection-poc-fraud-alerts-queue` | Consumidor de alertas |

## Fluxo de Dados

```
1. Producer gera transações sintéticas (2.000/s, 5 padrões de fraude)
2. PutRecords no Kinesis (batches de 500, 4 threads paralelas)
3. Event Source Mapping entrega batches de 100 records pra Lambda
4. Lambda:
   a. Busca perfil do cliente no DynamoDB (cache local por invocação)
   b. Busca estado de detecção no DynamoDB (velocity timestamps, última localização)
   c. Aplica 5 regras, calcula score máximo
   d. Atualiza estado no DynamoDB
   e. Se score >= 0.7: publica no SNS
   f. Escreve todas as transações (com score) no Kinesis output
5. Firehose consome Kinesis output → S3 (Parquet, particionado por hora)
6. Batch detection roda sobre os mesmos dados com as mesmas regras
7. Dashboard compara métricas dos dois pipelines
```

## Decisões de Design

### Por que 3 shards no Kinesis?
Cada shard suporta 1.000 writes/s. Com 2.000 TPS, 2 shards bastaria mas 3 dá margem pra bursts e permite parallelism factor 3 no Event Source Mapping (1 Lambda por shard).

### Por que DynamoDB como state store?
Alternativas: ElastiCache (Redis), Flink MapState. DynamoDB foi escolhido porque já é usado pros perfis, tem TTL nativo (limpeza automática do estado), e não adiciona mais um serviço. Trade-off: ~5ms de latência por read/write vs ~1ms do Redis.

### Por que batch size 100 e window 1s?
Batch size 100 com window de 1 segundo dá latência de ~1-2s. Batch size maior (500) reduziria custo de invocações mas aumentaria latência pra ~5s. Batch size menor (10) daria latência sub-segundo mas 10x mais invocações Lambda.

### Por que parallelism factor 3?
Um Lambda consumer por shard. Com 3 shards, 3 Lambdas processam em paralelo. Cada uma processa ~667 TPS. Poderia aumentar pra 10 (máximo), mas 3 é suficiente e mantém a ordem por partition key dentro de cada shard.

## Segurança
- IAM roles com least privilege (Lambda só acessa os recursos necessários)
- Encryption at rest (S3 SSE-AES256, DynamoDB encryption default)
- Block Public Access em todos os S3 buckets
- Sem credenciais no código (IAM role via execution role)
