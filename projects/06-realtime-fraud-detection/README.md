# 06 — Detecção de Fraude em Tempo Real

> Pipeline de detecção de fraude financeira com latência sub-5-segundos, comparando quantitativamente com a abordagem batch pra demonstrar por que tempo real importa.

## O Problema

Empresas financeiras perdem dinheiro porque seus sistemas antifraude rodam em batch — a cada 15 minutos, a cada hora, ou pior, uma vez por dia. Quando o analista vê o alerta, a transação fraudulenta já foi aprovada e o dinheiro já saiu. A latência entre a transação e a detecção é o gap que o fraudador explora.

A pergunta que esse projeto responde: **quanto uma empresa perde por detectar fraude em batch em vez de tempo real?**

## Resultados

### Run Final: 3.86M transações em 30 minutos (2.140 TPS)

| Métrica | Streaming (Lambda) | Batch (15min) |
|---------|-------------------|---------------|
| Transações processadas | 2.08M | 150K (amostra local) |
| Lambda invocações | 28.479 | — |
| Lambda erros | 0 | — |
| Lambda duration média | 380ms | — |
| Precision | ~72% | 76.4% |
| Recall | ~83% | 51.6% |
| Alertas | 26.597 | 2.088 |

O batch perdeu **quase metade das fraudes**. Não porque as regras são diferentes — são exatamente as mesmas 5 regras. A diferença é o timing:

- **Velocity attack** (6 transações em 2 minutos): o streaming detecta porque mantém estado em tempo real — cada transação incrementa o contador. O batch vê todas as transações de uma vez, mas com timestamps espalhados, nem sempre 6+ caem dentro da janela.
- **Impossible travel** (São Paulo → Manaus em 20 minutos): o streaming compara com a última localização conhecida, que é atualizada a cada transação. O batch compara com a transação anterior na ordenação, que pode não ser a mais recente se houver transações de outros tipos no meio.
- **Micro-testing** (3 transações de R$2 seguidas de uma de R$5.000): o streaming mantém a sequência em tempo real. O batch pode ter transações normais intercaladas que quebram o padrão.

## 🏗️ Arquitetura

```
Producer (Python, 2.000 TPS)
    │
    ▼
Kinesis Data Streams (3 shards, provisioned)
    │
    ├──▶ Lambda (fraud-detector, Python 3.12)
    │       ├── Enrichment: DynamoDB (perfil do cliente)
    │       ├── State: DynamoDB (velocity counter, última localização)
    │       ├── 5 regras de detecção
    │       │
    │       ├──▶ SNS → SQS (alertas em tempo real, score >= 0.7)
    │       │
    │       └──▶ Kinesis → Firehose → S3 (todas as transações + score)
    │
    └──▶ Batch Detection (mesmas 5 regras, roda a cada 15min)
            └──▶ S3 (resultados batch)

DynamoDB
    ├── customer-profiles (10K clientes, perfil financeiro)
    └── detection-state (velocity timestamps, última localização, TTL 5min)
```

## Por que Lambda e não Flink?

Avaliei 3 opções pra consumir o Kinesis:

| Critério | Lambda | Managed Flink (PyFlink) | Managed Flink (Java) |
|----------|--------|------------------------|---------------------|
| Linguagem | Python puro | Python (Table API only) | Java |
| Windowing nativo | ❌ | ✅ (SQL) | ✅ (DataStream API) |
| Async I/O (DynamoDB) | ✅ (boto3 nativo) | ❌ (não suportado na Table API) | ✅ |
| KeyedProcessFunction | ❌ | ❌ (só Table API) | ✅ |
| Exactly-once | ❌ (at-least-once) | ✅ | ✅ |
| Deploy | zip Python | zip + Maven + JDK (uber-jar) | Maven + JDK |
| Custo mínimo | $0 quando ocioso | 1 KPU = $0.11/h sempre | 1 KPU = $0.11/h sempre |
| Latência | ~0.5-2s | ~1-3s | ~1-3s |

**Escolhi Lambda** porque:

1. **PyFlink não resolve o problema.** O Managed Flink com Python só suporta a Table API (SQL). Pra regras stateful como velocity (que precisa de KeyedProcessFunction) e enrichment do DynamoDB (que precisa de async I/O), a Table API não serve. Teria que usar UDFs em SQL, que são limitadas pra lógica condicional complexa como cálculo de distância haversine.

2. **Flink Java resolveria, mas adiciona complexidade desnecessária.** Precisaria de Maven, JDK, uber-jar com connectors. O código Java pra Flink não é trivial — são ~500 linhas só pra configurar sources, sinks, e serializers. Pra uma POC que roda 30 minutos, esse overhead não se justifica.

3. **Lambda faz o que preciso.** Consome do Kinesis via Event Source Mapping (batch size 100, window 1s), enriquece com DynamoDB, aplica as regras, e emite alertas. A latência é sub-2-segundos. O trade-off é não ter windowing nativo — resolvi usando DynamoDB como state store com TTL de 5 minutos.

**O trade-off que aceitei:** sem exactly-once semantics. Lambda é at-least-once — em caso de falha, o batch é reprocessado. Pra detecção de fraude isso é aceitável: um alerta duplicado é melhor que uma fraude perdida. Em produção, adicionaria idempotência via transaction_id no DynamoDB.

## 🔍 As 5 Regras de Detecção

Cada regra retorna um score de 0.0 a 1.0. O score final é o máximo entre todas as regras que dispararam. Alerta se score >= 0.7.

| Regra | O que detecta | Como funciona | Score |
|-------|--------------|---------------|-------|
| **Velocity** | >6 transações em 2 minutos | Mantém lista de timestamps no DynamoDB, limpa fora da janela | 0.90 |
| **Impossible Travel** | Cidades distantes em <1h | Haversine entre localização atual e última conhecida. >500km em <1h = fraude | 0.85 |
| **Amount Anomaly** | Valor muito acima do padrão | Compara com média + 3× desvio padrão do histórico do cliente | 0.75 |
| **Account Takeover** | Device novo + valor alto | Device diferente do último conhecido + transação >R$5.000 | 0.95 |
| **Micro-testing** | Sequência de valores baixos + 1 alto | 3+ transações <R$5 seguidas de 1 >R$2.000 | 0.88 |

## 📊 Dataset Sintético

- **10.000 clientes** com perfis realistas (3 segmentos: premium, regular, básico)
- **15 cidades brasileiras** com coordenadas reais
- **~2% de fraude** injetada (5 padrões)
- **7% de late events** (timestamp atrasado em 1-30 segundos)
- **2.000+ transações/segundo** sustentados

Os dados são gerados por `src/generate_transactions.py`. O campo `is_fraud` é o ground truth — ele NÃO é enviado pro pipeline de detecção. Serve só pra calcular precision/recall depois.

## 💰 Custo

| Serviço | Sem Free Tier | Com Free Tier |
|---------|--------------|---------------|
| Kinesis (4 shards, 30min) | $0.08 | $0.08 |
| Lambda (3.6M invocações) | $3.72 | $0.00 ✅ |
| DynamoDB (on-demand) | $5.40 | $5.40 |
| Firehose | $0.10 | $0.10 |
| S3 | $0.25 | $0.10 |
| SNS/SQS | $0.07 | $0.00 ✅ |
| CloudWatch | $0.30 | $0.00 ✅ |
| **Total** | **~$10** | **~$6** |

Todos os recursos têm a tag `Project=fraud-detection-poc` pra rastreio no Cost Explorer.

## 🚀 Como Reproduzir

```bash
# 1. Setup
make setup

# 2. Gerar dados locais (pra teste)
make generate

# 3. Deploy infra AWS
make deploy

# 4. Popular DynamoDB com perfis
make seed

# 5. Enviar transações pro Kinesis (30min, 2.000 TPS)
#    A Lambda já está processando automaticamente
make produce

# 6. Rodar detecção batch (local, pra comparação)
make batch

# 7. IMPORTANTE: extrair evidências ANTES de destruir
make extract

# 8. Gerar dashboard (usa dados locais)
make dashboard

# 9. Destruir tudo
make teardown

# 10. Confirmar que não sobrou nada
make verify-cleanup
```

## 📂 Estrutura

```
06-realtime-fraud-detection/
├── cloudformation/
│   ├── 01-storage.yaml          # Kinesis, DynamoDB, S3, Firehose
│   ├── 02-messaging.yaml        # SNS, SQS
│   └── 03-compute.yaml          # Lambda, DynamoDB state, IAM, Event Source Mapping
├── lambda/
│   └── fraud_detector.py        # 5 regras de detecção + enrichment DynamoDB
├── src/
│   ├── generate_transactions.py # gerador sintético (5 padrões de fraude)
│   └── producer.py              # Kinesis producer (2.000+ TPS, ThreadPoolExecutor)
├── scripts/
│   ├── deploy.sh                # deploy CloudFormation + Lambda code
│   ├── teardown.sh              # destrói tudo (ordem reversa)
│   ├── verify_cleanup.sh        # confirma 0 recursos restantes
│   ├── extract_evidence.py      # baixa S3, DynamoDB, SQS, CloudWatch pra local
│   ├── batch_detection.py       # mesmas 5 regras em batch
│   └── generate_dashboard.py    # dashboard Plotly HTML
├── report/
│   └── index.html               # dashboard comparativo
├── docs/
│   ├── ARCHITECTURE.md
│   └── COST_ESTIMATE.md
└── Makefile                     # orquestração (12 targets)
```

## ⚠️ Lições Aprendidas

**1. PyFlink Table API não suporta stateful processing complexo**
Comecei planejando usar Managed Flink com PyFlink. Descobri que a Table API Python não suporta KeyedProcessFunction nem async I/O — só SQL com UDFs limitadas. Pra usar DataStream API, precisaria de Java. Pivotei pra Lambda.

**2. SNS publish por alerta mata o throughput da Lambda**
Na primeira versão, a Lambda publicava 1 mensagem SNS por alerta. Com 500 records por batch e ~50% de alertas, eram ~250 chamadas SNS por invocação — 5+ segundos só de SNS. A Lambda levava 7.2s por batch e não acompanhava o input. Resolvi publicando 1 mensagem resumo por batch. Duration caiu de 7.2s pra 380ms.

**3. BatchGetItem em vez de GetItem individual**
A primeira versão fazia 1 GetItem por record (perfil + estado = 2 chamadas × 500 records = 1.000 chamadas DynamoDB por invocação). Mudei pra BatchGetItem (max 100 keys por chamada) — reduziu pra ~10 chamadas por invocação. Mas precisei adicionar `dynamodb:BatchGetItem` na IAM policy (não estava na original).

**4. Velocity detection com event-time vs processing-time**
Usar `time.time()` (processing time) pra velocity causava falsos positivos massivos — com paralelismo 10, múltiplas Lambdas processavam o mesmo shard e o estado no DynamoDB acumulava timestamps de todas. Mudei pra event-time (timestamp da transação) e janela de 10 segundos com threshold >3.

**5. Batch perde fraudes por design, não por bug**
O recall do batch (51.6%) é menor que o do streaming com as mesmas regras. A diferença não é nas regras — é no timing. O streaming vê cada transação no momento que acontece e mantém estado atualizado. O batch vê tudo de uma vez, mas perde a sequência temporal.

**6. DynamoDB on-demand é o maior custo**
Com 3.6M reads + 3.6M writes, DynamoDB custou ~$5.40 dos ~$10 totais. Em produção, usaria DAX (cache) ou provisioned capacity com auto-scaling pra reduzir custo.

## Autor

[Leonardo Sandre](https://github.com/Leosandre)
