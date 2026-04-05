# Data Platform AWS — Multi-Source Integration, Entity Resolution e ML

## O Problema

Uma empresa de e-commerce recebe dados de clientes e vendas de 4 fontes diferentes. Ninguem confia nos dados porque:

- O mesmo cliente aparece em 3 fontes com nomes escritos diferentes
- O CPF vem em 10+ formatos diferentes entre fontes (`123.456.789-01`, `12345678901`, `123 456 789 01`, `123.456.78901`, `0123456789-01`, etc.)
- Cada fonte diz um email diferente pro mesmo cliente
- As vendas no ERP nao batem com as do gateway de pagamento
- O time de negocio quer previsao de vendas e deteccao de churn, mas nao consegue nem confiar nos dados atuais
- Dados sensiveis (CPF) precisam ser mascarados pra alguns times mas visiveis pra outros

Este projeto resolve tudo: unifica, reconcilia, limpa, mascara, modela, preve e visualiza.

## As 4 Fontes

| Fonte | Formato | Volume | Problemas injetados |
|-------|---------|--------|---------------------|
| ERP (PostgreSQL) | Parquet | 10k clientes, 100k pedidos, 300k itens | Nomes UPPERCASE, CPF em 10+ formatos diferentes |
| CRM (API REST) | JSON aninhado | 8k contatos (70% overlap ERP) | Nomes abreviados, 15% emails divergentes, CPF em formato diferente do ERP, 20% sem CPF |
| Gateway Pagamento | CSV pipe-delimited | 95k transacoes | Virgula decimal, datas formato americano, 5% valores divergentes, CPF em 70% dos registros |
| Google Analytics | JSON aninhado | 50k sessoes | 40% anonimo, eventos aninhados, sem CPF |

## Entity Resolution: Matching em 3 Fases com Enriquecimento

### Por que enriquecimento e nao cascata pura?

Existem duas abordagens pra matching multi-chave:

**Cascata pura:** CPF bate → pronto, nem olha o email. Email so entra se CPF nao bateu.

**Enriquecimento (a que usei):** CPF faz o match principal. Depois o email passa por TODOS os registros — confirma quem o CPF ja pegou e encontra novos.

O total de matches eh o mesmo nas duas abordagens. A diferenca eh o que voce sabe sobre cada match:

| Aspecto | Cascata | Enriquecimento |
|---------|---------|----------------|
| Match CPF + email bate | Voce nao sabe (email nem foi verificado) | Sabe: `match_types = cpf,email_confirmado` |
| CPF bate mas email nao | Nao detecta | Detecta: possivel fraude ou erro cadastral |
| Resiliencia | Se CPF degradar, perde o match | Email segura como fallback |
| Custo computacional | Marginalmente menor | Praticamente igual (email eh O(1) por lookup em hash) |
| Auditoria | "Bateu por CPF" | "Bateu por CPF e email confirmou" — auditor entende que eh solido |

**Decisao:** enriquecimento. O custo extra eh desprezivel (lookup em hash map), mas o valor da informacao adicional eh alto — score de confianca, resiliencia, deteccao de inconsistencia.

### Resultados

```
Fase 1 — CPF normalizado (match principal)
  10+ formatos de CPF normalizados pra 11 digitos
  3.558 matches

Fase 2 — Email (enriquecimento)
  2.914 confirmaram o match do CPF (redundancia)
  1.814 novos matches (CPF nao bateu, email sim)

Fase 3 — Nome fuzzy (ultimo recurso)
  Levenshtein token_sort_ratio >= 85, filtrado por estado
  309 matches

Total: 5.681 matches
  high (CPF):      3.558
  medium (email):  1.814
  low (fuzzy):       309
```

### Golden Record com Regras de Prioridade

Cada campo da golden record vem da fonte mais confiavel pra aquele campo:

| Campo | Prioridade | Justificativa |
|-------|-----------|---------------|
| nome | ERP > CRM | ERP eh o cadastro oficial |
| email | CRM > ERP | CRM eh o canal de comunicacao, email mais atualizado |
| telefone | CRM > ERP | Mesmo motivo do email |
| CPF | ERP | Fonte primaria de documento |
| endereco | ERP > CRM | ERP tem endereco de faturamento (mais confiavel) |
| segmento | CRM | Unica fonte que tem |

Linhagem campo a campo (de 10.000 clientes ERP):
```
email:    5.670 vieram do CRM, 4.330 do ERP
telefone: 3.928 do CRM, 6.072 do ERP
CPF:      100% do ERP
```

### Resultados da Golden Record

| Metrica | Valor |
|---------|-------|
| Golden records totais | 12.319 |
| Com ambas fontes (ERP+CRM) | 5.678 |
| So ERP | 4.322 |
| So CRM (leads) | 2.319 |

### Reconciliacao de Vendas (ERP vs Gateway)

| Status | Quantidade | Descricao |
|--------|-----------|-----------|
| match_exato | 90.367 | Valor ERP = Gateway (tolerancia R$0.05) |
| divergente | 4.633 | Valor diferente entre fontes |
| so_erp | 5.000 | Venda no ERP sem transacao no Gateway |

## Mascaramento de Dados Sensiveis (LGPD)

### O problema

Dois times consomem os dados:
- **Time de BI:** precisa de analytics mas NAO pode ver CPF real
- **Gerentes:** precisam ver CPF real pra atendimento ao cliente

### Opcoes avaliadas

| Abordagem | Como funciona | Veredicto |
|-----------|--------------|-----------|
| **Redshift Dynamic Data Masking** | Politica SQL que mascara por role. BI ve `***456789**`, gerente ve real | ESCOLHIDA |
| Lake Formation column-level | Bloqueia coluna inteira. BI nao ve CPF, gerente ve | Rejeitada: binario (tudo ou nada), nao mascara parcialmente |
| KMS encrypt/decrypt | Encripta campo. So quem tem key ve | Rejeitada: complexidade alta, precisa de codigo pra decrypt, nao funciona transparente no QuickSight |

### Por que Redshift Dynamic Data Masking

1. **Mascaramento parcial:** BI ve `***456789**` (nao eh tudo ou nada — ve parte do dado pra conferencia)
2. **Zero codigo:** eh uma politica SQL, nao precisa de Lambda/Glue pra mascarar
3. **Transparente no QuickSight:** o dashboard conecta no Redshift com a role do usuario, masking eh automatico
4. **Auditavel:** politica versionada, sabe-se quem ve o que
5. **Complementa Lake Formation:** LF controla quem acessa o S3 (camada bronze/silver nao acessivel pelo BI), DDM controla como ve no Redshift

### Implementacao

```sql
-- Politica de mascaramento
CREATE MASKING POLICY mask_cpf
  WITH (cpf VARCHAR)
  USING ('***' || SUBSTRING(cpf, 4, 6) || '**');

-- Aplica so pro BI (gerentes nao tem policy = veem real)
ATTACH MASKING POLICY mask_cpf
  ON golden_customers(cpf)
  TO ROLE role_bi;
```

Resultado:
```
-- user_bi:
SELECT cpf FROM golden_customers LIMIT 1;
→ ***456789**

-- user_gerente:
SELECT cpf FROM golden_customers LIMIT 1;
→ 12345678901
```

## Arquitetura AWS

```
4 Fontes (ERP, CRM, Gateway, Analytics)
    |
    v
S3 Bronze (raw, por fonte, acesso restrito via Lake Formation)
    |
    v
Glue Job: bronze_to_silver.py (limpeza por fonte, normalizacao CPF)
    |
    v
S3 Silver (limpo, schema padronizado)
    |
    v
Glue Job: silver_to_gold.py (matching 3 fases, golden record, reconciliacao)
    |
    v
S3 Gold (golden records, vendas reconciliadas, linhagem)
    |
    +---> Redshift Serverless (serving, Dynamic Data Masking por role)
    |         |
    |         +---> QuickSight (dashboards: BI ve mascarado, gerentes veem real)
    |
    +---> SageMaker (forecast vendas + churn prediction)

Orquestracao: Step Functions (bronze->silver->gold, retry com backoff)
IaC: Terraform (S3, Glue, Redshift, Step Functions, IAM)
```

## Stack

| Camada | Servico | Por que esse e nao outro |
|--------|---------|--------------------------|
| Storage | S3 | Data lake com camadas, encryption AES256, versionamento |
| Catalogo | Glue Data Catalog | Metastore central pra Athena + Redshift Spectrum |
| ETL | Glue (PySpark) | Volume justifica Spark, serverless, auto-scaling de DPUs |
| Orquestracao | Step Functions | < 10 jobs com dependencias simples (nao precisa de Airflow) |
| Serving | Redshift Serverless | BI com joins complexos, Dynamic Data Masking, workload intermitente |
| Mascaramento | Redshift DDM + Lake Formation | DDM pra mascaramento parcial, LF pra controle de acesso ao S3 |
| ML | SageMaker | Notebooks + training + endpoints |
| BI | QuickSight | Integra nativamente com Redshift, respeita roles/masking |
| IaC | Terraform | Infra reproduzivel, state remoto |

## Deploy na AWS

### Pre-requisitos

- Conta AWS com free tier ou budget definido
- AWS CLI configurado com profile
- Terraform >= 1.5
- VPC default na regiao (se nao existir: `aws ec2 create-default-vpc`)

### Passo a passo

```bash
# 1. Gerar dados sinteticos
make generate && make reconcile

# 2. Treinar modelos ML
python src/ml_models.py

# 3. Provisionar infra
cd terraform
terraform init
terraform apply -var='aws_profile=seu-profile' -var='redshift_admin_password=SuaSenha123!'

# 4. Upload dados pro S3
BUCKET=$(terraform output -raw s3_bucket)
aws s3 sync ../data/synthetic/ s3://$BUCKET/bronze/ --profile seu-profile
aws s3 sync ../data/reconciled/ s3://$BUCKET/gold/reconciled/ --profile seu-profile
aws s3 sync ../data/ml/ s3://$BUCKET/gold/ml/ --profile seu-profile

# 5. Carregar no Redshift
python ../src/load_redshift.py

# 6. Configurar QuickSight
# Ativar QuickSight Enterprise (free trial 30 dias) pelo console
# Depois rodar:
python ../src/setup_quicksight.py

# 7. IMPORTANTE: destruir tudo quando terminar
terraform destroy -var='aws_profile=seu-profile' -var='redshift_admin_password=SuaSenha123!'
```

### Custos

| Servico | Custo/hora | Custo estimado (2h de demo) |
|---------|-----------|---------------------------|
| Redshift Serverless (8 RPU) | $3.60/h | $7.20 |
| S3 (84 MB) | desprezivel | $0.00 |
| Glue (catalog only) | gratuito | $0.00 |
| QuickSight (free trial) | $0.00 | $0.00 |
| **Total** | | **~$7.20** |

### Licoes aprendidas no deploy

Problemas reais que encontrei e como resolvi:

**1. VPC default nao existia**
O Redshift Serverless exige VPC default. A conta nao tinha.
Solucao: `aws ec2 create-default-vpc`

**2. IAM role do Glue nao funciona no Redshift COPY**
Tentei usar a mesma role do Glue pro COPY do Redshift. Retornou `UnauthorizedException`.
Motivo: a trust policy da role do Glue tem `Principal: glue.amazonaws.com`, mas o Redshift precisa de `Principal: redshift.amazonaws.com`.
Solucao: criar role separada pro Redshift com trust policy correta.

**3. Redshift nao aceita conexao externa por padrao**
O workgroup eh criado com `publiclyAccessible: false`. Pra conectar de fora da VPC (ex: script local), precisa:
- Habilitar `publicly_accessible = true` no workgroup
- Abrir porta 5439 no security group
Em producao, restringir o CIDR pro IP do escritorio/VPN.

**4. Parquet com large_string nao carrega no Redshift**
Polars gera Parquet com tipo `large_string`. Redshift nao suporta — retorna "incompatible Parquet schema".
Solucao: converter pra `string` com PyArrow antes do upload: `col.cast(pa.string())`

**5. QuickSight API exige permissao RestoreAnalysis**
A API `create_analysis` falha silenciosamente (status `CREATION_FAILED`) se a lista de permissoes nao incluir `quicksight:RestoreAnalysis`.
O erro so aparece no `describe_analysis`, nao no retorno do `create_analysis`.

## Estrutura

```
04-aws-data-platform/
├── terraform/
│   └── main.tf                    # S3, Glue, Redshift, IAM, Step Functions, Security Group
├── src/
│   ├── generate_data.py           # gerador de dados sinteticos (4 fontes)
│   ├── reconcile.py               # reconciliacao local (3 fases matching + golden record)
│   ├── ml_models.py               # churn (XGBoost) + forecast (Prophet)
│   └── setup_quicksight.py        # cria data source + datasets no QuickSight
├── glue_jobs/
│   ├── bronze_to_silver.py        # limpeza por fonte (PySpark)
│   └── silver_to_gold.py          # matching + golden record + reconciliacao (PySpark)
├── step_functions/
│   └── pipeline.asl.json          # state machine (bronze -> silver -> gold)
├── redshift/
│   └── masking_policy.sql         # Dynamic Data Masking (role_bi vs role_gerentes)
├── data/                          # gitignored
│   ├── synthetic/                 # dados gerados
│   ├── reconciled/                # resultado da reconciliacao
│   └── ml/                        # features + scores + forecast
└── docs/
```

## Machine Learning

### Churn Prediction (XGBoost)

**Objetivo:** identificar clientes com alta probabilidade de parar de comprar, permitindo acoes preventivas (cupons, contato proativo).

**Features (RFM + comportamentais):**

| Feature | Descricao | Importancia |
|---------|-----------|-------------|
| frequency | Total de pedidos no periodo | 0.814 |
| recency_days | Dias desde a ultima compra | 0.111 |
| monthly_frequency | Pedidos por mes | 0.021 |
| monetary | Valor total gasto | 0.020 |
| tenure_days | Dias entre primeiro e ultimo pedido | 0.018 |
| avg_ticket | Ticket medio por pedido | 0.016 |
| spend_trend | Gasto recente vs historico | 0.000 |

**Resultados:**

| Metrica | Valor |
|---------|-------|
| AUC-ROC | 0.980 |
| Precision (churned) | 64% |
| Recall (churned) | 94% |
| Accuracy | 92% |
| Churn rate no dataset | 14.6% |

**Interpretacao:** o modelo captura 94% dos clientes que vao churnar (recall alto), com 64% de precisao. Isso significa que de cada 100 clientes marcados como "risco alto", 64 realmente vao churnar. Os 36 restantes recebem uma acao preventiva desnecessaria — custo baixo comparado a perder o cliente. Com 3 anos de dados, a feature mais importante passou a ser `frequency` (0.814) em vez de `recency` — o historico mais longo permite ao modelo identificar padroes de frequencia de compra como principal indicador de churn.

**Output:** tabela `churn_scores` com probabilidade e nivel de risco (alto >70%, medio 40-70%, baixo <40%) por cliente. Carregada no Redshift pra consumo no QuickSight.

### Forecast de Vendas (Prophet)

**Objetivo:** prever receita semanal pros proximos 90 dias, permitindo planejamento de estoque e metas.

**Resultados:**

| Metrica | Valor |
|---------|-------|
| MAPE (erro medio) | 7.1% |
| Periodo de treino | 158 semanas (~3 anos) |
| Periodo de forecast | 26 semanas (~6 meses) |
| Sazonalidade detectada | Semanal + anual |

**Output:** tabela `forecast_sales` com receita prevista + intervalo de confianca por semana. Carregada no Redshift.

### Disclaimer: dados sinteticos

As metricas de ML sao reais (train/test split, cross-validation no Prophet), mas treinadas sobre dados sinteticos com padroes injetados. Em producao, os numeros seriam diferentes:
- **Churn AUC** seria menor porque churn real tem causas externas (concorrencia, experiencia ruim) que nao estao nas features
- **Forecast MAPE** dependeria da volatilidade real do negocio

O objetivo aqui eh demonstrar o pipeline end-to-end (feature engineering → treino → avaliacao → deploy), nao produzir um modelo definitivo.

### Por que 3 anos de dados

Estendemos a base de 2 pra 3 anos (2022-2024) por dois motivos:
1. **Forecast:** Prophet precisa de pelo menos 2 ciclos anuais completos pra capturar sazonalidade. Com 3 anos, o MAPE caiu de 11.5% pra 7.1%
2. **Churn:** com mais historico, a feature `frequency` (0.814) superou `recency` (0.111) como principal indicador — o modelo consegue distinguir clientes que compram pouco (padrao normal) de clientes que pararam de comprar (churn)

## QuickSight Dashboards (planejados)

5 dashboards conectados ao Redshift Serverless. O mascaramento de CPF eh automatico via Dynamic Data Masking — BI ve mascarado, gerentes veem real, sem nenhuma configuracao no QuickSight.

### Dashboard 1: Visao 360 do Cliente

**Publico:** Gerentes

**Conteudo:**
- Filtro por cliente (busca por nome, email ou CPF)
- Card com golden record: nome, email, CPF (real pra gerentes), telefone, cidade, segmento
- Indicador de risco de churn (alto/medio/baixo) com cor
- Historico de compras (timeline)
- De qual fonte veio cada campo (linhagem visual)
- Tags e interacoes do CRM

**Fonte de dados:** `golden_customers` JOIN `churn_scores` JOIN `field_lineage` JOIN `reconciled_sales`

### Dashboard 2: Vendas — Real vs Previsto

**Publico:** Gerentes + BI

**Conteudo:**
- Grafico de linha: receita semanal real vs forecast com banda de confianca
- KPIs: receita acumulada, ticket medio, pedidos/semana
- Filtro por periodo e categoria de produto
- Tabela de top produtos por receita
- Indicador de tendencia (crescimento vs queda)

**Fonte de dados:** `reconciled_sales` JOIN `forecast_sales` JOIN `erp_products`

### Dashboard 3: Churn Risk

**Publico:** Gerentes

**Conteudo:**
- Lista de clientes em risco alto, ordenados por probabilidade
- Para cada cliente: ultima compra, valor total, frequencia, score
- Grafico de distribuicao dos scores (quantos em cada faixa)
- Features que mais contribuem pro risco (feature importance global)
- Acao sugerida por faixa (alto: contato telefonico, medio: cupom por email, baixo: monitorar)

**Fonte de dados:** `churn_scores` JOIN `golden_customers` JOIN `rfm_features`

### Dashboard 4: Reconciliacao de Fontes

**Publico:** BI / Data Quality

**Conteudo:**
- Funil de matching: CPF (3.558) → email novos (1.814) → fuzzy (309)
- % de match por nivel de confianca (high/medium/low)
- Vendas: match exato vs divergente vs so_erp (pie chart)
- Tabela de divergencias: order_id, valor ERP, valor Gateway, diferenca
- Metricas de qualidade por fonte: completude de CPF, email, telefone

**Fonte de dados:** `customer_matches` JOIN `reconciled_sales` JOIN `field_lineage`

### Dashboard 5: Data Quality

**Publico:** BI / Engenharia de Dados

**Conteudo:**
- Completude por campo e por fonte (heatmap)
- Volume de registros por fonte (barras)
- Conflitos resolvidos: quantos campos vieram de cada fonte (linhagem agregada)
- Freshness: data da ultima atualizacao por tabela
- Alertas: campos com completude < 90%

**Fonte de dados:** `field_lineage` + metricas calculadas dos dados gold

## Numeros

| Metrica | Valor |
|---------|-------|
| Clientes ERP | 10.000 |
| Contatos CRM | 8.000 (5.600 overlap, 2.400 leads) |
| Transacoes Gateway | 95.000 |
| Sessoes Analytics | 50.000 |
| Pedidos | 100.000 |
| Itens de pedido | ~300.000 |
| Matches CPF | 3.558 |
| Matches email (novos) | 1.814 |
| Matches email (confirmacao CPF) | 2.914 |
| Matches fuzzy | 309 |
| Golden records | 12.319 |
| Vendas reconciliadas (match exato) | 90.367 |
| Vendas divergentes | 4.633 |
| Churn rate | 14.6% |
| Churn AUC-ROC | 0.980 |
| Forecast MAPE | 7.1% |
| Forecast horizonte | 26 semanas (6 meses) |
| Formatos de CPF tratados | 10+ |
