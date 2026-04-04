# Pipeline DATASUS — Capacidades Hospitalares do Brasil

## Problema

O Brasil disponibiliza dados públicos de estabelecimentos de saúde via API do DATASUS (CNES). Porém, os dados brutos apresentam problemas de qualidade que impedem análise direta:

- **1.852 registros duplicados** (mesmo código CNES aparece múltiplas vezes)
- **1.796 estabelecimentos desabilitados** misturados com ativos
- Códigos numéricos sem descrição legível (tipo_unidade = 5 → ?)
- Código de município DATASUS (6 dígitos) incompatível com IBGE (7 dígitos)
- Campos texto com espaços extras e inconsistências

**Este pipeline resolve isso**: ingere dados brutos de 3 APIs públicas, trata os problemas de qualidade e entrega uma base analítica limpa respondendo — *"Qual a distribuição de capacidades hospitalares (centro cirúrgico, obstétrico, neonatal) por estado e município do Brasil, e como isso se relaciona com a população?"*

## Fontes de Dados

| Fonte | API | Registros | Descrição |
|-------|-----|-----------|-----------|
| DATASUS/CNES | `apidadosabertos.saude.gov.br/cnes/estabelecimentos` | ~8.900 | Hospitais (tipo 5, 7, 15) |
| IBGE Localidades | `servicodados.ibge.gov.br/api/v1/localidades/municipios` | 5.571 | Municípios com hierarquia geográfica |
| IBGE SIDRA | `apisidra.ibge.gov.br/values/t/4714/n6/all/v/93/p/2022` | 5.570 | População por município (Censo 2022) |

## Arquitetura

```
DATASUS API ──┐
IBGE Loc. API ┼──▶ Python (ingest.py) ──▶ data/raw/ (Parquet)
IBGE SIDRA ───┘                               │
                                               ▼
                                    dbt + DuckDB (transformação)
                                         │
                                    ┌────┴─────┐
                                    ▼          ▼
                                 staging     marts ──▶ Python (dashboard.py)
                                    │          │              │
                                    ▼          ▼              ▼
                              dbt tests    DuckDB      docs/dashboard.html
                              (20 testes)              (Plotly interativo)
```

## Stack

| Camada | Ferramenta | Justificativa |
|--------|-----------|---------------|
| Ingestão | Python (requests + pyarrow) | Controle de paginação da API (limit=20/page) |
| Storage | Parquet (Snappy) | Colunar, compacto, padrão da indústria |
| Warehouse | DuckDB | SQL analítico local, zero infra, lê Parquet nativo |
| Transformação | dbt-core + dbt-duckdb | Modelagem em camadas, testes, documentação |
| Qualidade | dbt tests (20 testes) | unique, not_null, accepted_values |
| Visualização | Plotly → HTML estático | Dashboard interativo, zero servidor, portátil |
| Orquestração | Makefile | Reproduzível, sem overengineering |

## Como rodar

```bash
# 1. Criar venv e instalar dependências
python3 -m venv .venv
source .venv/bin/activate
pip install -e .

# 2. Pipeline completo (ingestão + transformação + testes)
make all

# 3. Comandos individuais
make ingest      # Ingere dados das APIs → data/raw/
make transform   # Roda modelos dbt → DuckDB
make test        # Roda 20 testes de qualidade
make docs        # Gera documentação dbt navegável
make dashboard   # Gera dashboard HTML interativo em docs/
make clean       # Limpa tudo (dados + DuckDB + dashboard)
```

## Modelagem (Medallion)

```
raw/                      staging/                   marts/
(Parquet bruto)           (limpeza + tipagem)        (modelo analítico)

estabelecimentos.parquet  stg_estabelecimentos       dim_localidades
municipios.parquet        stg_municipios             dim_estabelecimentos
populacao.parquet         stg_populacao              fct_leitos
                                                     agg_leitos_por_uf
                                                     agg_leitos_por_municipio
```

## Problemas de qualidade tratados

| # | Problema | Quantidade | Tratamento |
|---|----------|-----------|-----------|
| 1 | Duplicatas por `codigo_cnes` | 1.852 | Dedup com `row_number()` (mantém mais recente) |
| 2 | Estabelecimentos desabilitados | 1.796 | Filtro por `codigo_motivo_desabilitacao is null` |
| 3 | Códigos sem descrição | 3 tipos | Mapeamento: 5→Hospital Geral, 7→Especializado, 15→Unidade Mista |
| 4 | Join key incompatível DATASUS↔IBGE | 100% dos registros | `left(id_municipio_ibge, 6) = codigo_municipio_datasus` |
| 5 | Campos texto com espaços | Vários | `trim()` em todos os campos texto |
| 6 | Município sem match no IBGE | 1 registro | Inner join (registro do DF com código administrativo) |

**Resultado**: de 8.972 registros brutos → 5.618 hospitais ativos, limpos e únicos.

## Decisões técnicas

- **DuckDB em vez de PostgreSQL**: zero infra, perfeito para pipeline local, lê Parquet nativamente
- **dbt em vez de scripts SQL soltos**: testes, documentação, linhagem e reprodutibilidade
- **Parquet em vez de CSV**: compressão Snappy, tipagem forte, leitura colunar eficiente
- **Makefile em vez de Airflow**: projeto local, sem necessidade de scheduler
- **Score de complexidade**: métrica derivada (0-6) que soma as capacidades do hospital — permite ranking e comparação
- **Métricas per capita**: hospitais por 100k habitantes usando Censo 2022 — normaliza a comparação entre UFs

## Dashboard

O pipeline gera automaticamente um [dashboard HTML interativo](docs/dashboard.html) com 9 visualizações:

1. 🧹 **Tratamento de Qualidade** — waterfall chart mostrando de bruto a limpo
2. 🏥 **Distribuição por UF** — ranking de hospitais por estado
3. 📊 **Per Capita por UF** — hospitais por 100k habitantes
4. 🗺️ **Visão Regional** — comparação entre regiões (absoluto e per capita)
5. 🏗️ **Tipos de Hospital** — distribuição e score de complexidade por tipo
6. 🎯 **Complexidade Hospitalar** — distribuição do score 0-6
7. ⚕️ **Capacidades por UF** — centro cirúrgico, obstétrico e neonatal
8. 🏙️ **Top Municípios** — ranking absoluto
9. 🚨 **Desertos Hospitalares** — municípios >50k hab com menos hospitais per capita

Para gerar: `make dashboard` → abre `docs/dashboard.html` no navegador.
