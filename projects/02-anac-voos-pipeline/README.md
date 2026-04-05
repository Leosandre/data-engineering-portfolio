# Pipeline ANAC — Pontualidade da Aviacao Civil Brasileira

## Problema

A ANAC publica dados de todos os voos realizados no Brasil (VRA - Voos Realizados pela Aviacao). Sao milhoes de registros com informacoes de atrasos, cancelamentos, rotas e companhias aereas. Porem os dados brutos tem problemas serios:

- CSVs mensais com **nomes inconsistentes** (`VRA_01_2022.csv` vs `vra_2022_09.csv` vs `VRA_AbriL_2019.csv`)
- **Schema muda entre anos** (ordem das colunas Origem/Destino invertida entre 2022 e 2024)
- **Encoding variado** (UTF-8 BOM, cedilha diferente no header entre periodos)
- Codigos ICAO de aeroportos e companhias **sem descricao**
- Horarios de partida/chegada como texto, sem calculo de atraso
- Cancelamentos sem contexto (sazonalidade, rota, companhia)

**Este pipeline resolve isso**: ingere 2 anos de dados (2022-2023, ~1.5M voos), unifica schemas, trata qualidade e entrega uma base analitica respondendo — *"Quais companhias, rotas e aeroportos tem os piores indices de pontualidade e cancelamento? Existem padroes sazonais?"*

## Fontes de Dados

| Fonte | Formato | Volume |
|-------|---------|--------|
| ANAC VRA (2022-2023) | CSV mensal (;) | ~70-87k registros/mes |
| ANAC Aeroportos | CSV | ~700 aeroportos |
| IBGE Municipios | JSON API | 5.571 municipios |

## Arquitetura

```
ANAC (CSVs mensais) ──▶ Python (download + unificacao) ──▶ data/bronze/ (Parquet)
ANAC (Aeroportos)   ──▶                                         │
IBGE (Municipios)   ──▶                                         ▼
                                                          Polars (ETL)
                                                               │
                                                         ┌─────┴──────┐
                                                         ▼            ▼
                                                   data/silver/   data/gold/
                                                    (Parquet)     (Parquet)
                                                         │            │
                                                         ▼            ▼
                                                  Great Expectations  DuckDB
                                                  (validacao)        (analise)
                                                                       │
                                                                       ▼
                                                                  Dashboard HTML
```

## Stack

| Camada | Ferramenta | Justificativa |
|--------|-----------|---------------|
| Ingestao | Python + httpx (async) | Download paralelo de 24 CSVs |
| Processing | Polars | Mais rapido que pandas pra CSVs grandes, lazy evaluation |
| Storage | Parquet (particionado) | Colunar, particionado por ano/mes |
| Qualidade | Great Expectations | Suites de validacao por camada, data docs |
| Analise | DuckDB | SQL analitico sobre Parquet |
| Visualizacao | Plotly (HTML estatico) | Dashboard interativo |
| Orquestracao | Makefile + Python | Simples, reproduzivel |

## Como rodar

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .

make all          # pipeline completo
make ingest       # baixa CSVs da ANAC
make transform    # bronze -> silver -> gold
make validate     # Great Expectations
make dashboard    # gera docs/dashboard.html
make clean        # limpa tudo
```

## Modelagem (Medallion)

```
bronze/                     silver/                      gold/
(CSV convertido)            (limpo, schema unificado)    (modelo analitico)

vra_YYYY_MM.parquet         fct_voos                     agg_pontualidade_cia
aeroportos.parquet          dim_aeroportos               agg_pontualidade_rota
                            dim_companhias               agg_pontualidade_aeroporto
                            dim_tempo                    agg_cancelamentos_mensal
                                                         agg_sazonalidade
```

## Problemas de qualidade tratados

| # | Problema | Tratamento |
|---|----------|-----------|
| 1 | Nomes de arquivo inconsistentes | Scraping da pagina ANAC pra pegar URLs reais |
| 2 | Schema diferente entre anos | Leitura por nome de coluna (nao posicao) |
| 3 | Encoding variado (BOM, cedilha) | Deteccao automatica + normalizacao |
| 4 | Duplicatas entre arquivos | Dedup por chave composta (cia + voo + data + origem) |
| 5 | Atrasos negativos | Classificados como "pontual" (chegou antes) |
| 6 | Voos origem = destino | Filtrados como inconsistentes |
| 7 | Datas/horarios invalidos | Parse com fallback, registros invalidos logados |
| 8 | Cancelamentos sem contexto | Enriquecidos com sazonalidade e rota |
