# Pipeline GitHub — Flatten de JSON Aninhado em Tabelas Relacionais

## O Problema

Qualquer engenheiro de dados que integra APIs de SaaS (Jira, Salesforce, HubSpot, GitHub) enfrenta o mesmo problema: a API retorna JSONs profundamente aninhados, com objetos dentro de objetos, arrays de objetos, campos que existem em uns registros e nao em outros. Esse JSON precisa virar tabelas relacionais normalizadas pra ser consumido por analistas, dashboards e modelos de ML.

Isso nao eh um problema teorico. Eh o trabalho do dia a dia.

A API do GitHub eh um caso real desse problema. Um unico JSON de issue tem:

```json
{
  "id": 4195282104,
  "number": 138178,
  "title": "Make cleanup aware of uid differences",
  "state": "closed",
  "user": {
    "login": "liggitt",
    "id": 980082,
    "type": "User"
  },
  "labels": [
    {"id": 105152717, "name": "area/test", "color": "0052cc"},
    {"id": 114528223, "name": "priority/important-soon", "color": "eb6420"},
    {"id": 148225179, "name": "lgtm", "color": "15dd18"}
  ],
  "assignees": [
    {"login": "Jefftree", "id": 7691399}
  ],
  "milestone": {
    "id": 14036449,
    "title": "v1.36",
    "state": "open",
    "open_issues": 1,
    "closed_issues": 1050
  },
  "pull_request": {
    "url": "https://api.github.com/repos/kubernetes/kubernetes/pulls/138178"
  },
  "reactions": {
    "total_count": 2,
    "+1": 0, "-1": 0, "laugh": 0, "hooray": 0,
    "confused": 0, "heart": 2, "rocket": 0, "eyes": 0
  },
  "created_at": "2026-04-02T15:14:45Z",
  "closed_at": "2026-04-02T17:39:09Z",
  "comments": 10
}
```

**33 campos no nivel raiz.** Dentro deles: 4 objetos aninhados (`user`, `milestone`, `pull_request`, `reactions`) e 2 arrays de objetos (`labels`, `assignees`). Cada issue tem em media **7.623 bytes** de JSON. E o pior: os campos sao opcionais — `milestone` existe em apenas 19.8% das issues, `assignees` em 47.9%.

Se voce tentar jogar isso direto numa tabela, vai ter colunas com JSON stringificado, arrays que ninguem consegue filtrar, e NULLs por todo lado.

## O Que Resolvemos

Decompus cada JSON aninhado em **7 tabelas relacionais normalizadas** com integridade referencial:

```
1 JSON (7.6 KB, 33 campos, 3 niveis)
│
├──▶ fct_issues (1.500 linhas)
│    Campos flat: id, title, state, created_at, closed_at, resolution_hours
│    FKs: user_id → dim_users, milestone_id → dim_milestones
│    Reactions flatten: reactions."+1" → reactions_thumbs_up (coluna)
│    Derivados: is_pull_request (bool), resolution_hours (calculado)
│
├──▶ dim_users (705 unicos)
│    Extraidos de: issue.user{} + issue.assignees[].each{}
│    Dedup global: mesmo user aparece como author e assignee
│
├──▶ dim_labels (133 unicos)
│    Extraidos de: issue.labels[].each{}
│
├──▶ dim_milestones (11 unicos)
│    Extraidos de: issue.milestone{} (quando existe — 19.8% das issues)
│
├──▶ dim_repos (3)
│    Flatten de: repo.owner{}, repo.license{}
│
├──▶ bridge_issue_labels (4.693 relacoes)
│    Relacao N:N — uma issue pode ter 0 a 35 labels
│
└──▶ bridge_issue_assignees (905 relacoes)
     Relacao N:N — uma issue pode ter 0 a N assignees
```

**Resultado:** 0 orfaos em todas as foreign keys. Qualquer analista consegue fazer `JOIN` entre as tabelas e responder perguntas como "qual o tempo medio de resolucao de issues com label `bug`?" sem precisar parsear JSON.

## Desafios Tecnicos Encontrados e Como Resolvemos

### 1. Campos opcionais e polimorficos

O campo `milestone` existe em apenas 19.8% das issues. O campo `pull_request` existe em 67.9% (porque o endpoint `/issues` retorna issues E PRs misturados). Se voce faz `json["milestone"]["title"]` sem checar, o pipeline quebra.

**Solucao:** checagem explicita com fallback pra None em cada campo aninhado. Flag `is_pull_request` derivado da presenca do campo `pull_request`.

### 2. Arrays de tamanho variavel (labels, assignees)

Uma issue pode ter 0 labels ou 35 labels. Nao da pra criar colunas `label_1`, `label_2`, ..., `label_35`. Isso eh o classico problema de "como representar relacao N:N vindo de um JSON".

**Solucao:** bridge tables. Cada par (issue_id, label_id) vira uma linha em `bridge_issue_labels`. Permite filtrar, agrupar e contar labels por issue com SQL padrao.

### 3. Usuarios duplicados entre contextos

O mesmo usuario pode aparecer como `user` (autor da issue) e como `assignee` (responsavel). Se voce extrai separadamente, tem duplicatas na dim_users.

**Solucao:** dicionario global `all_users[user_id] = {...}` que acumula usuarios de todos os contextos. Dedup natural pela chave do dict.

### 4. Reactions como objeto com chaves dinamicas

O campo `reactions` tem chaves como `"+1"`, `"-1"`, `"heart"` — nao sao nomes de campo validos em SQL. E a estrutura eh fixa mas as chaves parecem dinamicas.

**Solucao:** flatten explicito: `reactions["+1"]` → coluna `reactions_thumbs_up`. Cada chave vira uma coluna tipada na fct_issues.

### 5. Rate limiting da API

Sem token: 60 requests/hora. Com token: 5.000/hora. O pipeline precisa funcionar nos dois cenarios.

**Solucao:** leitura do header `x-ratelimit-remaining` a cada request. Se restam < 2 requests, calcula o tempo ate o reset e espera. Cache local (se o JSONL ja existe, nao re-baixa).

### 6. Paginacao cursor-based

A API do GitHub usa paginacao via header `Link` com cursors, nao offset simples. O formato eh `<url>; rel="next"`.

**Solucao:** parse do header Link com regex pra extrair a URL da proxima pagina.

### 7. Schema inconsistente entre registros (Polars)

Polars infere o schema pelas primeiras linhas. Se a primeira issue tem `milestone_id = null` e a centesima tem `milestone_id = 14036449` (int), o Polars quebra com `ComputeError: could not append value of type i64`.

**Solucao:** `infer_schema_length=len(fct_rows)` — forca o Polars a olhar todos os registros antes de decidir o tipo.

## O Que Melhora Com Esse Problema Resolvido

**Antes (JSON bruto):**
- Analista precisa saber Python pra parsear JSON
- Impossivel filtrar por label ou assignee com SQL
- Sem integridade referencial (quem eh o user? existe na base?)
- Sem metricas derivadas (tempo de resolucao, contagem de labels)
- Dados de 3 repos em 3 arquivos separados sem schema comum

**Depois (tabelas normalizadas):**
- Qualquer analista faz `SELECT * FROM fct_issues JOIN dim_labels USING (label_id)` 
- Tempo de resolucao pre-calculado em horas
- Bridge tables permitem analise N:N (quais labels estao associados a issues lentas?)
- Usuarios deduplicados globalmente entre repos
- Schema unico e tipado — pronto pra BI, dbt, ou qualquer ferramenta SQL

## Antes vs Depois: Queries que Agora Sao Possiveis

### Query 1: "Quais labels estao associados a issues que demoram mais pra resolver?"

**ANTES (JSON bruto) — o analista precisaria fazer isso em Python:**

```python
import json

# abrir arquivo de 10 MB, parsear linha por linha
resultados = {}
with open("issues_kubernetes_kubernetes.jsonl") as f:
    for line in f:
        issue = json.loads(line)
        if not issue.get("closed_at") or not issue.get("created_at"):
            continue
        # calcular resolucao manualmente
        created = datetime.fromisoformat(issue["created_at"].replace("Z", "+00:00"))
        closed = datetime.fromisoformat(issue["closed_at"].replace("Z", "+00:00"))
        horas = (closed - created).total_seconds() / 3600
        # iterar no array aninhado de labels
        for label in issue.get("labels", []):  # pode ser None!
            nome = label.get("name", "???")    # campo pode nao existir!
            if nome not in resultados:
                resultados[nome] = []
            resultados[nome].append(horas)
# ... e ainda precisa calcular media/mediana manualmente
```

**DEPOIS (tabelas normalizadas) — SQL puro, 6 linhas:**

```sql
select
    l.name as label,
    count(*) as issues,
    round(median(i.resolution_hours), 1) as mediana_horas
from fct_issues i
join bridge_issue_labels bl on i.issue_id = bl.issue_id
join dim_labels l on bl.label_id = l.label_id
where i.resolution_hours is not null
group by l.name
having count(*) >= 10
order by mediana_horas desc limit 10;
```

```
┌─────────────────────────┬────────┬───────────────┐
│          label          │ issues │ mediana_horas │
├─────────────────────────┼────────┼───────────────┤
│ sig/network             │     17 │          46.5 │
│ do-not-merge/hold       │     11 │          41.9 │
│ priority/important-soon │     16 │          36.4 │
│ kind/flake              │     33 │          34.3 │
│ kind/feature            │     16 │          32.6 │
│ sig/node                │     77 │          32.5 │
│ area/kubelet            │     43 │          25.7 │
└─────────────────────────┴────────┴───────────────┘
```

O JOIN entre 3 tabelas (`fct_issues` → `bridge_issue_labels` → `dim_labels`) so eh possivel porque o flatten decompos o array `labels[]` em uma bridge table. Sem isso, o analista precisaria parsear JSON em Python.

---

### Query 2: "Quais contributors resolvem issues mais rapido?"

**ANTES (JSON bruto) — o analista precisaria:**

```python
# extrair user.login de dentro do objeto aninhado
# cruzar com closed_at - created_at
# filtrar apenas issues (nao PRs) — mas o campo pull_request
# eh um objeto que pode ou nao existir
# agrupar por login, calcular mediana...
for issue in issues:
    login = issue.get("user", {}).get("login", "???")  # 2 niveis
    is_pr = "pull_request" in issue                     # campo polimorfico
    if is_pr:
        continue
    # ... mais 20 linhas de codigo
```

**DEPOIS — SQL puro:**

```sql
select
    u.login,
    count(*) as issues_fechadas,
    round(median(i.resolution_hours), 1) as mediana_horas,
    sum(i.reactions_total) as total_reactions
from fct_issues i
join dim_users u on i.user_id = u.user_id
where i.state = 'closed'
  and i.resolution_hours is not null
  and not i.is_pull_request
group by u.login
having count(*) >= 5
order by mediana_horas asc limit 10;
```

O `JOIN` com `dim_users` funciona porque o flatten extraiu `user.login` e `user.id` do objeto aninhado pra uma tabela separada. O campo `is_pull_request` foi derivado da presenca do objeto `pull_request{}` no JSON original. O `resolution_hours` foi pre-calculado a partir de `created_at` e `closed_at`.

Nenhuma dessas operacoes seria possivel com SQL direto sobre o JSON bruto.

## Stack

| Camada | Ferramenta | Por que essa e nao outra |
|--------|-----------|--------------------------|
| Ingestao | httpx (sync) | Rate limiting precisa de controle fino por request. Async nao ajuda com 60 req/hora |
| Raw | JSONL | Preserva o JSON original — se o flatten tiver bug, nao precisa re-baixar |
| Flatten | Python puro + Polars | O flatten eh logica de negocio (quais campos extrair, como tratar opcionais). Polars so entra pra serializar em Parquet |
| Storage | Parquet | Colunar, tipado, comprimido. DuckDB le nativamente |
| Analise | DuckDB | SQL analitico sobre Parquet, zero infra |
| Dashboard | Plotly (HTML) | Interativo, autocontido, sem servidor |

## Como Rodar

```bash
python3 -m venv .venv && source .venv/bin/activate && pip install -e .

make all          # pipeline completo
make ingest       # baixa JSONs da API do GitHub (usa cache)
make flatten      # JSON aninhado → 7 tabelas Parquet
make analyze      # gera agregacoes analiticas
make dashboard    # gera docs/dashboard.html
```

Para usar com token (5.000 req/hora em vez de 60):
```bash
export GITHUB_TOKEN=ghp_seu_token_aqui
make all
```

## Numeros

| Metrica | Valor |
|---------|-------|
| Issues + PRs ingeridos | 1.500 |
| Tamanho raw (JSONL) | 10.9 MB |
| Tabelas geradas | 7 |
| Usuarios unicos extraidos | 705 |
| Labels unicos | 133 |
| Relacoes issue-label (N:N) | 4.693 |
| Relacoes issue-assignee (N:N) | 905 |
| Orfaos em FKs | 0 |
| Labels por issue (max) | 35 |
| Campos opcionais tratados | 5 (milestone, PR, labels, assignees, resolution) |
