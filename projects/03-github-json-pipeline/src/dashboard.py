"""
Dashboard do pipeline GitHub — tema verde terminal/hacker.
"""

import duckdb
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
NORM = BASE / "data" / "normalized"
GOLD = BASE / "data" / "analytics"
SAIDA = BASE / "docs" / "dashboard.html"

con = duckdb.connect()

stats = con.sql(f"""
    select
        count(*) as total,
        sum(case when is_pull_request then 1 else 0 end) as prs,
        sum(case when not is_pull_request then 1 else 0 end) as issues,
        sum(case when state = 'closed' then 1 else 0 end) as closed,
        round(median(resolution_hours), 1) as median_res,
        count(distinct user_id) as users
    from read_parquet('{NORM}/fct_issues.parquet')
""").fetchone()
total, prs, issues, closed, median_res, n_users = stats

n_labels = con.sql(f"select count(*) from read_parquet('{NORM}/dim_labels.parquet')").fetchone()[0]
n_tables = 7  # tabelas geradas pelo flatten

repos = con.sql(f"select * from read_parquet('{GOLD}/agg_repos.parquet')").fetchdf()
contribs = con.sql(f"select * from read_parquet('{GOLD}/agg_contributors.parquet') limit 20").fetchdf()
labels = con.sql(f"select * from read_parquet('{GOLD}/agg_labels.parquet') limit 25").fetchdf()
ivp = con.sql(f"select * from read_parquet('{GOLD}/agg_issue_vs_pr.parquet')").fetchdf()

bridge_labels = con.sql(f"select count(*) from read_parquet('{NORM}/bridge_issue_labels.parquet')").fetchone()[0]
bridge_assign = con.sql(f"select count(*) from read_parquet('{NORM}/bridge_issue_assignees.parquet')").fetchone()[0]

con.close()

# paleta verde terminal
VD, VDC, AM, VM, CZ = "#00ff41", "#39ff14", "#ffb000", "#ff3131", "#888"
FUNDO = "#0d1117"
PAPEL = "#161b22"
TEXTO = "#c9d1d9"
GRID = "#21262d"

def _layout(fig, **kw):
    fig.update_layout(plot_bgcolor=FUNDO, paper_bgcolor=PAPEL, font_color=TEXTO,
        title_font_color=VD, xaxis=dict(gridcolor=GRID), yaxis=dict(gridcolor=GRID), **kw)

graficos = []

# 1. Decomposicao: 1 JSON -> 7 tabelas
fig = go.Figure(go.Sankey(
    node=dict(
        label=["JSON bruto (1500)", "fct_issues", "dim_users", "dim_labels",
               "dim_milestones", "bridge_labels", "bridge_assignees", "dim_repos"],
        color=[AM, VD, VDC, VDC, VDC, "#ff6b6b", "#ff6b6b", VDC],
        pad=15, thickness=20,
    ),
    link=dict(
        source=[0, 0, 0, 0, 0, 0, 0],
        target=[1, 2, 3, 4, 5, 6, 7],
        value=[1500, 705, 133, 11, 4693, 905, 3],
        color=["rgba(0,255,65,0.2)"] * 7,
    ),
))
_layout(fig, title="Decomposicao: 1 JSON aninhado -> 7 tabelas relacionais", height=400)
graficos.append(("Flatten Pipeline", fig,
    f"1.500 JSONs aninhados decompostos em 7 tabelas normalizadas: "
    f"1.500 issues, {n_users} users, {n_labels} labels, {bridge_labels} relacoes issue-label, "
    f"{bridge_assign} relacoes issue-assignee."))

# 2. Issues vs PRs por repo
fig = go.Figure()
for repo in repos["repo"]:
    r = ivp[ivp["repo"] == repo]
    iss = r[~r["is_pull_request"]]["total"].values
    pr = r[r["is_pull_request"]]["total"].values
    fig.add_trace(go.Bar(name=f"{repo} (issues)", x=[repo.split('/')[1]], y=iss if len(iss) else [0], marker_color=VD))
    fig.add_trace(go.Bar(name=f"{repo} (PRs)", x=[repo.split('/')[1]], y=pr if len(pr) else [0], marker_color=AM))
fig.update_layout(barmode="group")
_layout(fig, title="Issues vs Pull Requests por repositorio", height=400)
graficos.append(("Issues vs PRs", fig, "Distribuicao entre issues e pull requests em cada repo."))

# 3. Tempo de resolucao por repo
fig = go.Figure()
fig.add_trace(go.Bar(x=repos["repo"].str.split("/").str[1], y=repos["avg_issue_resolution_hours"],
    name="Media", marker_color=VD, text=repos["avg_issue_resolution_hours"], textposition="outside"))
fig.add_trace(go.Bar(x=repos["repo"].str.split("/").str[1], y=repos["median_resolution_hours"],
    name="Mediana", marker_color=AM, text=repos["median_resolution_hours"], textposition="outside"))
fig.update_layout(barmode="group")
_layout(fig, title="Tempo de resolucao (horas)", yaxis_title="Horas", height=400)
graficos.append(("Tempo de Resolucao", fig, "Media vs mediana de tempo de resolucao. Mediana eh mais representativa (outliers puxam a media)."))

# 4. Top contributors
fig = go.Figure(go.Bar(
    y=contribs["login"].iloc[::-1], x=contribs["total_created"].iloc[::-1],
    orientation="h", marker_color=VD,
    text=contribs["total_created"].iloc[::-1], textposition="outside"))
_layout(fig, title="Top 20 contributors (por issues/PRs criados)", xaxis_title="Total criado", height=600)
graficos.append(("Top Contributors", fig, "Contributors mais ativos por volume de issues e PRs criados."))

# 5. Labels mais usados
top_labels = labels.head(20)
fig = go.Figure(go.Bar(
    y=top_labels["label_name"].iloc[::-1], x=top_labels["usage_count"].iloc[::-1],
    orientation="h",
    marker_color=["#" + c if c else VD for c in top_labels["color"].iloc[::-1]],
    text=top_labels["usage_count"].iloc[::-1], textposition="outside"))
_layout(fig, title="Labels mais usados", xaxis_title="Uso", height=600)
graficos.append(("Labels", fig, "Labels com as cores originais do GitHub. Cada label foi extraido de dentro do array aninhado no JSON."))

# 6. Labels vs tempo de resolucao
labels_res = labels[labels["median_resolution_hours"].notna()].sort_values("median_resolution_hours", ascending=False).head(15)
fig = go.Figure(go.Bar(
    y=labels_res["label_name"].iloc[::-1], x=labels_res["median_resolution_hours"].iloc[::-1],
    orientation="h", marker_color=AM,
    text=labels_res["median_resolution_hours"].iloc[::-1].apply(lambda x: f"{x}h"), textposition="outside"))
_layout(fig, title="Labels com maior tempo de resolucao (mediana)", xaxis_title="Horas", height=500)
graficos.append(("Labels vs Resolucao", fig, "Quais labels estao associados a issues que demoram mais pra fechar."))


# --- HTML ---
def _kpi(valor, label):
    v = f"{valor:,}".replace(",", ".") if isinstance(valor, (int, float)) and not isinstance(valor, bool) else str(valor)
    return f'<div class="kpi"><div class="value">{v}</div><div class="label">{label}</div></div>'

html = [f"""<!DOCTYPE html>
<html lang="pt-BR"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Dashboard — GitHub JSON Flatten Pipeline</title>
<script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'SF Mono',SFMono-Regular,Consolas,'Liberation Mono',monospace;background:#010409;color:#c9d1d9}}
.header{{background:#0d1117;border-bottom:2px solid #00ff41;padding:2.5rem;text-align:center}}
.header h1{{font-size:1.4rem;color:#00ff41;letter-spacing:2px}}
.header p{{color:#8b949e;font-size:.85rem;margin-top:.5rem;font-family:-apple-system,sans-serif}}
.kpis{{display:flex;gap:.8rem;padding:1.5rem 2rem;flex-wrap:wrap;justify-content:center}}
.kpi{{background:#0d1117;border:1px solid #21262d;border-radius:6px;padding:1rem 1.3rem;text-align:center;min-width:150px}}
.kpi .value{{font-size:1.6rem;font-weight:700;color:#00ff41}}.kpi .label{{font-size:.75rem;color:#8b949e;margin-top:.3rem;font-family:-apple-system,sans-serif}}
.section{{max-width:1200px;margin:1rem auto;background:#0d1117;border:1px solid #21262d;border-radius:6px;padding:1.5rem}}
.section h2{{font-size:1.1rem;color:#00ff41;margin-bottom:.5rem;letter-spacing:1px}}
.insight{{background:#161b22;border-left:3px solid #ffb000;padding:.7rem 1rem;margin-bottom:1rem;border-radius:0 6px 6px 0;font-size:.82rem;line-height:1.5;font-family:-apple-system,sans-serif;color:#8b949e}}
.footer{{text-align:center;padding:2rem;color:#484f58;font-size:.8rem;font-family:-apple-system,sans-serif}}.footer a{{color:#00ff41}}
</style></head><body>
<div class="header">
    <h1>&gt; github-json-flatten-pipeline</h1>
    <p>1 JSON aninhado &rarr; 7 tabelas relacionais &middot; {total:,} registros de 3 repos</p>
</div>
<div class="kpis">
    {_kpi(total, "Issues + PRs")}
    {_kpi(issues, "Issues")}
    {_kpi(prs, "Pull Requests")}
    {_kpi(n_users, "Users Extraidos")}
    {_kpi(n_labels, "Labels Unicos")}
    {_kpi(n_tables, "Tabelas Geradas")}
    {_kpi(f"{median_res}h", "Mediana Resolucao")}
</div>"""]

for i, (titulo, fig, insight) in enumerate(graficos):
    fig.update_layout(margin=dict(l=60, r=30, t=50, b=50))
    plot = fig.to_html(full_html=False, include_plotlyjs=False, div_id=f"g{i}")
    html.append(f'<div class="section"><h2>{titulo}</h2><div class="insight">{insight}</div>{plot}</div>')

html.append("""<div class="footer">
    Fonte: <a href="https://docs.github.com/en/rest">GitHub API</a> &middot;
    <a href="https://github.com/Leosandre/data-engineering-portfolio">Repositorio</a>
</div></body></html>""")

SAIDA.parent.mkdir(parents=True, exist_ok=True)
SAIDA.write_text("".join(html), encoding="utf-8")
print(f"Dashboard: {SAIDA} ({SAIDA.stat().st_size / 1024:.0f} KB)")
