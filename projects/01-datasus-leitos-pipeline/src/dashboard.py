"""
Le os dados do DuckDB e gera um dashboard HTML estatico em docs/dashboard.html.
Usa Plotly pra montar os graficos — o HTML final eh autocontido (so depende do CDN do Plotly).
"""

import duckdb
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
DB = str(BASE / "data" / "datasus_leitos.duckdb")
SAIDA = BASE / "docs" / "dashboard.html"

con = duckdb.connect(DB, read_only=True)

# Metricas de qualidade (antes vs depois da limpeza)
quality = con.sql("""
    with bruto as (
        select count(*) as total from read_parquet('data/raw/estabelecimentos.parquet')
    ),
    limpo as (
        select count(*) as total from stg_estabelecimentos
    ),
    problemas as (
        select
            count(*) - count(distinct codigo_cnes) as duplicatas,
            sum(case when codigo_motivo_desabilitacao_estabelecimento is not null
                then 1 else 0 end) as desabilitados
        from read_parquet('data/raw/estabelecimentos.parquet')
    )
    select b.total as bruto, l.total as limpo, p.duplicatas, p.desabilitados
    from bruto b, limpo l, problemas p
""").fetchone()

por_uf = con.sql("select * from agg_leitos_por_uf order by total_hospitais desc").fetchdf()

top_municipios = con.sql("""
    select * from agg_leitos_por_municipio order by total_hospitais desc limit 20
""").fetchdf()

desertos = con.sql("""
    select * from agg_leitos_por_municipio
    where populacao > 50000
    order by hospitais_por_100k_hab asc limit 20
""").fetchdf()

por_tipo = con.sql("""
    select tipo_unidade, count(*) as qtd, round(avg(score_complexidade), 2) as score_medio
    from fct_leitos group by 1 order by 2 desc
""").fetchdf()

dist_score = con.sql("""
    select score_complexidade, count(*) as qtd from fct_leitos group by 1 order by 1
""").fetchdf()

por_regiao = con.sql("""
    select
        l.nome_regiao,
        count(*) as total_hospitais,
        round(100000.0 * count(*) / nullif(sum(l.populacao), 0), 2) as hospitais_por_100k_hab,
        round(avg(f.score_complexidade), 2) as score_medio
    from fct_leitos f
    join dim_localidades l on f.codigo_municipio = l.codigo_municipio
    group by 1 order by 2 desc
""").fetchdf()

capacidades = con.sql("""
    select sigla_uf, total_hospitais,
        hospitais_com_centro_cirurgico, hospitais_com_centro_obstetrico,
        hospitais_com_centro_neonatal
    from agg_leitos_por_uf order by total_hospitais desc limit 15
""").fetchdf()

total_municipios = con.sql("select count(*) from agg_leitos_por_municipio").fetchone()[0]

con.close()

# paleta
AZ, VD, VM, LR, RX, CZ = "#1f77b4", "#2ca02c", "#d62728", "#ff7f0e", "#9467bd", "#7f7f7f"
FUNDO = "#fafafa"

graficos = []

# --- 1. Waterfall de qualidade ---
bruto, limpo, dupes, desab = quality
removidos = bruto - limpo
outros = removidos - dupes - desab

fig = go.Figure(go.Waterfall(
    x=["Registros brutos", "Duplicatas", "Desabilitados", "Outros filtros", "Registros limpos"],
    y=[bruto, -dupes, -desab, -outros, limpo],
    measure=["absolute", "relative", "relative", "relative", "total"],
    connector={"line": {"color": CZ}},
    decreasing={"marker": {"color": VM}},
    totals={"marker": {"color": AZ}},
    text=[f"{bruto:,}", f"-{dupes:,}", f"-{desab:,}", f"-{outros:,}", f"{limpo:,}"],
    textposition="outside",
))
fig.update_layout(title="Qualidade dos Dados: de bruto a limpo",
                  yaxis_title="Registros", showlegend=False, height=400, plot_bgcolor=FUNDO)
graficos.append(("Tratamento de Qualidade", fig,
    f"De <b>{bruto:,}</b> registros brutos, o pipeline entregou <b>{limpo:,}</b> hospitais ativos e unicos. "
    f"Removidos: <b>{dupes:,}</b> duplicatas, <b>{desab:,}</b> desabilitados."))

# --- 2. Hospitais por UF ---
fig = go.Figure(go.Bar(
    x=por_uf["sigla_uf"], y=por_uf["total_hospitais"],
    marker_color=AZ, text=por_uf["total_hospitais"], textposition="outside"))
fig.update_layout(title="Hospitais ativos por estado", xaxis_title="UF",
                  yaxis_title="Hospitais", height=450, plot_bgcolor=FUNDO)
graficos.append(("Distribuicao por UF", fig,
    f"MG lidera com {por_uf.iloc[0]['total_hospitais']} hospitais, seguido por BA e SP."))

# --- 3. Per capita por UF ---
uf_ord = por_uf.sort_values("hospitais_por_100k_hab", ascending=True)
fig = go.Figure(go.Bar(
    y=uf_ord["sigla_uf"], x=uf_ord["hospitais_por_100k_hab"],
    orientation="h", marker_color=VD,
    text=uf_ord["hospitais_por_100k_hab"], textposition="outside"))
fig.update_layout(title="Hospitais por 100 mil habitantes (por UF)",
                  xaxis_title="Hospitais / 100k hab", height=650, plot_bgcolor=FUNDO)
graficos.append(("Per Capita por UF", fig,
    "Per capita normaliza a comparacao. Norte e Nordeste tem mais hospitais por habitante "
    "(muitas unidades mistas), enquanto SP e RJ concentram em grandes centros."))

# --- 4. Por regiao ---
cores = [AZ, LR, VD, VM, RX]
fig = make_subplots(rows=1, cols=2, subplot_titles=("Total", "Por 100k hab"))
fig.add_trace(go.Bar(x=por_regiao["nome_regiao"], y=por_regiao["total_hospitais"],
    marker_color=cores[:len(por_regiao)], text=por_regiao["total_hospitais"],
    textposition="outside", showlegend=False), row=1, col=1)
reg_pc = por_regiao.sort_values("hospitais_por_100k_hab", ascending=False)
fig.add_trace(go.Bar(x=reg_pc["nome_regiao"], y=reg_pc["hospitais_por_100k_hab"],
    marker_color=cores[:len(reg_pc)], text=reg_pc["hospitais_por_100k_hab"],
    textposition="outside", showlegend=False), row=1, col=2)
fig.update_layout(title="Visao por regiao", height=400, plot_bgcolor=FUNDO)
graficos.append(("Visao Regional", fig,
    "Nordeste lidera em absoluto. Per capita, a distribuicao entre regioes eh mais equilibrada."))

# --- 5. Tipo de unidade ---
fig = make_subplots(rows=1, cols=2, specs=[[{"type": "pie"}, {"type": "bar"}]],
                    subplot_titles=("Por tipo", "Score medio de complexidade"))
fig.add_trace(go.Pie(labels=por_tipo["tipo_unidade"], values=por_tipo["qtd"],
    marker_colors=[AZ, LR, VD], textinfo="label+percent+value"), row=1, col=1)
fig.add_trace(go.Bar(x=por_tipo["tipo_unidade"], y=por_tipo["score_medio"],
    marker_color=[AZ, LR, VD], text=por_tipo["score_medio"],
    textposition="outside", showlegend=False), row=1, col=2)
fig.update_layout(title="Tipos de unidade hospitalar", height=400, plot_bgcolor=FUNDO)
graficos.append(("Tipos de Hospital", fig,
    "Hospitais Gerais sao maioria e tem o maior score de complexidade (5.03/6)."))

# --- 6. Distribuicao do score ---
fig = go.Figure(go.Bar(x=dist_score["score_complexidade"], y=dist_score["qtd"],
    marker_color=AZ, text=dist_score["qtd"], textposition="outside"))
fig.update_layout(
    title="Score de complexidade hospitalar (0-6)",
    xaxis_title="Score (cirurgico + obstetrico + neonatal + hospitalar + apoio + ambulatorial)",
    yaxis_title="Hospitais", height=400, plot_bgcolor=FUNDO)
graficos.append(("Complexidade Hospitalar", fig,
    "Maioria tem score 5 ou 6. Score baixo indica hospitais especializados (ex: psiquiatricos)."))

# --- 7. Capacidades por UF ---
fig = go.Figure()
for col, label, cor in [
    ("hospitais_com_centro_cirurgico", "Centro Cirurgico", AZ),
    ("hospitais_com_centro_obstetrico", "Centro Obstetrico", VD),
    ("hospitais_com_centro_neonatal", "Centro Neonatal", LR),
]:
    fig.add_trace(go.Bar(x=capacidades["sigla_uf"], y=capacidades[col], name=label, marker_color=cor))
fig.update_layout(title="Capacidades hospitalares por UF (top 15)", barmode="group",
                  xaxis_title="UF", yaxis_title="Hospitais", height=450, plot_bgcolor=FUNDO)
graficos.append(("Capacidades por UF", fig,
    "Centro cirurgico eh a capacidade mais comum. Centro neonatal eh a mais escassa."))

# --- 8. Top municipios ---
t = top_municipios.iloc[::-1]
fig = go.Figure(go.Bar(
    y=t["nome_municipio"] + " - " + t["sigla_uf"], x=t["total_hospitais"],
    orientation="h", marker_color=AZ, text=t["total_hospitais"], textposition="outside"))
fig.update_layout(title="Top 20 municipios por numero de hospitais",
                  xaxis_title="Hospitais", height=600, plot_bgcolor=FUNDO)
graficos.append(("Top Municipios", fig, "Capitais dominam o ranking absoluto."))

# --- 9. Desertos hospitalares ---
d = desertos.iloc[::-1]
fig = go.Figure(go.Bar(
    y=d["nome_municipio"] + " - " + d["sigla_uf"], x=d["hospitais_por_100k_hab"],
    orientation="h", marker_color=VM, text=d["hospitais_por_100k_hab"], textposition="outside"))
fig.update_layout(title="Desertos hospitalares: municipios >50k hab com menos hospitais per capita",
                  xaxis_title="Hospitais / 100k hab", height=600, plot_bgcolor=FUNDO)
graficos.append(("Desertos Hospitalares", fig,
    "Municipios com mais de 50 mil habitantes e pouquissimos hospitais per capita. "
    "Potenciais areas com dificuldade de acesso a atendimento."))


# --- Monta o HTML ---

def _kpi(valor, label):
    v = f"{valor:,}".replace(",", ".") if isinstance(valor, int) else str(valor)
    return f'<div class="kpi"><div class="value">{v}</div><div class="label">{label}</div></div>'

html = [f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Dashboard — Capacidades Hospitalares do Brasil</title>
<script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; background:#f0f2f5; color:#333; }}
.header {{ background:linear-gradient(135deg,#1a365d,#2b6cb0); color:#fff; padding:2rem; text-align:center; }}
.header h1 {{ font-size:1.8rem; margin-bottom:.5rem; }}
.header p {{ opacity:.9; }}
.kpis {{ display:flex; gap:1rem; padding:1.5rem 2rem; flex-wrap:wrap; justify-content:center; }}
.kpi {{ background:#fff; border-radius:12px; padding:1.2rem 1.5rem; text-align:center; min-width:180px; box-shadow:0 2px 8px rgba(0,0,0,.08); }}
.kpi .value {{ font-size:2rem; font-weight:700; color:#1a365d; }}
.kpi .label {{ font-size:.85rem; color:#666; margin-top:.3rem; }}
.section {{ max-width:1200px; margin:1.5rem auto; background:#fff; border-radius:12px; padding:1.5rem; box-shadow:0 2px 8px rgba(0,0,0,.08); }}
.section h2 {{ font-size:1.3rem; margin-bottom:.5rem; color:#1a365d; }}
.insight {{ background:#ebf8ff; border-left:4px solid #2b6cb0; padding:.8rem 1rem; margin-bottom:1rem; border-radius:0 8px 8px 0; font-size:.9rem; line-height:1.5; }}
.footer {{ text-align:center; padding:2rem; color:#999; font-size:.85rem; }}
.footer a {{ color:#2b6cb0; }}
</style>
</head>
<body>
<div class="header">
    <h1>Capacidades Hospitalares do Brasil</h1>
    <p>Pipeline de dados: DATASUS/CNES + IBGE &middot; Tratamento com dbt + DuckDB</p>
</div>
<div class="kpis">
    {_kpi(bruto, "Registros Brutos")}
    {_kpi(limpo, "Hospitais Ativos")}
    {_kpi(removidos, "Registros Removidos")}
    {_kpi(27, "UFs Cobertas")}
    {_kpi(total_municipios, "Municipios com Hospitais")}
</div>
"""]

for i, (titulo, fig, insight) in enumerate(graficos):
    fig.update_layout(margin=dict(l=60, r=30, t=50, b=50))
    plot = fig.to_html(full_html=False, include_plotlyjs=False, div_id=f"g{i}")
    html.append(f"""
<div class="section">
    <h2>{titulo}</h2>
    <div class="insight">{insight}</div>
    {plot}
</div>""")

html.append("""
<div class="footer">
    Fonte: <a href="https://apidadosabertos.saude.gov.br">DATASUS</a> +
    <a href="https://servicodados.ibge.gov.br">IBGE</a> &middot;
    <a href="https://github.com/Leosandre/data-engineering-portfolio">GitHub</a>
</div>
</body></html>""")

SAIDA.parent.mkdir(parents=True, exist_ok=True)
SAIDA.write_text("".join(html), encoding="utf-8")
print(f"Dashboard: {SAIDA} ({SAIDA.stat().st_size / 1024:.0f} KB)")
