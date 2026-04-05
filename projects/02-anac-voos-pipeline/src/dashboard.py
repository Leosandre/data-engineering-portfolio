"""
Dashboard HTML com os resultados do pipeline de voos ANAC.
"""

import duckdb
import polars as pl
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
GOLD = BASE / "data" / "gold"
SILVER = BASE / "data" / "silver"
SAIDA = BASE / "docs" / "dashboard.html"
FMT_DT = "%d/%m/%Y %H:%M"

con = duckdb.connect()

# metricas gerais
stats = con.sql(f"""
    select
        count(*) as total,
        sum(case when situacao_voo = 'REALIZADO' then 1 else 0 end) as realizados,
        sum(case when situacao_voo = 'CANCELADO' then 1 else 0 end) as cancelados,
        sum(case when classificacao = 'PONTUAL' then 1 else 0 end) as pontuais,
        round(median(atraso_chegada_min), 1) as mediana_atraso,
        count(distinct icao_empresa) as cias,
        count(distinct icao_origem) as aeroportos
    from read_parquet('{SILVER}/fct_voos.parquet')
""").fetchone()
total, realizados, cancelados, pontuais, mediana_atraso, n_cias, n_aeroportos = stats

cias = con.sql(f"""
    select * from read_parquet('{GOLD}/agg_pontualidade_cia.parquet')
    where total_voos > 1000
    order by total_voos desc limit 20
""").fetchdf()

aeroportos = con.sql(f"""
    select * from read_parquet('{GOLD}/agg_pontualidade_aeroporto.parquet')
    where total_voos > 5000
    order by total_voos desc limit 20
""").fetchdf()

rotas = con.sql(f"""
    select * from read_parquet('{GOLD}/agg_pontualidade_rota.parquet')
    order by total_voos desc limit 20
""").fetchdf()

mensal = con.sql(f"""
    select * from read_parquet('{GOLD}/agg_mensal.parquet')
    order by ano, mes
""").fetchdf()

dia_semana = con.sql(f"""
    select * from read_parquet('{GOLD}/agg_dia_semana.parquet')
    order by dia_semana
""").fetchdf()

hora = con.sql(f"""
    select * from read_parquet('{GOLD}/agg_hora.parquet')
    order by hora_partida
""").fetchdf()

piores_cias = con.sql(f"""
    select * from read_parquet('{GOLD}/agg_pontualidade_cia.parquet')
    where total_voos > 5000
    order by pct_pontualidade asc limit 15
""").fetchdf()

piores_rotas = con.sql(f"""
    select * from read_parquet('{GOLD}/agg_pontualidade_rota.parquet')
    where total_voos > 2000
    order by pct_pontualidade asc limit 15
""").fetchdf()

AZ, VD, VM, LR, RX = "#00b4d8", "#06d6a0", "#ef476f", "#ffd166", "#8338ec"
FUNDO = "#1a1a2e"
PAPEL = "#16213e"
TEXTO = "#e0e0e0"
GRID = "#2a2a4a"

def _layout(fig, **kw):
    fig.update_layout(
        plot_bgcolor=FUNDO, paper_bgcolor=PAPEL,
        font_color=TEXTO, title_font_color="#ffd166",
        xaxis=dict(gridcolor=GRID), yaxis=dict(gridcolor=GRID),
        **kw
    )
graficos = []

# 1. Qualidade
bruto = sum(pl.read_parquet(f).height for f in sorted(Path("data/bronze").glob("vra_*.parquet")))
limpo = total
removidos = bruto - limpo

# recalcula decomposicao real
_raw = pl.concat([pl.read_parquet(f) for f in sorted(Path("data/bronze").glob("vra_*.parquet"))])
for _c in ["icao_origem", "icao_destino"]:
    _raw = _raw.with_columns(pl.col(_c).str.strip_chars().str.to_uppercase())
n_origem_destino = _raw.filter(pl.col("icao_origem") == pl.col("icao_destino")).height
_raw = _raw.with_columns(pl.col("partida_prevista").str.strip_chars().str.to_datetime(FMT_DT, strict=False))
n_sem_data = _raw.filter(pl.col("partida_prevista").is_null()).height
n_outros = removidos - n_origem_destino - n_sem_data
del _raw

fig = go.Figure(go.Waterfall(
    x=["Brutos", "Origem=Destino", "Sem data/periodo", "Duplicatas+outros", "Limpos"],
    y=[bruto, -n_origem_destino, -n_sem_data, -n_outros, limpo],
    measure=["absolute", "relative", "relative", "relative", "total"],
    connector={"line": {"color": "#4a4a6a"}},
    decreasing={"marker": {"color": VM}},
    totals={"marker": {"color": AZ}},
    text=[f"{bruto:,}", f"-{n_origem_destino:,}", f"-{n_sem_data:,}",
          f"{'-' if n_outros > 0 else ''}{n_outros:,}", f"{limpo:,}"],
    textposition="outside",
))
_layout(fig, title="Qualidade: de bruto a limpo", yaxis_title="Voos", showlegend=False, height=400)
removidos = bruto - limpo
graficos.append(("Tratamento de Qualidade", fig,
    f"De {bruto:,} registros brutos, {limpo:,} voos validos ({removidos:,} removidos = {100*removidos/bruto:.1f}%). "
    f"Principais filtros: {n_origem_destino:,} com origem=destino, {n_sem_data:,} sem data de partida."))

# 2. Pontualidade por companhia
fig = make_subplots(rows=1, cols=2, subplot_titles=("% Pontualidade", "% Cancelamentos"))
fig.add_trace(go.Bar(y=cias["icao_empresa"], x=cias["pct_pontualidade"],
    orientation="h", marker_color=VD, showlegend=False), row=1, col=1)
fig.add_trace(go.Bar(y=cias["icao_empresa"], x=cias["pct_cancelados"],
    orientation="h", marker_color=VM, showlegend=False), row=1, col=2)
_layout(fig, title="Pontualidade por companhia (top 20 por volume)", height=600)
graficos.append(("Pontualidade por Companhia", fig,
    "Companhias com mais de 1.000 voos no periodo. Pontualidade = chegou no horario ou antes."))

# 3. Piores companhias
fig = go.Figure(go.Bar(
    y=piores_cias["icao_empresa"].iloc[::-1], x=piores_cias["pct_pontualidade"].iloc[::-1],
    orientation="h", marker_color=VM,
    text=piores_cias["pct_pontualidade"].iloc[::-1].apply(lambda x: f"{x}%"), textposition="outside"))
_layout(fig, title="Piores companhias em pontualidade (>5.000 voos)", xaxis_title="% Pontualidade", height=500)
graficos.append(("Piores Companhias", fig,
    "Companhias com mais de 5.000 voos e menor indice de pontualidade."))

# 4. Sazonalidade mensal
mensal["periodo"] = mensal["ano"].astype(str) + "-" + mensal["mes"].astype(str).str.zfill(2)
fig = make_subplots(rows=2, cols=1, subplot_titles=("Volume de voos", "% Pontualidade"),
                    shared_xaxes=True, vertical_spacing=0.1)
fig.add_trace(go.Bar(x=mensal["periodo"], y=mensal["total_voos"], marker_color=AZ,
    showlegend=False), row=1, col=1)
fig.add_trace(go.Scatter(x=mensal["periodo"], y=mensal["pct_pontualidade"], mode="lines+markers",
    line_color=LR, showlegend=False), row=2, col=1)
_layout(fig, title="Sazonalidade mensal", height=500)
graficos.append(("Sazonalidade", fig,
    "Volume de voos e pontualidade ao longo dos meses. Dezembro e janeiro (ferias) tendem a ter mais voos e mais atrasos."))

# 5. Dia da semana
dias = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sab", "Dom"]
fig = make_subplots(rows=1, cols=2, subplot_titles=("Volume", "% Pontualidade"))
fig.add_trace(go.Bar(x=dias, y=dia_semana["total_voos"], marker_color=AZ, showlegend=False), row=1, col=1)
fig.add_trace(go.Bar(x=dias, y=dia_semana["pct_pontualidade"], marker_color=VD, showlegend=False), row=1, col=2)
_layout(fig, title="Padrao por dia da semana", height=400)
graficos.append(("Dia da Semana", fig, "Sabado tem menos voos e melhor pontualidade."))

# 6. Hora do dia
fig = make_subplots(rows=1, cols=2, subplot_titles=("Volume", "Mediana atraso (min)"))
fig.add_trace(go.Bar(x=hora["hora_partida"], y=hora["total_voos"], marker_color=AZ, showlegend=False), row=1, col=1)
fig.add_trace(go.Bar(x=hora["hora_partida"], y=hora["mediana_atraso_min"], marker_color=LR, showlegend=False), row=1, col=2)
_layout(fig, title="Padrao por hora do dia", height=400)
graficos.append(("Hora do Dia", fig, "Atrasos acumulam ao longo do dia — voos da manha sao mais pontuais."))

# 7. Top rotas
fig = go.Figure(go.Bar(
    y=rotas["rota"].iloc[::-1], x=rotas["total_voos"].iloc[::-1],
    orientation="h", marker_color=AZ,
    text=rotas["total_voos"].iloc[::-1], textposition="outside"))
_layout(fig, title="Top 20 rotas por volume", xaxis_title="Voos", height=600)
graficos.append(("Top Rotas", fig, "Ponte aerea (CGH-SDU) e rotas pra Brasilia dominam."))

# 8. Piores rotas
fig = go.Figure(go.Bar(
    y=piores_rotas["rota"].iloc[::-1], x=piores_rotas["pct_pontualidade"].iloc[::-1],
    orientation="h", marker_color=VM,
    text=piores_rotas["pct_pontualidade"].iloc[::-1].apply(lambda x: f"{x}%"), textposition="outside"))
_layout(fig, title="Piores rotas em pontualidade (>2.000 voos)", xaxis_title="% Pontualidade", height=500)
graficos.append(("Piores Rotas", fig, "Rotas com mais de 2.000 voos e menor pontualidade."))

# 9. Aeroportos
aero_pont = aeroportos.sort_values("pct_pontualidade", ascending=True).head(15)
fig = go.Figure(go.Bar(
    y=aero_pont["icao_aeroporto"].iloc[::-1], x=aero_pont["pct_pontualidade"].iloc[::-1],
    orientation="h", marker_color=VM,
    text=aero_pont["pct_pontualidade"].iloc[::-1].apply(lambda x: f"{x}%"), textposition="outside"))
_layout(fig, title="Aeroportos com pior pontualidade (>5.000 voos)", xaxis_title="% Pontualidade", height=500)
graficos.append(("Piores Aeroportos", fig, "Aeroportos de origem com mais de 5.000 voos e menor pontualidade."))


# --- HTML ---
def _kpi(valor, label):
    v = f"{valor:,}".replace(",", ".") if isinstance(valor, (int, float)) else str(valor)
    return f'<div class="kpi"><div class="value">{v}</div><div class="label">{label}</div></div>'

pct_pont = round(100 * pontuais / realizados, 1)
pct_canc = round(100 * cancelados / total, 1)

html = [f"""<!DOCTYPE html>
<html lang="pt-BR"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Dashboard — Pontualidade da Aviacao Civil Brasileira</title>
<script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'SF Mono',SFMono-Regular,Consolas,'Liberation Mono',monospace;background:#0f0f1a;color:#c8c8d0}}
.header{{background:linear-gradient(135deg,#0a0a1a 0%,#1a1a3e 50%,#0d1b2a 100%);color:#fff;padding:2.5rem;text-align:center;border-bottom:3px solid #ffd166}}
.header h1{{font-size:1.6rem;margin-bottom:.5rem;letter-spacing:1px;text-transform:uppercase;color:#ffd166}}
.header p{{opacity:.7;font-size:.9rem;font-family:-apple-system,sans-serif}}
.kpis{{display:flex;gap:1rem;padding:1.5rem 2rem;flex-wrap:wrap;justify-content:center}}
.kpi{{background:#16213e;border:1px solid #2a2a4a;border-radius:8px;padding:1.2rem 1.5rem;text-align:center;min-width:160px}}
.kpi .value{{font-size:1.8rem;font-weight:700;color:#ffd166}}.kpi .label{{font-size:.8rem;color:#8888a0;margin-top:.3rem;font-family:-apple-system,sans-serif}}
.section{{max-width:1200px;margin:1.5rem auto;background:#16213e;border:1px solid #2a2a4a;border-radius:8px;padding:1.5rem}}
.section h2{{font-size:1.2rem;margin-bottom:.5rem;color:#ffd166;letter-spacing:.5px}}
.insight{{background:#1a1a2e;border-left:3px solid #00b4d8;padding:.8rem 1rem;margin-bottom:1rem;border-radius:0 6px 6px 0;font-size:.85rem;line-height:1.5;font-family:-apple-system,sans-serif;color:#a0a0b8}}
.footer{{text-align:center;padding:2rem;color:#555;font-size:.8rem;font-family:-apple-system,sans-serif}}.footer a{{color:#00b4d8}}
</style></head><body>
<div class="header">
    <h1>Pontualidade da Aviacao Civil Brasileira</h1>
    <p>2022-2023 &middot; {total:,} voos analisados &middot; Fonte: ANAC/VRA</p>
</div>
<div class="kpis">
    {_kpi(total, "Voos Analisados")}
    {_kpi(f"{pct_pont}%", "Pontualidade")}
    {_kpi(f"{pct_canc}%", "Cancelamentos")}
    {_kpi(f"{mediana_atraso} min", "Mediana Atraso")}
    {_kpi(n_cias, "Companhias")}
    {_kpi(n_aeroportos, "Aeroportos")}
</div>"""]

for i, (titulo, fig, insight) in enumerate(graficos):
    fig.update_layout(margin=dict(l=60, r=30, t=50, b=50))
    plot = fig.to_html(full_html=False, include_plotlyjs=False, div_id=f"g{i}")
    html.append(f'<div class="section"><h2>{titulo}</h2><div class="insight">{insight}</div>{plot}</div>')

html.append("""<div class="footer">
    Fonte: <a href="https://www.gov.br/anac/pt-br/assuntos/dados-e-estatisticas">ANAC</a> &middot;
    <a href="https://github.com/Leosandre/data-engineering-portfolio">GitHub</a>
</div></body></html>""")

SAIDA.parent.mkdir(parents=True, exist_ok=True)
SAIDA.write_text("".join(html), encoding="utf-8")
print(f"Dashboard: {SAIDA} ({SAIDA.stat().st_size / 1024:.0f} KB)")
