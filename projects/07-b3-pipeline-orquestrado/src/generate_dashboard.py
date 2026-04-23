"""
Gera dashboard HTML + PNGs com insights financeiros a partir do Redshift.
Roda após o backfill, antes do teardown.

Uso:
    python generate_dashboard.py --workgroup b3-pipeline-wg --output docs/
"""

import argparse
import json
import logging
import os
import time

import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def query_redshift(client, workgroup, database, sql):
    resp = client.execute_statement(WorkgroupName=workgroup, Database=database, Sql=sql)
    stmt_id = resp["Id"]
    while True:
        status = client.describe_statement(Id=stmt_id)
        if status["Status"] in ("FINISHED", "FAILED", "ABORTED"):
            break
        time.sleep(1)
    if status["Status"] == "FAILED":
        raise RuntimeError(status.get("Error"))

    result = client.get_statement_result(Id=stmt_id)
    cols = [c["name"] for c in result["ColumnMetadata"]]
    rows = []
    for record in result["Records"]:
        row = []
        for field in record:
            val = list(field.values())[0]
            row.append(val)
        rows.append(row)
    return pd.DataFrame(rows, columns=cols)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workgroup", default="b3-pipeline-wg")
    parser.add_argument("--database", default="dev")
    parser.add_argument("--output", default="docs")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    client = boto3.client("redshift-data")
    wg, db = args.workgroup, args.database

    # 1. Retorno acumulado por tipo de ativo
    logger.info("Query: retorno acumulado por asset_type...")
    df_returns = query_redshift(client, wg, db, """
        SELECT trade_date, asset_type,
               AVG(daily_return) as avg_return
        FROM analytics.fct_daily_quotes
        WHERE daily_return IS NOT NULL
        GROUP BY trade_date, asset_type
        ORDER BY trade_date
    """)
    df_returns["trade_date"] = pd.to_datetime(df_returns["trade_date"])
    df_returns["avg_return"] = df_returns["avg_return"].astype(float)

    for at in df_returns["asset_type"].unique():
        mask = df_returns["asset_type"] == at
        df_returns.loc[mask, "cumulative"] = (1 + df_returns.loc[mask, "avg_return"]).cumprod() - 1

    fig1 = px.line(
        df_returns, x="trade_date", y="cumulative", color="asset_type",
        title="Retorno Acumulado: Ações vs FIIs — Dezembro 2024",
        labels={"cumulative": "Retorno (%)", "trade_date": "", "asset_type": "Tipo"},
    )
    fig1.update_layout(yaxis_tickformat=".1%", template="plotly_white")
    fig1.write_image(f"{args.output}/retorno_acumulado.png", width=900, height=450)
    logger.info("  → retorno_acumulado.png")

    # 2. Top 10 performers e bottom 10
    logger.info("Query: top/bottom performers...")
    df_perf = query_redshift(client, wg, db, """
        WITH ranked AS (
            SELECT ticker, asset_type, sector,
                   ROUND((LAST_VALUE(close_price) OVER (PARTITION BY ticker ORDER BY trade_date
                     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                   - FIRST_VALUE(close_price) OVER (PARTITION BY ticker ORDER BY trade_date
                     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
                   / NULLIF(FIRST_VALUE(close_price) OVER (PARTITION BY ticker ORDER BY trade_date
                     ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 0), 4) as monthly_return
            FROM analytics.fct_daily_quotes
        )
        SELECT DISTINCT ticker, asset_type, sector, monthly_return
        FROM ranked
        ORDER BY monthly_return DESC
    """)
    df_perf["monthly_return"] = df_perf["monthly_return"].astype(float)

    top10 = df_perf.head(10)
    bottom10 = df_perf.tail(10).sort_values("monthly_return")
    combined = pd.concat([top10, bottom10])
    combined["color"] = combined["monthly_return"].apply(lambda x: "green" if x >= 0 else "red")

    fig2 = go.Figure()
    fig2.add_trace(go.Bar(
        x=combined["monthly_return"], y=combined["ticker"],
        orientation="h",
        marker_color=combined["color"],
        text=combined["monthly_return"].apply(lambda x: f"{x:.1%}"),
        textposition="outside",
    ))
    fig2.update_layout(
        title="Top 10 e Bottom 10 — Retorno em Dezembro 2024",
        xaxis_title="Retorno (%)", yaxis_title="",
        xaxis_tickformat=".0%", template="plotly_white",
        height=500, width=900,
    )
    fig2.write_image(f"{args.output}/top_bottom_performers.png", width=900, height=500)
    logger.info("  → top_bottom_performers.png")

    # 3. Performance por setor
    logger.info("Query: performance por setor...")
    df_sector = query_redshift(client, wg, db, """
        SELECT asset_type, sector,
               ROUND(AVG(daily_return), 6) as avg_daily_return,
               COUNT(DISTINCT ticker) as num_assets,
               ROUND(SUM(volume) / 1e6, 1) as total_volume_mm
        FROM analytics.fct_daily_quotes
        WHERE sector != 'Unknown' AND daily_return IS NOT NULL
        GROUP BY asset_type, sector
        ORDER BY avg_daily_return DESC
    """)
    df_sector["avg_daily_return"] = df_sector["avg_daily_return"].astype(float)
    df_sector["num_assets"] = df_sector["num_assets"].astype(int)

    fig3 = px.bar(
        df_sector, x="sector", y="avg_daily_return", color="asset_type",
        title="Retorno Médio Diário por Setor — Dezembro 2024",
        labels={"avg_daily_return": "Retorno Médio Diário", "sector": "Setor", "asset_type": "Tipo"},
        barmode="group",
    )
    fig3.update_layout(yaxis_tickformat=".2%", template="plotly_white", xaxis_tickangle=-45)
    fig3.write_image(f"{args.output}/performance_setor.png", width=900, height=500)
    logger.info("  → performance_setor.png")

    # 4. Volatilidade: scatter plot
    logger.info("Query: volatilidade vs retorno...")
    df_vol = query_redshift(client, wg, db, """
        SELECT ticker, asset_type,
               ROUND(AVG(daily_return), 6) as avg_return,
               ROUND(STDDEV(daily_return), 6) as volatility
        FROM analytics.fct_daily_quotes
        WHERE daily_return IS NOT NULL
        GROUP BY ticker, asset_type
        HAVING COUNT(*) >= 10
    """)
    df_vol["avg_return"] = df_vol["avg_return"].astype(float)
    df_vol["volatility"] = df_vol["volatility"].astype(float)

    fig4 = px.scatter(
        df_vol, x="volatility", y="avg_return", color="asset_type",
        text="ticker", title="Risco vs Retorno — Dezembro 2024",
        labels={"volatility": "Volatilidade (desvio padrão)", "avg_return": "Retorno Médio Diário", "asset_type": "Tipo"},
    )
    fig4.update_traces(textposition="top center", textfont_size=8)
    fig4.update_layout(
        xaxis_tickformat=".2%", yaxis_tickformat=".2%",
        template="plotly_white", height=550, width=900,
    )
    fig4.write_image(f"{args.output}/risco_retorno.png", width=900, height=550)
    logger.info("  → risco_retorno.png")

    # 5. Resumo numérico (JSON pra usar no README)
    logger.info("Query: resumo geral...")
    df_summary = query_redshift(client, wg, db, """
        SELECT
            COUNT(*) as total_records,
            COUNT(DISTINCT ticker) as total_assets,
            COUNT(DISTINCT trade_date) as trading_days,
            SUM(CASE WHEN asset_type = 'STOCK' THEN 1 ELSE 0 END) as stock_records,
            SUM(CASE WHEN asset_type = 'FII' THEN 1 ELSE 0 END) as fii_records,
            MIN(trade_date) as first_date,
            MAX(trade_date) as last_date
        FROM analytics.fct_daily_quotes
    """)
    summary = df_summary.iloc[0].to_dict()
    with open(f"{args.output}/summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)
    logger.info(f"  → summary.json: {summary}")

    logger.info(f"Dashboard completo em {args.output}/")


if __name__ == "__main__":
    main()
