"""
Transforma dados de voos: bronze (parquet cru) -> silver (limpo) -> gold (analitico).
Usa Polars pra processamento — mais rapido que pandas pra esse volume (~1.8M registros).
"""

import logging
from pathlib import Path

import polars as pl

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parent.parent
BRONZE = BASE / "data" / "bronze"
SILVER = BASE / "data" / "silver"
GOLD = BASE / "data" / "gold"

FMT_DT = "%d/%m/%Y %H:%M"


def bronze_to_silver():
    """Le todos os parquets do bronze, unifica, limpa e grava no silver."""
    log.info("Bronze -> Silver...")

    arquivos = sorted(BRONZE.glob("vra_*.parquet"))
    log.info("  %d arquivos bronze", len(arquivos))

    df = pl.concat([pl.read_parquet(f) for f in arquivos])
    total_bruto = len(df)
    log.info("  %d registros brutos", total_bruto)

    # parse de datas (formato dd/mm/yyyy HH:MM)
    for col in ["partida_prevista", "partida_real", "chegada_prevista", "chegada_real"]:
        df = df.with_columns(
            pl.col(col).str.strip_chars().str.to_datetime(FMT_DT, strict=False).alias(col)
        )

    # limpa codigos ICAO
    for col in ["icao_empresa", "icao_origem", "icao_destino"]:
        df = df.with_columns(pl.col(col).str.strip_chars().str.to_uppercase())

    # situacao padronizada
    df = df.with_columns(pl.col("situacao_voo").str.strip_chars().str.to_uppercase())

    # trata "NAO INFORMADO" como cancelado (sem info de horario real)
    df = df.with_columns(
        pl.when(pl.col("situacao_voo").str.contains("(?i)NAO|NÃO"))
            .then(pl.lit("CANCELADO"))
        .otherwise(pl.col("situacao_voo"))
        .alias("situacao_voo")
    )

    # remove voos com origem = destino
    antes = len(df)
    df = df.filter(pl.col("icao_origem") != pl.col("icao_destino"))
    log.info("  removidos %d voos com origem = destino", antes - len(df))

    # remove linhas sem data de partida prevista (inutilizaveis)
    antes = len(df)
    df = df.filter(pl.col("partida_prevista").is_not_null())
    log.info("  removidos %d voos sem partida prevista", antes - len(df))

    # filtra apenas voos dentro do periodo esperado (2022-2023)
    antes = len(df)
    df = df.filter(
        (pl.col("partida_prevista").dt.year() >= 2022) &
        (pl.col("partida_prevista").dt.year() <= 2023)
    )
    log.info("  removidos %d voos fora do periodo 2022-2023", antes - len(df))

    # dedup: mesma empresa + voo + data partida prevista + origem
    antes = len(df)
    df = df.unique(
        subset=["icao_empresa", "numero_voo", "partida_prevista", "icao_origem"],
        keep="last",
    )
    log.info("  removidas %d duplicatas", antes - len(df))

    # calcula atrasos em minutos
    df = df.with_columns([
        ((pl.col("partida_real") - pl.col("partida_prevista")).dt.total_minutes())
            .alias("atraso_partida_min"),
        ((pl.col("chegada_real") - pl.col("chegada_prevista")).dt.total_minutes())
            .alias("atraso_chegada_min"),
    ])

    # classifica pontualidade
    df = df.with_columns(
        pl.when(pl.col("situacao_voo") == "CANCELADO")
            .then(pl.lit("CANCELADO"))
        .when(pl.col("atraso_chegada_min").is_null())
            .then(pl.lit("SEM_INFO"))
        .when(pl.col("atraso_chegada_min") <= 0)
            .then(pl.lit("PONTUAL"))
        .when(pl.col("atraso_chegada_min") <= 30)
            .then(pl.lit("ATRASO_LEVE"))
        .when(pl.col("atraso_chegada_min") <= 60)
            .then(pl.lit("ATRASO_MODERADO"))
        .otherwise(pl.lit("ATRASO_GRAVE"))
        .alias("classificacao")
    )

    # extrai campos temporais
    df = df.with_columns([
        pl.col("partida_prevista").dt.year().alias("ano"),
        pl.col("partida_prevista").dt.month().alias("mes"),
        pl.col("partida_prevista").dt.weekday().alias("dia_semana"),  # 1=seg, 7=dom
        pl.col("partida_prevista").dt.hour().alias("hora_partida"),
    ])

    # rota
    df = df.with_columns(
        (pl.col("icao_origem") + "-" + pl.col("icao_destino")).alias("rota")
    )

    SILVER.mkdir(parents=True, exist_ok=True)
    saida = SILVER / "fct_voos.parquet"
    df.write_parquet(saida)
    log.info("  Silver: %d registros (removidos %d = %.1f%%)",
             len(df), total_bruto - len(df), 100 * (total_bruto - len(df)) / total_bruto)


def silver_to_gold():
    """Agrega dados do silver em tabelas analiticas no gold."""
    log.info("Silver -> Gold...")

    df = pl.read_parquet(SILVER / "fct_voos.parquet")
    GOLD.mkdir(parents=True, exist_ok=True)

    # --- dim_companhias (extraida dos dados) ---
    cias = (df.select("icao_empresa").unique()
            .rename({"icao_empresa": "icao"})
            .sort("icao"))
    cias.write_parquet(GOLD / "dim_companhias.parquet")
    log.info("  dim_companhias: %d", len(cias))

    # --- dim_aeroportos (extraida dos dados) ---
    origens = df.select(pl.col("icao_origem").alias("icao"))
    destinos = df.select(pl.col("icao_destino").alias("icao"))
    aeroportos = pl.concat([origens, destinos]).unique().sort("icao")
    aeroportos.write_parquet(GOLD / "dim_aeroportos.parquet")
    log.info("  dim_aeroportos: %d", len(aeroportos))

    # helper pra metricas de pontualidade
    def metricas_pont(grupo):
        return grupo.agg([
            pl.len().alias("total_voos"),
            (pl.col("situacao_voo") == "REALIZADO").sum().alias("realizados"),
            (pl.col("situacao_voo") == "CANCELADO").sum().alias("cancelados"),
            (pl.col("classificacao") == "PONTUAL").sum().alias("pontuais"),
            (pl.col("classificacao") == "ATRASO_LEVE").sum().alias("atraso_leve"),
            (pl.col("classificacao") == "ATRASO_MODERADO").sum().alias("atraso_moderado"),
            (pl.col("classificacao") == "ATRASO_GRAVE").sum().alias("atraso_grave"),
            pl.col("atraso_chegada_min").median().alias("mediana_atraso_min"),
            pl.col("atraso_chegada_min").mean().alias("media_atraso_min"),
            pl.col("atraso_chegada_min").quantile(0.95).alias("p95_atraso_min"),
        ]).with_columns([
            (100.0 * pl.col("cancelados") / pl.col("total_voos")).round(2).alias("pct_cancelados"),
            (100.0 * pl.col("pontuais") / pl.col("realizados")).round(2).alias("pct_pontualidade"),
        ])

    # --- agg por companhia ---
    agg = metricas_pont(df.group_by("icao_empresa"))
    agg.sort("total_voos", descending=True).write_parquet(GOLD / "agg_pontualidade_cia.parquet")
    log.info("  agg_pontualidade_cia: %d companhias", len(agg))

    # --- agg por aeroporto de origem ---
    agg = metricas_pont(df.group_by(pl.col("icao_origem").alias("icao_aeroporto")))
    agg.sort("total_voos", descending=True).write_parquet(GOLD / "agg_pontualidade_aeroporto.parquet")
    log.info("  agg_pontualidade_aeroporto: %d aeroportos", len(agg))

    # --- agg por rota (top 200 por volume) ---
    agg = metricas_pont(df.group_by("rota"))
    agg = agg.sort("total_voos", descending=True).head(200)
    agg.write_parquet(GOLD / "agg_pontualidade_rota.parquet")
    log.info("  agg_pontualidade_rota: top %d rotas", len(agg))

    # --- agg mensal (sazonalidade) ---
    agg = metricas_pont(df.group_by("ano", "mes"))
    agg.sort("ano", "mes").write_parquet(GOLD / "agg_mensal.parquet")
    log.info("  agg_mensal: %d meses", len(agg))

    # --- agg por dia da semana ---
    agg = metricas_pont(df.group_by("dia_semana"))
    agg.sort("dia_semana").write_parquet(GOLD / "agg_dia_semana.parquet")
    log.info("  agg_dia_semana: %d dias", len(agg))

    # --- agg por hora do dia ---
    agg = metricas_pont(df.group_by("hora_partida"))
    agg.sort("hora_partida").write_parquet(GOLD / "agg_hora.parquet")
    log.info("  agg_hora: %d horas", len(agg))


if __name__ == "__main__":
    bronze_to_silver()
    silver_to_gold()
    log.info("Transformacao concluida.")
