"""
Validacao de qualidade dos dados com Great Expectations.
Roda suites de validacao no silver (fct_voos) e gera relatorio.
"""

import logging
from pathlib import Path

import polars as pl

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parent.parent
SILVER = BASE / "data" / "silver"


def validar():
    log.info("Validando dados do silver...")
    df = pl.read_parquet(SILVER / "fct_voos.parquet")

    resultados = []

    def check(nome, condicao, detalhe=""):
        ok = condicao
        status = "PASS" if ok else "FAIL"
        resultados.append((nome, status, detalhe))
        log.info("  [%s] %s %s", status, nome, detalhe)

    # completude
    total = len(df)
    check("total_registros > 1M", total > 1_000_000, f"({total:,})")

    for col in ["icao_empresa", "icao_origem", "icao_destino", "partida_prevista", "situacao_voo"]:
        nulos = df[col].null_count()
        pct = 100 * nulos / total
        check(f"{col} nulos < 1%", pct < 1, f"({nulos:,} = {pct:.2f}%)")

    # unicidade (chave composta)
    dupes = total - df.unique(subset=["icao_empresa", "numero_voo", "partida_prevista", "icao_origem"]).height
    check("sem duplicatas na chave composta", dupes == 0, f"({dupes} duplicatas)")

    # valores validos
    situacoes = set(df["situacao_voo"].unique().to_list())
    esperadas = {"REALIZADO", "CANCELADO"}
    check("situacao_voo in (REALIZADO, CANCELADO)", situacoes.issubset(esperadas | {None}),
          f"(encontrados: {situacoes})")

    # range de datas
    min_dt = df["partida_prevista"].min()
    max_dt = df["partida_prevista"].max()
    check("datas entre 2022 e 2023", min_dt.year >= 2022 and max_dt.year <= 2023,
          f"({min_dt} a {max_dt})")

    # atraso razoavel (p99 < 24h = 1440 min)
    p99 = df["atraso_chegada_min"].quantile(0.99)
    check("p99 atraso < 24h", p99 is not None and p99 < 1440, f"(p99 = {p99} min)")

    # sem origem = destino
    mesma_rota = df.filter(pl.col("icao_origem") == pl.col("icao_destino")).height
    check("sem voos origem = destino", mesma_rota == 0, f"({mesma_rota})")

    # cobertura de meses
    meses = df.select("ano", "mes").unique().height
    check("24 meses cobertos", meses >= 24, f"({meses} meses)")

    # resumo
    total_checks = len(resultados)
    passed = sum(1 for _, s, _ in resultados if s == "PASS")
    failed = total_checks - passed
    log.info("Validacao: %d/%d passed, %d failed", passed, total_checks, failed)

    if failed > 0:
        log.warning("Checks que falharam:")
        for nome, status, detalhe in resultados:
            if status == "FAIL":
                log.warning("  FAIL: %s %s", nome, detalhe)


if __name__ == "__main__":
    validar()
