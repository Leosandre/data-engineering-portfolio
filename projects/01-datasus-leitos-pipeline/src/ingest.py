"""
Coleta dados de estabelecimentos hospitalares (DATASUS/CNES),
municipios (IBGE) e populacao (Censo 2022) e salva em Parquet.
"""

import json
import logging
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

DATASUS_BASE = "https://apidadosabertos.saude.gov.br/cnes/estabelecimentos"
IBGE_MUNICIPIOS = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
IBGE_POPULACAO = "https://apisidra.ibge.gov.br/values/t/4714/n6/all/v/93/p/2022"

# 5 = Hospital Geral, 7 = Hospital Especializado, 15 = Unidade Mista
TIPOS_HOSPITAL = [5, 7, 15]
PAGE_SIZE = 20  # limite fixo da API do DATASUS

session = requests.Session()
session.headers.update({"Accept": "application/json"})


def _get(url, params=None, tentativas=3):
    """GET com retry simples."""
    for i in range(1, tentativas + 1):
        try:
            r = session.get(url, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, json.JSONDecodeError) as err:
            if i == tentativas:
                raise
            log.warning("Tentativa %d falhou (%s): %s", i, url, err)
            time.sleep(2 * i)


def _salvar_parquet(registros, nome):
    caminho = RAW_DIR / f"{nome}.parquet"
    pq.write_table(pa.Table.from_pylist(registros), caminho, compression="snappy")
    log.info("%s: %d registros -> %s", nome, len(registros), caminho)


def coletar_estabelecimentos():
    log.info("Coletando estabelecimentos hospitalares (DATASUS)...")
    todos = []

    for tipo in TIPOS_HOSPITAL:
        log.info("  tipo_unidade=%d", tipo)
        offset = 0
        while True:
            pagina = _get(DATASUS_BASE, {"codigo_tipo_unidade": tipo, "limit": PAGE_SIZE, "offset": offset})
            registros = pagina.get("estabelecimentos", [])
            if not registros:
                break
            todos.extend(registros)
            offset += PAGE_SIZE

            if len(registros) < PAGE_SIZE:
                break
            if offset % 500 < PAGE_SIZE:
                log.info("    %d registros...", offset)

        log.info("  tipo_unidade=%d: %d registros no total", tipo, offset + len(registros) if registros else offset)

    log.info("Total estabelecimentos: %d", len(todos))
    _salvar_parquet(todos, "estabelecimentos")


def coletar_municipios():
    log.info("Coletando municipios (IBGE)...")
    bruto = _get(IBGE_MUNICIPIOS)

    registros = []
    for m in bruto:
        # Municipios novos nao tem microrregiao, pega UF via regiao-imediata
        micro = m.get("microrregiao")
        if micro and micro.get("mesorregiao"):
            uf = micro["mesorregiao"]["UF"]
        else:
            uf = m["regiao-imediata"]["regiao-intermediaria"]["UF"]

        registros.append({
            "id_municipio": m["id"],
            "nome_municipio": m["nome"],
            "id_uf": uf["id"],
            "sigla_uf": uf["sigla"],
            "nome_uf": uf["nome"],
            "nome_regiao": uf["regiao"]["nome"],
            "sigla_regiao": uf["regiao"]["sigla"],
        })

    _salvar_parquet(registros, "municipios")


def coletar_populacao():
    log.info("Coletando populacao por municipio - Censo 2022 (IBGE/SIDRA)...")
    bruto = _get(IBGE_POPULACAO)

    registros = []
    for row in bruto[1:]:  # primeira linha eh metadado
        pop = row.get("V", "")
        registros.append({
            "id_municipio": int(row["D1C"]),
            "nome_municipio_uf": row["D1N"],
            "populacao": int(pop) if pop and pop != "-" else None,
            "ano": int(row["D3C"]),
        })

    _salvar_parquet(registros, "populacao")


if __name__ == "__main__":
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    coletar_municipios()
    coletar_populacao()
    coletar_estabelecimentos()
    log.info("Pronto.")
