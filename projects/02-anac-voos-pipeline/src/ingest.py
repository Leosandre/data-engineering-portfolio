"""
Baixa CSVs mensais de voos (VRA) do site da ANAC e converte pra Parquet.
Tambem baixa dados de aeroportos.

Os nomes dos arquivos no site sao inconsistentes entre anos
(VRA_01_2022.csv, vra_2022_09.csv, VRA_AbriL_2019.csv), entao
faz scraping da pagina pra descobrir as URLs reais.
"""

import asyncio
import logging
import re
from pathlib import Path

import httpx
import polars as pl
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parent.parent
BRONZE = BASE / "data" / "bronze"
ANAC_BASE = "https://www.gov.br/anac/pt-br/assuntos/dados-e-estatisticas/percentuais-de-atrasos-e-cancelamentos-2"
IBGE_MUNICIPIOS = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
ANOS = [2022, 2023]
HEADERS = {"User-Agent": "Mozilla/5.0 (pipeline-anac-portfolio)"}

# colunas padronizadas (o schema muda entre anos)
COLUNAS_PADRAO = [
    "icao_empresa", "numero_voo", "codigo_di", "codigo_tipo_linha",
    "icao_origem", "icao_destino",
    "partida_prevista", "partida_real",
    "chegada_prevista", "chegada_real",
    "situacao_voo",
]


async def descobrir_urls_csv(client, ano):
    """Faz scraping da pagina do ano pra pegar as URLs reais dos CSVs."""
    url = f"{ANAC_BASE}/{ano}"
    for tentativa in range(3):
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            break
        except (httpx.HTTPError, httpx.ReadError) as e:
            if tentativa == 2:
                log.error("Falha ao acessar pagina %s: %s", url, e)
                return []
            await asyncio.sleep(3 * (tentativa + 1))

    soup = BeautifulSoup(resp.text, "lxml")
    urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r"vra.*\.csv/view$", href, re.IGNORECASE):
            download_url = href.replace("/view", "/@@download/file")
            urls.append(download_url)

    log.info("  %d: %d CSVs encontrados", ano, len(urls))
    return urls


def extrair_ano_mes(url):
    """Extrai ano e mes da URL, lidando com os padroes inconsistentes."""
    nome = url.split("/")[-3].lower()  # ex: vra_01_2022.csv (antes de @@download/file)
    nome = re.sub(r"\.csv$", "", nome)  # remove extensao
    # pega todos os numeros de 2+ digitos
    nums = re.findall(r"\d{2,4}", nome)
    ano = mes = None
    for n in nums:
        v = int(n)
        if 2000 <= v <= 2099:
            ano = v
        elif 1 <= v <= 12 and mes is None:
            mes = v
    return ano, mes


def ler_csv_anac(conteudo_bytes):
    """Le CSV da ANAC lidando com encoding e schema variavel."""
    # tenta utf-8-sig (BOM), fallback pra latin-1
    for enc in ["utf-8-sig", "latin-1"]:
        try:
            texto = conteudo_bytes.decode(enc)
            break
        except UnicodeDecodeError:
            continue

    df = pl.read_csv(
        texto.encode("utf-8"),
        separator=";",
        infer_schema_length=0,  # tudo como string primeiro
        truncate_ragged_lines=True,
    )

    # normaliza nomes de coluna (remove acentos, lowercase, etc)
    mapa = {}
    for col in df.columns:
        c = col.strip().lower()
        c = c.replace("\ufeff", "")  # BOM
        if "empresa" in c:
            mapa[col] = "icao_empresa"
        elif "mero" in c and "voo" in c:
            mapa[col] = "numero_voo"
        elif "digo di" in c or "codigo di" in c:
            mapa[col] = "codigo_di"
        elif "tipo linha" in c:
            mapa[col] = "codigo_tipo_linha"
        elif "origem" in c:
            mapa[col] = "icao_origem"
        elif "destino" in c:
            mapa[col] = "icao_destino"
        elif "partida" in c and "prev" in c:
            mapa[col] = "partida_prevista"
        elif "partida" in c and "real" in c:
            mapa[col] = "partida_real"
        elif "chegada" in c and "prev" in c:
            mapa[col] = "chegada_prevista"
        elif "chegada" in c and "real" in c:
            mapa[col] = "chegada_real"
        elif "situa" in c:
            mapa[col] = "situacao_voo"

    df = df.rename(mapa)

    # garante que so tem as colunas padrao, na ordem certa
    for col in COLUNAS_PADRAO:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    return df.select(COLUNAS_PADRAO)


async def baixar_csv(client, url, semaforo):
    """Baixa um CSV com controle de concorrencia."""
    async with semaforo:
        for tentativa in range(3):
            try:
                resp = await client.get(url, follow_redirects=True)
                resp.raise_for_status()
                return resp.content
            except (httpx.HTTPError, httpx.ReadError) as e:
                if tentativa == 2:
                    log.error("Falha ao baixar %s: %s", url, e)
                    return None
                await asyncio.sleep(2 * (tentativa + 1))


async def ingerir_voos():
    log.info("Ingerindo voos da ANAC (anos: %s)...", ANOS)
    BRONZE.mkdir(parents=True, exist_ok=True)

    semaforo = asyncio.Semaphore(4)  # max 4 downloads simultaneos

    async with httpx.AsyncClient(headers=HEADERS, timeout=120) as client:
        # descobre URLs de todos os anos
        todas_urls = []
        for ano in ANOS:
            urls = await descobrir_urls_csv(client, ano)
            todas_urls.extend(urls)

        log.info("Total: %d CSVs pra baixar", len(todas_urls))

        # baixa tudo em paralelo
        tarefas = [baixar_csv(client, url, semaforo) for url in todas_urls]
        resultados = await asyncio.gather(*tarefas)

    total_linhas = 0
    for url, conteudo in zip(todas_urls, resultados):
        if conteudo is None:
            continue

        ano, mes = extrair_ano_mes(url)
        if ano is None:
            log.warning("Nao consegui extrair ano/mes de %s, pulando", url)
            continue

        df = ler_csv_anac(conteudo)
        df = df.with_columns([
            pl.lit(ano).alias("ano"),
            pl.lit(mes).alias("mes"),
        ])

        saida = BRONZE / f"vra_{ano}_{mes:02d}.parquet"
        df.write_parquet(saida)
        total_linhas += len(df)
        log.info("  %s: %d registros", saida.name, len(df))

    log.info("Total voos ingeridos: %d", total_linhas)


def ingerir_aeroportos():
    """Baixa lista de aeroportos publicos da ANAC."""
    log.info("Baixando dados de aeroportos...")

    # usa a lista de aeroportos que aparece nos proprios dados de voo
    # (nao tem um CSV separado facil de baixar da ANAC)
    # vamos criar a dim_aeroportos a partir dos dados de voo no transform
    log.info("  Aeroportos serao extraidos dos dados de voo no transform")


def ingerir_municipios():
    log.info("Baixando municipios (IBGE)...")
    import json
    resp = httpx.get(IBGE_MUNICIPIOS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    registros = []
    for m in data:
        micro = m.get("microrregiao")
        if micro and micro.get("mesorregiao"):
            uf = micro["mesorregiao"]["UF"]
        else:
            uf = m["regiao-imediata"]["regiao-intermediaria"]["UF"]
        registros.append({
            "id_municipio": m["id"],
            "nome_municipio": m["nome"],
            "sigla_uf": uf["sigla"],
            "nome_uf": uf["nome"],
            "nome_regiao": uf["regiao"]["nome"],
        })

    df = pl.DataFrame(registros)
    saida = BRONZE / "municipios.parquet"
    df.write_parquet(saida)
    log.info("  %d municipios -> %s", len(df), saida.name)


if __name__ == "__main__":
    BRONZE.mkdir(parents=True, exist_ok=True)
    ingerir_municipios()
    asyncio.run(ingerir_voos())
    log.info("Ingestao concluida.")
