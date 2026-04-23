"""
Baixa cotações diárias dos ativos da B3 (ações + FIIs) via yfinance
e salva como Parquet no S3.

Uso:
    python ingest_quotes.py --date 2024-12-02 --bucket b3-pipeline-data-ACCOUNT_ID
"""

import argparse
import io
import logging
from datetime import datetime, timedelta

import boto3
import pandas as pd
import yfinance as yf

from src.tickers import ALL_TICKERS, ASSET_TYPE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def download_quotes(date_str: str) -> pd.DataFrame | None:
    """Baixa cotações de um dia específico. Retorna None se não houve pregão."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    start = dt.strftime("%Y-%m-%d")
    end = (dt + timedelta(days=1)).strftime("%Y-%m-%d")

    logger.info(f"Baixando cotações de {date_str} para {len(ALL_TICKERS)} ativos...")
    data = yf.download(ALL_TICKERS, start=start, end=end, group_by="ticker", auto_adjust=True, threads=True)

    if data.empty:
        logger.info(f"Sem dados para {date_str} (feriado ou fim de semana)")
        return None

    rows = []
    for ticker in ALL_TICKERS:
        try:
            td = data[ticker]
            if td.empty or pd.isna(td["Close"].iloc[0]):
                continue
            rows.append({
                "trade_date": dt.date(),
                "ticker": ticker.replace(".SA", ""),
                "asset_type": ASSET_TYPE[ticker],
                "open_price": float(td["Open"].iloc[0]),
                "high_price": float(td["High"].iloc[0]),
                "low_price": float(td["Low"].iloc[0]),
                "close_price": float(td["Close"].iloc[0]),
                "volume": int(td["Volume"].iloc[0]),
            })
        except (KeyError, IndexError):
            continue

    if not rows:
        logger.info(f"Nenhuma cotação válida para {date_str}")
        return None

    df = pd.DataFrame(rows)
    logger.info(f"{len(df)} cotações extraídas ({df['asset_type'].value_counts().to_dict()})")
    return df


def upload_to_s3(df: pd.DataFrame, bucket: str, date_str: str):
    key = f"raw/quotes/dt={date_str}/quotes.parquet"
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    logger.info(f"Upload: s3://{bucket}/{key} ({len(df)} registros)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--bucket", required=True)
    args = parser.parse_args()

    df = download_quotes(args.date)
    if df is not None:
        upload_to_s3(df, args.bucket, args.date)


if __name__ == "__main__":
    main()
