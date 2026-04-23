"""
Baixa dados cadastrais dos ativos da B3 (ações + FIIs) via yfinance
e salva como Parquet no S3.

Uso:
    python ingest_companies.py --bucket b3-pipeline-data-ACCOUNT_ID
"""

import argparse
import io
import logging

import boto3
import pandas as pd
import yfinance as yf

from src.tickers import ALL_TICKERS, ASSET_TYPE

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def download_company_info() -> pd.DataFrame:
    rows = []
    for ticker_str in ALL_TICKERS:
        try:
            info = yf.Ticker(ticker_str).info
            rows.append({
                "ticker": ticker_str.replace(".SA", ""),
                "asset_type": ASSET_TYPE[ticker_str],
                "company_name": info.get("longName", info.get("shortName", "")),
                "sector": info.get("sector", "Unknown"),
                "industry": info.get("industry", "Unknown"),
                "market_cap": info.get("marketCap"),
                "dividend_yield": info.get("dividendYield"),
                "currency": info.get("currency", "BRL"),
            })
            logger.info(f"  {ticker_str}: {rows[-1]['company_name']}")
        except Exception as e:
            logger.warning(f"  {ticker_str}: erro — {e}")
    df = pd.DataFrame(rows)
    logger.info(f"{len(df)} ativos extraídos ({df['asset_type'].value_counts().to_dict()})")
    return df


def upload_to_s3(df: pd.DataFrame, bucket: str, key: str):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    logger.info(f"Upload: s3://{bucket}/{key} ({len(df)} registros)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    args = parser.parse_args()

    logger.info("Baixando dados cadastrais dos ativos da B3...")
    companies = download_company_info()
    upload_to_s3(companies, args.bucket, "raw/companies/companies.parquet")


if __name__ == "__main__":
    main()
