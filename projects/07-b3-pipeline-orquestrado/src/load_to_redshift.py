"""
Carrega dados Parquet do S3 para tabelas raw no Redshift via COPY.
Schemas e tabelas já devem existir (criados no deploy).
"""

import argparse
import logging
import os
import time

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_client():
    return boto3.client("redshift-data", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))


def run_sql(client, workgroup, database, sql):
    resp = client.execute_statement(WorkgroupName=workgroup, Database=database, Sql=sql)
    stmt_id = resp["Id"]
    while True:
        status = client.describe_statement(Id=stmt_id)
        if status["Status"] in ("FINISHED", "FAILED", "ABORTED"):
            break
        time.sleep(2)
    if status["Status"] == "FAILED":
        raise RuntimeError(f"SQL falhou: {status.get('Error', 'unknown')}")
    logger.info(f"SQL OK: {sql.strip()[:80]}...")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--bucket", required=True)
    args = parser.parse_args()

    workgroup = os.environ.get("REDSHIFT_WORKGROUP", "b3-pipeline-wg")
    database = os.environ.get("REDSHIFT_DB", "dev")
    iam_role = os.environ["REDSHIFT_IAM_ROLE"]
    client = get_client()

    # COPY cotações (delete+insert pra idempotência)
    s3_path = f"s3://{args.bucket}/raw/quotes/dt={args.date}/"
    run_sql(client, workgroup, database,
            f"""DELETE FROM "raw".quotes WHERE trade_date = '{args.date}'""")
    run_sql(client, workgroup, database, f"""
        COPY "raw".quotes FROM '{s3_path}' IAM_ROLE '{iam_role}' FORMAT AS PARQUET
    """)
    logger.info(f"Cotações de {args.date} carregadas")

    # COPY empresas (truncate+load)
    companies_path = f"s3://{args.bucket}/raw/companies/"
    run_sql(client, workgroup, database, 'TRUNCATE TABLE "raw".companies')
    run_sql(client, workgroup, database, f"""
        COPY "raw".companies (ticker, asset_type, company_name, sector, industry, market_cap, dividend_yield, currency)
        FROM '{companies_path}' IAM_ROLE '{iam_role}' FORMAT AS PARQUET
    """)
    logger.info("Empresas carregadas")


if __name__ == "__main__":
    main()
