"""
Configura QuickSight: data source + datasets via tabelas do Redshift.
"""

import logging
import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ACCOUNT_ID = "306158388479"
REGION = "us-east-1"
QS_USER = f"arn:aws:quicksight:{REGION}:{ACCOUNT_ID}:user/default/agenda-facil"

session = boto3.Session(profile_name="agenda-facil", region_name=REGION)
qs = session.client("quicksight")

DS_ARN = f"arn:aws:quicksight:{REGION}:{ACCOUNT_ID}:datasource/redshift-portfolio"


def create_dataset(ds_id, name, table_name):
    log.info(f"  {name}...")
    try:
        qs.create_data_set(
            AwsAccountId=ACCOUNT_ID,
            DataSetId=ds_id,
            Name=name,
            PhysicalTableMap={
                "table": {
                    "RelationalTable": {
                        "DataSourceArn": DS_ARN,
                        "Schema": "public",
                        "Name": table_name,
                        "InputColumns": get_columns(table_name),
                    }
                }
            },
            ImportMode="DIRECT_QUERY",
            Permissions=[{
                "Principal": QS_USER,
                "Actions": [
                    "quicksight:DescribeDataSet", "quicksight:DescribeDataSetPermissions",
                    "quicksight:PassDataSet", "quicksight:DescribeIngestion",
                    "quicksight:ListIngestions", "quicksight:UpdateDataSet",
                    "quicksight:DeleteDataSet", "quicksight:CreateIngestion",
                    "quicksight:CancelIngestion", "quicksight:UpdateDataSetPermissions",
                ],
            }],
        )
        log.info(f"    OK!")
    except qs.exceptions.ResourceExistsException:
        log.info(f"    ja existe")
    except Exception as e:
        log.error(f"    ERRO: {e}")


def get_columns(table_name):
    """Retorna colunas da tabela do Redshift pra QuickSight."""
    import redshift_connector
    conn = redshift_connector.connect(
        host=f"data-platform-portfolio-dev-wg.{ACCOUNT_ID}.{REGION}.redshift-serverless.amazonaws.com",
        database="analytics", user="admin", password="Portfolio2026!", port=5439
    )
    cur = conn.cursor()
    cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}' AND table_schema = 'public' ORDER BY ordinal_position")
    rows = cur.fetchall()
    conn.close()

    type_map = {
        "bigint": "INTEGER", "integer": "INTEGER", "smallint": "INTEGER",
        "double precision": "DECIMAL", "real": "DECIMAL", "numeric": "DECIMAL",
        "boolean": "BOOLEAN",
        "timestamp without time zone": "DATETIME", "timestamp with time zone": "DATETIME",
    }

    cols = []
    for name, dtype in rows:
        qs_type = type_map.get(dtype, "STRING")
        cols.append({"Name": name, "Type": qs_type})
    return cols


if __name__ == "__main__":
    log.info("Criando datasets no QuickSight...")

    tables = {
        "ds-golden-customers": ("Golden Customers", "golden_customers"),
        "ds-reconciled-sales": ("Vendas Reconciliadas", "reconciled_sales"),
        "ds-churn-scores": ("Churn Scores", "churn_scores"),
        "ds-forecast-sales": ("Forecast Vendas", "forecast_sales"),
        "ds-customer-matches": ("Customer Matches", "customer_matches"),
        "ds-field-lineage": ("Field Lineage", "field_lineage"),
    }

    for ds_id, (name, table) in tables.items():
        create_dataset(ds_id, name, table)

    log.info("Datasets criados! Acesse QuickSight pra montar os dashboards.")
    log.info(f"URL: https://{REGION}.quicksight.aws.amazon.com/sn/start")
