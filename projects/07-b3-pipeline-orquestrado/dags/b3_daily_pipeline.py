"""
DAG: b3_daily_pipeline
Pipeline diário de cotações da B3 — ingestão, transformação dbt, testes e alerta.
Schedule: dias úteis às 19h UTC (16h BRT, após fechamento da B3).
"""

import logging
import sys
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

logger = logging.getLogger(__name__)

B3_HOLIDAYS_2024 = {
    "2024-01-01", "2024-02-12", "2024-02-13", "2024-03-29",
    "2024-04-21", "2024-05-01", "2024-05-30", "2024-07-09",
    "2024-09-07", "2024-10-12", "2024-11-02", "2024-11-15",
    "2024-11-20", "2024-12-24", "2024-12-25", "2024-12-31",
}

default_args = {
    "owner": "leosandre",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


def _is_trading_day(**context):
    ds = context["ds"]
    dt = datetime.strptime(ds, "%Y-%m-%d")
    if dt.weekday() >= 5:
        logger.info(f"{ds} é fim de semana — skip")
        return False
    if ds in B3_HOLIDAYS_2024:
        logger.info(f"{ds} é feriado B3 — skip")
        return False
    return True


def _ingest_quotes(**context):
    sys.path.insert(0, "/home/airflow/airflow/dags")
    from src.ingest_quotes import download_quotes, upload_to_s3
    ds = context["ds"]
    bucket = Variable.get("data_bucket")
    df = download_quotes(ds)
    if df is not None:
        upload_to_s3(df, bucket, ds)


def _ingest_companies(**context):
    ds = context["ds"]
    dt = datetime.strptime(ds, "%Y-%m-%d")
    if dt.weekday() != 0 and dt.day != 1:
        logger.info("Não é segunda nem dia 1 — skip ingestão de empresas")
        return
    sys.path.insert(0, "/home/airflow/airflow/dags")
    from src.ingest_companies import download_company_info, upload_to_s3
    bucket = Variable.get("data_bucket")
    df = download_company_info()
    upload_to_s3(df, bucket, "raw/companies/companies.parquet")


def _notify_success(**context):
    ds = context["ds"]
    topic = Variable.get("sns_topic_arn")
    boto3.client("sns").publish(
        TopicArn=topic,
        Subject=f"B3 Pipeline OK — {ds}",
        Message=f"Pipeline executado com sucesso para {ds}.",
    )


def _notify_failure(context):
    try:
        topic = Variable.get("sns_topic_arn")
        ds = context.get("ds", "unknown")
        boto3.client("sns").publish(
            TopicArn=topic,
            Subject=f"B3 Pipeline FALHOU — {ds}",
            Message=f"Falha no pipeline para {ds}. Verificar logs.",
        )
    except Exception:
        logger.exception("Falha ao enviar alerta SNS")


with DAG(
    dag_id="b3_daily_pipeline",
    description="Pipeline diario de cotacoes B3 com dbt incremental",
    schedule="0 19 * * 1-5",
    start_date=datetime(2024, 12, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    on_failure_callback=_notify_failure,
    tags=["b3", "dbt", "incremental"],
) as dag:

    check_trading_day = ShortCircuitOperator(
        task_id="check_trading_day",
        python_callable=_is_trading_day,
    )

    ingest_quotes = PythonOperator(
        task_id="ingest_quotes",
        python_callable=_ingest_quotes,
    )

    ingest_companies = PythonOperator(
        task_id="ingest_companies",
        python_callable=_ingest_companies,
    )

    load_to_redshift = BashOperator(
        task_id="load_to_redshift",
        bash_command=(
            "source /home/airflow/venv/bin/activate && "
            "export REDSHIFT_WORKGROUP={{ var.value.redshift_workgroup }} && "
            "export REDSHIFT_IAM_ROLE={{ var.value.redshift_iam_role }} && "
            "python /home/airflow/airflow/dags/src/load_to_redshift.py "
            "--date {{ ds }} "
            "--bucket {{ var.value.data_bucket }} "
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "source /home/airflow/venv/bin/activate && "
            "cd /home/airflow/airflow/dags/dbt_project && "
            "dbt deps --no-version-check && "
            "dbt run --select +fct_daily_quotes+ agg_sector_performance"
        ),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            "source /home/airflow/venv/bin/activate && "
            "cd /home/airflow/airflow/dags/dbt_project && "
            "dbt snapshot"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "source /home/airflow/venv/bin/activate && "
            "cd /home/airflow/airflow/dags/dbt_project && "
            "dbt test"
        ),
    )

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=_notify_success,
        trigger_rule="all_success",
    )

    (
        check_trading_day
        >> [ingest_quotes, ingest_companies]
        >> load_to_redshift
        >> dbt_run
        >> dbt_snapshot
        >> dbt_test
        >> notify_success
    )
