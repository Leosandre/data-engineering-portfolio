"""
Extrai todas as evidencias da POC antes do teardown.

Baixa do S3, DynamoDB, CloudWatch e SQS pra pasta local data/evidence/
pra que o dashboard e a documentacao possam ser gerados offline.

Uso:
    python scripts/extract_evidence.py --profile agenda-facil --region us-east-1
"""

import argparse
import gzip
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PROJECT = "fraud-detection-poc"
EVIDENCE_DIR = Path(__file__).parent.parent / "data" / "evidence"


def extract_s3(session, region):
    """Baixa todos os dados do S3 data lake."""
    s3 = session.client("s3", region_name=region)
    account = session.client("sts").get_caller_identity()["Account"]
    bucket = f"{PROJECT}-lake-{account}"

    s3_dir = EVIDENCE_DIR / "s3"
    s3_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Baixando dados do S3 ({bucket})...")
    paginator = s3.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            local_path = s3_dir / key
            local_path.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket, key, str(local_path))
            count += 1

    log.info(f"  {count} arquivos baixados do S3")
    return count


def extract_dynamodb(session, region):
    """Exporta perfis do DynamoDB."""
    dynamo = session.resource("dynamodb", region_name=region)
    table = dynamo.Table(f"{PROJECT}-customer-profiles")

    ddb_dir = EVIDENCE_DIR / "dynamodb"
    ddb_dir.mkdir(parents=True, exist_ok=True)
    out_path = ddb_dir / "customer_profiles.jsonl"

    log.info("Exportando perfis do DynamoDB...")
    count = 0
    with open(out_path, "w") as f:
        scan_kwargs = {}
        while True:
            resp = table.scan(**scan_kwargs)
            for item in resp["Items"]:
                # converter Decimal pra float pro JSON
                clean = {k: float(v) if hasattr(v, "as_tuple") else v for k, v in item.items()}
                f.write(json.dumps(clean, ensure_ascii=False) + "\n")
                count += 1
            if "LastEvaluatedKey" not in resp:
                break
            scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    log.info(f"  {count:,} perfis exportados")
    return count


def extract_sqs_alerts(session, region):
    """Drena alertas da fila SQS."""
    sqs = session.client("sqs", region_name=region)

    # pegar URL da fila
    try:
        resp = sqs.get_queue_url(QueueName=f"{PROJECT}-fraud-alerts-queue")
        queue_url = resp["QueueUrl"]
    except Exception as e:
        log.warning(f"  Fila SQS nao encontrada: {e}")
        return 0

    sqs_dir = EVIDENCE_DIR / "sqs"
    sqs_dir.mkdir(parents=True, exist_ok=True)
    out_path = sqs_dir / "fraud_alerts.jsonl"

    log.info("Drenando alertas do SQS...")
    count = 0
    with open(out_path, "w") as f:
        while True:
            resp = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=2,
            )
            messages = resp.get("Messages", [])
            if not messages:
                break
            for msg in messages:
                f.write(msg["Body"] + "\n")
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
                count += 1

    log.info(f"  {count:,} alertas extraidos do SQS")
    return count


def extract_cloudwatch_metrics(session, region):
    """Baixa metricas custom do CloudWatch."""
    cw = session.client("cloudwatch", region_name=region)

    cw_dir = EVIDENCE_DIR / "cloudwatch"
    cw_dir.mkdir(parents=True, exist_ok=True)

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=3)  # janela de 3h pra pegar tudo

    metrics_to_fetch = [
        ("fraud/detection_latency_ms", "Average"),
        ("fraud/detection_latency_ms", "p99"),
        ("fraud/transactions_per_second", "Average"),
        ("fraud/alerts_count", "Sum"),
    ]

    log.info("Baixando metricas do CloudWatch...")
    all_metrics = {}
    for metric_name, stat in metrics_to_fetch:
        try:
            resp = cw.get_metric_statistics(
                Namespace=PROJECT,
                MetricName=metric_name.split("/")[-1],
                StartTime=start_time,
                EndTime=end_time,
                Period=60,
                Statistics=[stat] if stat != "p99" else [],
                ExtendedStatistics=["p99"] if stat == "p99" else [],
            )
            datapoints = sorted(resp["Datapoints"], key=lambda x: x["Timestamp"])
            all_metrics[f"{metric_name}_{stat}"] = [
                {"timestamp": dp["Timestamp"].isoformat(),
                 "value": dp.get(stat, dp.get("ExtendedStatistics", {}).get("p99", 0))}
                for dp in datapoints
            ]
        except Exception as e:
            log.warning(f"  Metrica {metric_name} nao encontrada: {e}")

    out_path = cw_dir / "metrics.json"
    with open(out_path, "w") as f:
        json.dump(all_metrics, f, indent=2, default=str)

    log.info(f"  {len(all_metrics)} series de metricas salvas")
    return len(all_metrics)


def extract_kinesis_metrics(session, region):
    """Baixa metricas do Kinesis (IncomingRecords, etc) do CloudWatch."""
    cw = session.client("cloudwatch", region_name=region)

    cw_dir = EVIDENCE_DIR / "cloudwatch"
    cw_dir.mkdir(parents=True, exist_ok=True)

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=3)

    streams = [f"{PROJECT}-transactions", f"{PROJECT}-alerts"]
    kinesis_metrics = {}

    log.info("Baixando metricas do Kinesis...")
    for stream in streams:
        for metric in ["IncomingRecords", "IncomingBytes", "GetRecords.Latency"]:
            try:
                resp = cw.get_metric_statistics(
                    Namespace="AWS/Kinesis",
                    MetricName=metric,
                    Dimensions=[{"Name": "StreamName", "Value": stream}],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=60,
                    Statistics=["Sum", "Average"],
                )
                datapoints = sorted(resp["Datapoints"], key=lambda x: x["Timestamp"])
                kinesis_metrics[f"{stream}/{metric}"] = [
                    {"timestamp": dp["Timestamp"].isoformat(),
                     "sum": dp.get("Sum", 0), "avg": dp.get("Average", 0)}
                    for dp in datapoints
                ]
            except Exception:
                pass

    out_path = cw_dir / "kinesis_metrics.json"
    with open(out_path, "w") as f:
        json.dump(kinesis_metrics, f, indent=2, default=str)

    log.info(f"  {len(kinesis_metrics)} series de metricas Kinesis salvas")
    return len(kinesis_metrics)


def generate_summary(session, region):
    """Gera resumo da POC com contagens e timestamps."""
    summary = {
        "project": PROJECT,
        "region": region,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "evidence_dir": str(EVIDENCE_DIR),
        "files": {},
    }

    for root, dirs, files in os.walk(EVIDENCE_DIR):
        for fname in files:
            fpath = Path(root) / fname
            rel = fpath.relative_to(EVIDENCE_DIR)
            summary["files"][str(rel)] = {
                "size_bytes": fpath.stat().st_size,
                "lines": sum(1 for _ in open(fpath)) if fpath.suffix in (".jsonl", ".json") else None,
            }

    out_path = EVIDENCE_DIR / "summary.json"
    with open(out_path, "w") as f:
        json.dump(summary, f, indent=2)

    log.info(f"\nResumo salvo em {out_path}")
    log.info(f"Total de arquivos: {len(summary['files'])}")
    total_size = sum(v["size_bytes"] for v in summary["files"].values())
    log.info(f"Tamanho total: {total_size / 1024 / 1024:.1f} MB")


def main():
    parser = argparse.ArgumentParser(description="Extrai evidencias da POC")
    parser.add_argument("--profile", default="agenda-facil")
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()

    session = boto3.Session(profile_name=args.profile)

    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)

    log.info("=" * 60)
    log.info("EXTRACAO DE EVIDENCIAS — POC Fraud Detection")
    log.info("=" * 60)

    extract_s3(session, args.region)
    extract_dynamodb(session, args.region)
    extract_sqs_alerts(session, args.region)
    extract_cloudwatch_metrics(session, args.region)
    extract_kinesis_metrics(session, args.region)
    generate_summary(session, args.region)

    log.info("")
    log.info("Extracao completa! Evidencias em: data/evidence/")
    log.info("Agora pode rodar: make teardown")


if __name__ == "__main__":
    main()
