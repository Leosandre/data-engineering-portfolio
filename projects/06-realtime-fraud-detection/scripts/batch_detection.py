"""
Deteccao de fraude em batch — mesmas 5 regras aplicadas sobre dados do S3.

Simula o cenario batch: roda a cada 15 minutos, processa tudo que chegou,
e registra detection_timestamp = agora (pra comparar latencia com streaming).

Uso:
    python scripts/batch_detection.py --profile agenda-facil --region us-east-1
"""

import argparse
import gzip
import json
import logging
import math
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PROJECT = "fraud-detection-poc"
EVIDENCE_DIR = Path(__file__).parent.parent / "data" / "evidence"


def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return r * 2 * math.asin(min(1.0, math.sqrt(a)))


def load_profiles(session, region):
    """Carrega perfis do DynamoDB."""
    dynamo = session.resource("dynamodb", region_name=region)
    table = dynamo.Table(f"{PROJECT}-customer-profiles")

    profiles = {}
    scan_kwargs = {}
    while True:
        resp = table.scan(**scan_kwargs)
        for item in resp["Items"]:
            p = {k: float(v) if hasattr(v, "as_tuple") else v for k, v in item.items()}
            profiles[p["customer_id"]] = p
        if "LastEvaluatedKey" not in resp:
            break
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    log.info(f"  {len(profiles):,} perfis carregados")
    return profiles


def load_transactions_from_s3(session, region):
    """Baixa transacoes processadas do S3 (output do Firehose)."""
    s3 = session.client("s3", region_name=region)
    account = session.client("sts").get_caller_identity()["Account"]
    bucket = f"{PROJECT}-lake-{account}"

    txns = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix="processed/"):
        for obj in page.get("Contents", []):
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
            try:
                text = gzip.decompress(body).decode("utf-8")
            except gzip.BadGzipFile:
                text = body.decode("utf-8")
            for line in text.strip().split("\n"):
                if line.strip():
                    txns.append(json.loads(line))

    log.info(f"  {len(txns):,} transacoes carregadas do S3")
    return txns


def load_transactions_local():
    """Carrega transacoes do ground truth local (fallback se S3 vazio)."""
    gt_path = Path(__file__).parent.parent / "data" / "ground_truth.jsonl"
    txn_path = Path(__file__).parent.parent / "data" / "transactions.jsonl"

    if not txn_path.exists():
        log.warning("Arquivo transactions.jsonl nao encontrado")
        return []

    txns = []
    # carregar ground truth pra ter is_fraud
    gt = {}
    if gt_path.exists():
        with open(gt_path) as f:
            for line in f:
                r = json.loads(line)
                gt[r["transaction_id"]] = r

    with open(txn_path) as f:
        for line in f:
            txns.append(json.loads(line))

    log.info(f"  {len(txns):,} transacoes carregadas localmente")
    return txns


def batch_detect(txns, profiles):
    """Aplica as 5 regras em batch sobre todas as transacoes."""
    detection_ts = datetime.now(timezone.utc).isoformat()

    # agrupar por customer_id e ordenar por timestamp
    by_customer = defaultdict(list)
    for txn in txns:
        by_customer[txn["customer_id"]].append(txn)

    results = []
    for cid, customer_txns in by_customer.items():
        customer_txns.sort(key=lambda t: t.get("timestamp", ""))
        profile = profiles.get(cid, {})

        for i, txn in enumerate(customer_txns):
            scores = []
            triggered = []

            # 1. Velocity: >8 txns em janela de 2 minutos (usando event time)
            if i >= 3:
                try:
                    ts_current = datetime.fromisoformat(txn["timestamp"]).timestamp()
                    in_window = 0
                    for t in customer_txns[max(0, i - 20):i + 1]:
                        ts_t = datetime.fromisoformat(t["timestamp"]).timestamp()
                        if abs(ts_current - ts_t) < 120:
                            in_window += 1
                    if in_window > 6:
                        scores.append(0.9)
                        triggered.append("velocity")
                except (ValueError, KeyError):
                    pass

            # 2. Impossible travel
            if i > 0 and txn.get("lat") and txn.get("lon"):
                prev = customer_txns[i - 1]
                if prev.get("lat") and prev.get("lon"):
                    try:
                        dist = haversine_km(prev["lat"], prev["lon"], txn["lat"], txn["lon"])
                        ts1 = datetime.fromisoformat(prev["timestamp"]).timestamp()
                        ts2 = datetime.fromisoformat(txn["timestamp"]).timestamp()
                        elapsed_h = abs(ts2 - ts1) / 3600
                        if dist > 500 and elapsed_h < 1:
                            scores.append(0.85)
                            triggered.append("impossible_travel")
                    except (ValueError, KeyError):
                        pass

            # 3. Amount anomaly
            avg = profile.get("avg_amount", 0)
            std = profile.get("std_amount", 0)
            if std > 0 and txn.get("amount", 0) > avg + 3 * std:
                scores.append(0.75)
                triggered.append("amount_anomaly")
            elif std == 0 and txn.get("amount", 0) > 5000:
                scores.append(0.6)
                triggered.append("amount_anomaly")

            # 4. Account takeover
            last_device = profile.get("last_device_id", "")
            if (txn.get("device_id") and txn["device_id"] != last_device
                    and txn.get("amount", 0) > 5000):
                scores.append(0.95)
                triggered.append("account_takeover")

            # 5. Micro-testing
            if i >= 3:
                recent = [t.get("amount", 0) for t in customer_txns[max(0, i - 3):i]]
                if (all(a < 5 for a in recent) and len(recent) >= 3
                        and txn.get("amount", 0) > 2000):
                    scores.append(0.88)
                    triggered.append("micro_testing")

            final_score = max(scores) if scores else 0.0

            results.append({
                "transaction_id": txn.get("transaction_id", ""),
                "customer_id": cid,
                "amount": txn.get("amount", 0),
                "fraud_score": round(final_score, 3),
                "rules_triggered": triggered,
                "detection_timestamp": detection_ts,
                "event_timestamp": txn.get("timestamp", ""),
                "is_fraud": txn.get("is_fraud"),
                "fraud_type": txn.get("fraud_type"),
            })

    return results


def main():
    parser = argparse.ArgumentParser(description="Batch fraud detection")
    parser.add_argument("--profile", default="agenda-facil")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--local", action="store_true", help="Usar dados locais em vez de S3")
    args = parser.parse_args()

    session = boto3.Session(profile_name=args.profile)

    log.info("=== Batch Fraud Detection ===")

    # carregar perfis
    log.info("Carregando perfis...")
    if args.local:
        profiles = {}
        ppath = Path(__file__).parent.parent / "data" / "customer_profiles.jsonl"
        with open(ppath) as f:
            for line in f:
                p = json.loads(line)
                profiles[p["customer_id"]] = p
        log.info(f"  {len(profiles):,} perfis carregados localmente")
    else:
        profiles = load_profiles(session, args.region)

    # carregar transacoes
    log.info("Carregando transacoes...")
    if args.local:
        txns = load_transactions_local()
    else:
        txns = load_transactions_from_s3(session, args.region)
        if not txns:
            log.warning("S3 vazio, usando dados locais")
            txns = load_transactions_local()

    # detectar
    log.info("Aplicando regras...")
    start = time.time()
    results = batch_detect(txns, profiles)
    elapsed = time.time() - start

    # salvar resultados
    out_dir = Path(__file__).parent.parent / "data" / "batch_results"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "batch_detections.jsonl"

    alerts = [r for r in results if r["fraud_score"] >= 0.7]

    with open(out_path, "w") as f:
        for r in results:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    log.info(f"\n=== Resultado ===")
    log.info(f"  Transacoes: {len(results):,}")
    log.info(f"  Alertas:    {len(alerts):,} ({len(alerts)/len(results)*100:.1f}%)")
    log.info(f"  Tempo:      {elapsed:.1f}s")
    log.info(f"  Salvo em:   {out_path}")


if __name__ == "__main__":
    main()
