"""
Producer: envia transacoes pro Kinesis Data Streams e popula DynamoDB com perfis.

Usa ThreadPoolExecutor pra enviar batches em paralelo e garantir 2.000+ TPS.

Uso:
    python src/producer.py --profile agenda-facil --region us-east-1
    python src/producer.py --seed-only   # so popula DynamoDB
"""

import argparse
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal

import boto3

from generate_transactions import generate_stream, get_customers

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PROJECT = "fraud-detection-poc"
STREAM_NAME = f"{PROJECT}-transactions"
TABLE_NAME = f"{PROJECT}-customer-profiles"
KINESIS_BATCH = 500  # max do PutRecords
GROUND_TRUTH_FIELDS = ("is_fraud", "fraud_type")


def seed_dynamodb(session, region):
    """Popula DynamoDB com perfis iniciais dos 10K clientes."""
    dynamo = session.resource("dynamodb", region_name=region)
    table = dynamo.Table(TABLE_NAME)

    customers = get_customers()
    log.info(f"Populando DynamoDB com {len(customers):,} perfis...")

    count = 0
    with table.batch_writer() as batch:
        for c in customers:
            item = {
                "customer_id": c.customer_id,
                "name": c.name,
                "home_city": c.home_city,
                "avg_amount": Decimal(str(round(c.avg_amount, 2))),
                "std_amount": Decimal(str(round(c.std_amount, 2))),
                "last_lat": Decimal(str(round(c.home_lat, 6))),
                "last_lon": Decimal(str(round(c.home_lon, 6))),
                "last_device_id": c.device_id,
                "avg_txn_per_hour": Decimal(str(round(c.avg_txn_per_hour, 2))),
                "segment": c.segment,
            }
            batch.put_item(Item=item)
            count += 1

    log.info(f"  {count:,} perfis inseridos no DynamoDB")
    return count


def _send_batch(kinesis, records):
    """Envia um batch de records pro Kinesis. Retorna (sent, failed)."""
    try:
        resp = kinesis.put_records(StreamName=STREAM_NAME, Records=records)
        failed = resp.get("FailedRecordCount", 0)
        return len(records) - failed, failed
    except Exception as e:
        log.error(f"PutRecords falhou: {e}")
        return 0, len(records)


def produce_to_kinesis(session, region, duration=1800, tps=2000):
    """
    Envia transacoes pro Kinesis em paralelo.

    Salva ground truth (is_fraud, fraud_type) em arquivo local
    pra validacao posterior — esses campos NAO vao pro Kinesis.
    """
    kinesis = session.client("kinesis", region_name=region)

    log.info(f"Enviando ~{tps} evt/s pro Kinesis por {duration}s ({duration//60}min)...")
    log.info(f"  Stream: {STREAM_NAME}")
    log.info(f"  Threads: 4 (paralelo)")

    ground_truth_path = "data/ground_truth.jsonl"
    import os
    os.makedirs("data", exist_ok=True)

    sent_total = 0
    failed_total = 0
    start = time.time()
    buffer = []

    with open(ground_truth_path, "w") as gt_file, \
         ThreadPoolExecutor(max_workers=4) as executor:

        futures = []

        for txn in generate_stream(target_tps=tps, duration_seconds=duration):
            # salvar ground truth local
            gt_file.write(json.dumps({
                "transaction_id": txn["transaction_id"],
                "is_fraud": txn["is_fraud"],
                "fraud_type": txn["fraud_type"],
                "timestamp": txn["timestamp"],
            }, ensure_ascii=False) + "\n")

            # remover ground truth antes de enviar
            payload = {k: v for k, v in txn.items() if k not in GROUND_TRUTH_FIELDS}

            buffer.append({
                "Data": json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                "PartitionKey": txn["customer_id"],
            })

            if len(buffer) >= KINESIS_BATCH:
                batch_to_send = buffer
                buffer = []
                futures.append(executor.submit(_send_batch, kinesis, batch_to_send))

                # colher resultados de futures completos pra nao acumular
                done = [f for f in futures if f.done()]
                for f in done:
                    s, fail = f.result()
                    sent_total += s
                    failed_total += fail
                    futures.remove(f)

        # flush do que sobrou
        if buffer:
            futures.append(executor.submit(_send_batch, kinesis, buffer))

        # esperar todos terminarem
        for f in as_completed(futures):
            s, fail = f.result()
            sent_total += s
            failed_total += fail

    elapsed = time.time() - start
    log.info(f"\n=== Resultado ===")
    log.info(f"  Enviados:  {sent_total:,}")
    log.info(f"  Falhas:    {failed_total:,}")
    log.info(f"  Tempo:     {elapsed:.1f}s")
    log.info(f"  Rate:      {sent_total/elapsed:.0f} evt/s")
    log.info(f"  Ground truth salvo em: {ground_truth_path}")

    if failed_total > 0:
        log.warning(f"  {failed_total:,} records falharam — possivel throttling nos shards")


def main():
    parser = argparse.ArgumentParser(description="Producer de transacoes")
    parser.add_argument("--profile", default="agenda-facil")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--duration", type=int, default=1800, help="Duracao em segundos (default: 30min)")
    parser.add_argument("--tps", type=int, default=2000, help="Transacoes por segundo")
    parser.add_argument("--seed-only", action="store_true", help="So popula DynamoDB")
    parser.add_argument("--skip-seed", action="store_true", help="Pula seed do DynamoDB")
    args = parser.parse_args()

    session = boto3.Session(profile_name=args.profile)

    if not args.skip_seed:
        seed_dynamodb(session, args.region)

    if not args.seed_only:
        produce_to_kinesis(session, args.region, args.duration, args.tps)


if __name__ == "__main__":
    main()
