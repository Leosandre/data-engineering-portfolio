"""
Lambda: detecta fraude em transacoes do Kinesis em tempo real.

Otimizado pra throughput: batch reads/writes no DynamoDB em vez de
chamadas individuais por record. Processa 500 records por invocacao
com ~2 chamadas DynamoDB (BatchGetItem + BatchWriteItem).
"""

import base64
import json
import math
import os
import time
import logging
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

import boto3

log = logging.getLogger()
log.setLevel(logging.INFO)

PROFILES_TABLE = os.environ.get("PROFILES_TABLE", "fraud-detection-poc-customer-profiles")
STATE_TABLE = os.environ.get("STATE_TABLE", "fraud-detection-poc-detection-state")
OUTPUT_STREAM = os.environ.get("OUTPUT_STREAM", "fraud-detection-poc-alerts")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")

dynamodb = boto3.resource("dynamodb")
kinesis = boto3.client("kinesis")
sns = boto3.client("sns")

profiles_table = dynamodb.Table(PROFILES_TABLE)
state_table = dynamodb.Table(STATE_TABLE)


def _batch_get_profiles(customer_ids):
    """Busca perfis de multiplos clientes em uma unica chamada."""
    profiles = {}
    unique_ids = list(set(customer_ids))

    # BatchGetItem aceita max 100 keys
    for i in range(0, len(unique_ids), 100):
        chunk = unique_ids[i:i + 100]
        resp = dynamodb.batch_get_item(RequestItems={
            PROFILES_TABLE: {
                "Keys": [{"customer_id": cid} for cid in chunk],
            }
        })
        for item in resp.get("Responses", {}).get(PROFILES_TABLE, []):
            p = {k: float(v) if hasattr(v, "as_tuple") else v for k, v in item.items()}
            profiles[p["customer_id"]] = p

    return profiles


def _batch_get_states(customer_ids):
    """Busca estados de deteccao de multiplos clientes."""
    states = {}
    unique_ids = list(set(customer_ids))

    for i in range(0, len(unique_ids), 100):
        chunk = unique_ids[i:i + 100]
        resp = dynamodb.batch_get_item(RequestItems={
            STATE_TABLE: {
                "Keys": [{"customer_id": cid} for cid in chunk],
            }
        })
        for item in resp.get("Responses", {}).get(STATE_TABLE, []):
            s = {k: (float(v) if hasattr(v, "as_tuple") else
                     [float(x) if hasattr(x, "as_tuple") else x for x in v] if isinstance(v, list) else v)
                 for k, v in item.items()}
            states[s["customer_id"]] = s

    return states


def _batch_write_states(states_to_write):
    """Escreve estados atualizados em batch."""
    ttl = int(time.time()) + 300

    with state_table.batch_writer() as batch:
        for cid, state in states_to_write.items():
            item = {"customer_id": cid, "ttl": ttl}
            for k, v in state.items():
                if k in ("customer_id", "ttl"):
                    continue
                if isinstance(v, float):
                    item[k] = Decimal(str(round(v, 6)))
                elif isinstance(v, list):
                    item[k] = [Decimal(str(round(x, 6))) if isinstance(x, float) else x for x in v]
                else:
                    item[k] = v
            batch.put_item(Item=item)


def _haversine_km(lat1, lon1, lat2, lon2):
    r = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return r * 2 * math.asin(min(1.0, math.sqrt(a)))


def detect(txn, profile, state):
    """Aplica 5 regras e retorna (score, rules_triggered, updated_state)."""
    scores = []
    triggered = []

    # usar event time da transacao (nao processing time)
    try:
        event_ts = datetime.fromisoformat(txn["timestamp"]).timestamp()
    except (ValueError, KeyError):
        event_ts = time.time()

    # 1. Velocity: >3 transacoes em 10 segundos (event time)
    # Janela curta (10s) com threshold baixo pega bursts reais
    # sem disparar pra clientes normais (~0.2 txn/s = ~2 em 10s)
    timestamps = state.get("velocity_timestamps", [])
    timestamps = [float(ts) for ts in timestamps if (event_ts - float(ts)) < 10]
    timestamps.append(event_ts)
    state["velocity_timestamps"] = timestamps[-30:]
    if len(timestamps) > 3:
        scores.append(0.9)
        triggered.append("velocity")

    # 2. Impossible travel: >500km em <1h
    if txn.get("lat") and txn.get("lon"):
        last_lat = state.get("last_lat") or (profile.get("last_lat") if profile else None)
        last_lon = state.get("last_lon") or (profile.get("last_lon") if profile else None)
        last_ts = state.get("last_txn_ts", 0)
        if last_lat and last_lon:
            dist = _haversine_km(float(last_lat), float(last_lon), txn["lat"], txn["lon"])
            elapsed_h = (event_ts - float(last_ts)) / 3600 if float(last_ts) > 0 else 999
            if dist > 500 and elapsed_h < 1:
                scores.append(0.85)
                triggered.append("impossible_travel")

    # 3. Amount anomaly: >3x desvio padrao
    if profile:
        avg = profile.get("avg_amount", 0)
        std = profile.get("std_amount", 0)
        if std > 0 and txn["amount"] > avg + 3 * std:
            scores.append(0.75)
            triggered.append("amount_anomaly")
        elif std == 0 and txn["amount"] > 5000:
            scores.append(0.6)
            triggered.append("amount_anomaly")

    # 4. Account takeover: device novo + valor alto
    if profile:
        last_device = profile.get("last_device_id", "")
        if txn.get("device_id") and txn["device_id"] != last_device and txn["amount"] > 5000:
            scores.append(0.95)
            triggered.append("account_takeover")

    # 5. Micro-testing: 3+ txns <R$5 seguidas de 1 >R$2000
    recent = state.get("recent_amounts", [])
    recent = [float(a) for a in recent]
    recent.append(txn["amount"])
    recent = recent[-10:]
    state["recent_amounts"] = recent
    if len(recent) >= 4 and txn["amount"] > 2000:
        low_count = 0
        for a in reversed(recent[:-1]):
            if a < 5:
                low_count += 1
            else:
                break
        if low_count >= 3:
            scores.append(0.88)
            triggered.append("micro_testing")

    # atualizar estado com event time
    state["last_lat"] = txn.get("lat", 0)
    state["last_lon"] = txn.get("lon", 0)
    state["last_txn_ts"] = event_ts

    return max(scores) if scores else 0.0, triggered, state


def handler(event, context):
    """Lambda handler — batch do Kinesis."""
    records = event.get("Records", [])
    if not records:
        return {"statusCode": 200}

    # decodificar todos os records
    txns = []
    for record in records:
        payload = base64.b64decode(record["kinesis"]["data"])
        txns.append(json.loads(payload))

    # batch read: perfis + estados (2 chamadas DynamoDB pra todo o batch)
    customer_ids = [t["customer_id"] for t in txns]
    profiles = _batch_get_profiles(customer_ids)
    states = _batch_get_states(customer_ids)

    # processar cada transacao
    output_buffer = []
    alert_count = 0
    states_to_write = {}

    for txn in txns:
        cid = txn["customer_id"]
        profile = profiles.get(cid)
        state = states.get(cid, {})

        score, triggered, updated_state = detect(txn, profile, state)
        states[cid] = updated_state  # atualizar pra proxima txn do mesmo cliente no batch
        states_to_write[cid] = updated_state

        result = {
            **txn,
            "fraud_score": round(score, 3),
            "rules_triggered": triggered,
            "detection_timestamp": datetime.now(timezone.utc).isoformat(),
        }

        output_buffer.append({
            "Data": json.dumps(result, ensure_ascii=False).encode("utf-8"),
            "PartitionKey": cid,
        })

        if score >= 0.7:
            alert_count += 1

    # publicar resumo de alertas no SNS (1 mensagem por batch, nao por alerta)
    if alert_count > 0 and SNS_TOPIC_ARN:
        try:
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=f"Fraude: {alert_count} alertas no batch",
                Message=json.dumps({"alerts": alert_count, "batch_size": len(txns)}, ensure_ascii=False),
            )
        except Exception as e:
            log.error(f"SNS: {e}")

    # batch write: estados atualizados (1 chamada DynamoDB)
    _batch_write_states(states_to_write)

    # enviar pro Kinesis output
    for i in range(0, len(output_buffer), 500):
        batch = output_buffer[i:i + 500]
        try:
            kinesis.put_records(StreamName=OUTPUT_STREAM, Records=batch)
        except Exception as e:
            log.error(f"Kinesis output: {e}")

    log.info(f"Processados: {len(txns)} | Alertas: {alert_count}")
    return {"statusCode": 200, "processed": len(txns), "alerts": alert_count}
