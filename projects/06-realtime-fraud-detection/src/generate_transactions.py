"""
Gerador de transacoes financeiras sinteticas com padroes de fraude injetados.

Gera 10.000 clientes e produz transacoes em modo streaming (yield continuo)
ou batch (JSONL local). Cada transacao tem um campo is_fraud como ground truth
que NAO deve ser enviado pro pipeline de deteccao.

Padroes de fraude (~2-3% do total):
  1. Velocity attack     - >5 transacoes em 2 minutos
  2. Impossible travel    - cidades distantes em intervalo impossivel
  3. Amount anomaly       - valor >3x desvio padrao do historico
  4. Account takeover     - device novo + transacao alta
  5. Micro-testing        - sequencia de valores baixos + 1 alto
"""

import json
import math
import os
import random
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

# --- Constantes ---

NUM_CUSTOMERS = 10_000
FRAUD_RATE = 0.008  # ~0.8% dos eventos disparam fraude, mas cada fraude gera multiplas txns → ~2-3% total
LATE_EVENT_RATE = 0.07  # 7% dos eventos chegam atrasados
OUTPUT_DIR = Path(__file__).parent.parent / "data"

# pre-computar pra performance
_COUNTER = 0

def _fast_id():
    """ID unico rapido (hex do counter + random) — 10x mais rapido que uuid4."""
    global _COUNTER
    _COUNTER += 1
    return f"{_COUNTER:08x}{os.getpid():04x}{random.getrandbits(32):08x}"

PAYMENT_TYPES = ["pix", "credit_card", "ted"]
CATEGORIES = [
    "alimentacao", "transporte", "saude", "educacao", "lazer",
    "vestuario", "eletronicos", "servicos", "supermercado", "combustivel",
]
MERCHANTS = [
    "Mercado Livre", "iFood", "Uber", "99", "Amazon BR",
    "Magazine Luiza", "Americanas", "Shopee", "Rappi", "PicPay",
    "Posto Shell", "Farmacia Raia", "Renner", "C&A", "Padaria Central",
]

# Cidades brasileiras com coordenadas reais
CITIES = [
    ("Sao Paulo", -23.5505, -46.6333),
    ("Rio de Janeiro", -22.9068, -43.1729),
    ("Belo Horizonte", -19.9167, -43.9345),
    ("Salvador", -12.9714, -38.5014),
    ("Brasilia", -15.7975, -47.8919),
    ("Curitiba", -25.4284, -49.2733),
    ("Fortaleza", -3.7172, -38.5433),
    ("Recife", -8.0476, -34.8770),
    ("Porto Alegre", -30.0346, -51.2177),
    ("Manaus", -3.1190, -60.0217),
    ("Belem", -1.4558, -48.5024),
    ("Goiania", -16.6869, -49.2648),
    ("Campinas", -22.9099, -47.0626),
    ("Florianopolis", -27.5954, -48.5480),
    ("Natal", -5.7945, -35.2110),
]


@dataclass
class CustomerProfile:
    customer_id: str
    name: str
    home_city: str
    home_lat: float
    home_lon: float
    device_id: str
    segment: str
    avg_amount: float
    std_amount: float
    avg_txn_per_hour: float
    # estado mutavel pra gerar fraudes coerentes
    last_lat: float = 0.0
    last_lon: float = 0.0
    last_ts: float = 0.0
    recent_amounts: list = field(default_factory=list)


def _haversine_km(lat1, lon1, lat2, lon2):
    """Distancia em km entre dois pontos (formula de haversine)."""
    r = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return r * 2 * math.asin(math.sqrt(a))


def _generate_customers(n):
    """Cria n clientes com perfis realistas."""
    segments = ["premium", "regular", "basico"]
    seg_weights = [0.15, 0.55, 0.30]
    first_names = [
        "Ana", "Carlos", "Maria", "Joao", "Fernanda", "Pedro", "Julia",
        "Lucas", "Beatriz", "Rafael", "Camila", "Bruno", "Larissa", "Diego",
        "Patricia", "Thiago", "Amanda", "Gustavo", "Isabela", "Rodrigo",
    ]
    last_names = [
        "Silva", "Santos", "Oliveira", "Souza", "Pereira", "Costa", "Ferreira",
        "Almeida", "Nascimento", "Lima", "Araujo", "Ribeiro", "Carvalho",
        "Gomes", "Martins", "Rocha", "Barbosa", "Moreira", "Dias", "Teixeira",
    ]

    customers = []
    for i in range(n):
        city_name, lat, lon = random.choice(CITIES)
        seg = random.choices(segments, seg_weights)[0]

        # perfil financeiro varia por segmento
        if seg == "premium":
            avg = random.uniform(500, 3000)
            std = avg * random.uniform(0.3, 0.6)
            freq = random.uniform(2, 8)
        elif seg == "regular":
            avg = random.uniform(80, 500)
            std = avg * random.uniform(0.4, 0.8)
            freq = random.uniform(1, 5)
        else:
            avg = random.uniform(20, 150)
            std = avg * random.uniform(0.3, 0.5)
            freq = random.uniform(0.5, 3)

        c = CustomerProfile(
            customer_id=f"cust_{i:06d}",
            name=f"{random.choice(first_names)} {random.choice(last_names)}",
            home_city=city_name,
            home_lat=lat + random.uniform(-0.05, 0.05),
            home_lon=lon + random.uniform(-0.05, 0.05),
            device_id=_fast_id()[:12],
            segment=seg,
            avg_amount=round(avg, 2),
            std_amount=round(std, 2),
            avg_txn_per_hour=round(freq, 2),
            last_lat=lat,
            last_lon=lon,
            last_ts=time.time(),
        )
        customers.append(c)
    return customers


def _normal_transaction(customer, ts):
    """Gera uma transacao normal pra um cliente."""
    amount = max(1.0, random.gauss(customer.avg_amount, customer.std_amount))
    # pequena variacao na localizacao (mesma cidade)
    lat = customer.home_lat + random.uniform(-0.02, 0.02)
    lon = customer.home_lon + random.uniform(-0.02, 0.02)

    txn = {
        "transaction_id": _fast_id(),
        "customer_id": customer.customer_id,
        "amount": round(amount, 2),
        "currency": "BRL",
        "merchant": random.choice(MERCHANTS),
        "category": random.choice(CATEGORIES),
        "city": customer.home_city,
        "lat": round(lat, 6),
        "lon": round(lon, 6),
        "device_id": customer.device_id,
        "payment_type": random.choice(PAYMENT_TYPES),
        "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
        "is_fraud": False,
        "fraud_type": None,
    }

    customer.last_lat = lat
    customer.last_lon = lon
    customer.last_ts = ts
    customer.recent_amounts.append(amount)
    if len(customer.recent_amounts) > 20:
        customer.recent_amounts.pop(0)

    return txn


def _fraud_velocity(customer, ts):
    """Velocity attack: 6 transacoes rapidas em 2 minutos."""
    txns = []
    for i in range(6):
        t = ts + i * 15  # uma a cada 15 segundos
        txn = _normal_transaction(customer, t)
        txn["is_fraud"] = True
        txn["fraud_type"] = "velocity"
        txn["amount"] = round(random.uniform(100, 800), 2)
        txns.append(txn)
    return txns


def _fraud_impossible_travel(customer, ts):
    """Impossible travel: transacao em cidade distante em <30min."""
    # primeira transacao na cidade do cliente
    txn1 = _normal_transaction(customer, ts)

    # segunda transacao em cidade distante, 20 minutos depois
    distant_cities = [
        (name, lat, lon) for name, lat, lon in CITIES
        if _haversine_km(customer.home_lat, customer.home_lon, lat, lon) > 800
    ]
    if not distant_cities:
        distant_cities = [("Manaus", -3.1190, -60.0217)]

    city_name, lat, lon = random.choice(distant_cities)
    txn2 = _normal_transaction(customer, ts + 20 * 60)
    txn2["city"] = city_name
    txn2["lat"] = round(lat + random.uniform(-0.01, 0.01), 6)
    txn2["lon"] = round(lon + random.uniform(-0.01, 0.01), 6)
    txn2["is_fraud"] = True
    txn2["fraud_type"] = "impossible_travel"
    txn1["is_fraud"] = True
    txn1["fraud_type"] = "impossible_travel"

    return [txn1, txn2]


def _fraud_amount_anomaly(customer, ts):
    """Amount anomaly: valor muito acima do padrao do cliente."""
    txn = _normal_transaction(customer, ts)
    # 4-8x o desvio padrao acima da media
    multiplier = random.uniform(4, 8)
    txn["amount"] = round(customer.avg_amount + multiplier * customer.std_amount, 2)
    txn["is_fraud"] = True
    txn["fraud_type"] = "amount_anomaly"
    return [txn]


def _fraud_account_takeover(customer, ts):
    """Account takeover: device novo + transacao alta."""
    txn = _normal_transaction(customer, ts)
    txn["device_id"] = _fast_id()[:12]  # device diferente
    txn["amount"] = round(random.uniform(5000, 15000), 2)
    txn["is_fraud"] = True
    txn["fraud_type"] = "account_takeover"
    return [txn]


def _fraud_micro_testing(customer, ts):
    """Micro-testing: 3 transacoes pequenas + 1 grande."""
    txns = []
    for i in range(3):
        txn = _normal_transaction(customer, ts + i * 30)
        txn["amount"] = round(random.uniform(0.50, 4.99), 2)
        txn["is_fraud"] = True
        txn["fraud_type"] = "micro_testing"
        txns.append(txn)

    # transacao grande logo depois
    big = _normal_transaction(customer, ts + 120)
    big["amount"] = round(random.uniform(2000, 8000), 2)
    big["is_fraud"] = True
    big["fraud_type"] = "micro_testing"
    txns.append(big)
    return txns


FRAUD_GENERATORS = [
    (_fraud_velocity, 0.20),
    (_fraud_impossible_travel, 0.20),
    (_fraud_amount_anomaly, 0.25),
    (_fraud_account_takeover, 0.20),
    (_fraud_micro_testing, 0.15),
]


def generate_stream(target_tps=2000, duration_seconds=1800):
    """
    Gera transacoes em modo streaming (generator).
    Yield: list de dicts (batch de 1 segundo).
    Pre-gera cada batch pra atingir TPS alto.
    """
    customers = _generate_customers(NUM_CUSTOMERS)
    fraud_funcs = [f for f, _ in FRAUD_GENERATORS]
    fraud_weights = [w for _, w in FRAUD_GENERATORS]

    start = time.time()
    count = 0
    fraud_count = 0
    seconds_elapsed = 0

    while (time.time() - start) < duration_seconds:
        batch_start = time.time()
        batch_ts = time.time()

        for _ in range(target_tps):
            customer = random.choice(customers)

            if random.random() < FRAUD_RATE:
                fraud_fn = random.choices(fraud_funcs, fraud_weights)[0]
                txns = fraud_fn(customer, batch_ts)
                for txn in txns:
                    if random.random() < LATE_EVENT_RATE:
                        delay = random.uniform(1, 30)
                        original_ts = datetime.fromisoformat(txn["timestamp"])
                        txn["timestamp"] = (original_ts - timedelta(seconds=delay)).isoformat()
                    yield txn
                    count += 1
                fraud_count += len(txns)
            else:
                txn = _normal_transaction(customer, batch_ts)
                if random.random() < LATE_EVENT_RATE:
                    delay = random.uniform(1, 30)
                    original_ts = datetime.fromisoformat(txn["timestamp"])
                    txn["timestamp"] = (original_ts - timedelta(seconds=delay)).isoformat()
                yield txn
                count += 1

            batch_ts += 0.0005

        # throttle — so espera se gerou rapido demais
        elapsed = time.time() - batch_start
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)

        seconds_elapsed += 1
        if seconds_elapsed % 10 == 0:
            now = time.time()
            rate = count / (now - start)
            fraud_pct = (fraud_count / count * 100) if count else 0
            print(f"  {count:,} eventos | {rate:.0f} evt/s | fraude: {fraud_pct:.1f}%")

    total_time = time.time() - start
    fraud_pct = (fraud_count / count * 100) if count else 0
    print(f"\nTotal: {count:,} eventos em {total_time:.1f}s "
          f"({count/total_time:.0f} evt/s) | fraude: {fraud_pct:.1f}%")


def generate_batch(num_events=150_000):
    """Gera transacoes em batch e salva como JSONL."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "transactions.jsonl"

    customers = _generate_customers(NUM_CUSTOMERS)
    fraud_funcs = [f for f, _ in FRAUD_GENERATORS]
    fraud_weights = [w for _, w in FRAUD_GENERATORS]

    count = 0
    fraud_count = 0
    base_ts = time.time()
    # espalhar timestamps ao longo de 30 minutos (simula producao real)
    ts_interval = 1800.0 / num_events  # ~0.012s entre eventos

    with open(out_path, "w") as f:
        while count < num_events:
            customer = random.choice(customers)
            ts = base_ts + count * ts_interval

            if random.random() < FRAUD_RATE:
                fraud_fn = random.choices(fraud_funcs, fraud_weights)[0]
                txns = fraud_fn(customer, ts)
                for txn in txns:
                    if random.random() < LATE_EVENT_RATE:
                        delay = random.uniform(1, 30)
                        original_ts = datetime.fromisoformat(txn["timestamp"])
                        late_ts = original_ts - timedelta(seconds=delay)
                        txn["timestamp"] = late_ts.isoformat()
                    f.write(json.dumps(txn, ensure_ascii=False) + "\n")
                    count += 1
                fraud_count += len([t for t in txns if t["is_fraud"]])
            else:
                txn = _normal_transaction(customer, ts)
                if random.random() < LATE_EVENT_RATE:
                    delay = random.uniform(1, 30)
                    original_ts = datetime.fromisoformat(txn["timestamp"])
                    late_ts = original_ts - timedelta(seconds=delay)
                    txn["timestamp"] = late_ts.isoformat()
                f.write(json.dumps(txn, ensure_ascii=False) + "\n")
                count += 1

    fraud_pct = (fraud_count / count * 100) if count else 0
    size_mb = out_path.stat().st_size / 1024 / 1024
    print(f"Gerado: {out_path}")
    print(f"  {count:,} transacoes | {size_mb:.1f} MB | fraude: {fraud_pct:.1f}%")

    # salvar perfis dos clientes (pra seed do DynamoDB)
    profiles_path = OUTPUT_DIR / "customer_profiles.jsonl"
    with open(profiles_path, "w") as f:
        for c in customers:
            profile = {
                "customer_id": c.customer_id,
                "name": c.name,
                "home_city": c.home_city,
                "avg_amount": c.avg_amount,
                "std_amount": c.std_amount,
                "last_lat": c.home_lat,
                "last_lon": c.home_lon,
                "last_device_id": c.device_id,
                "avg_txn_per_hour": c.avg_txn_per_hour,
                "segment": c.segment,
            }
            f.write(json.dumps(profile, ensure_ascii=False) + "\n")
    print(f"  {len(customers):,} perfis salvos em {profiles_path}")


def get_customers():
    """Retorna lista de clientes (pra uso pelo producer)."""
    return _generate_customers(NUM_CUSTOMERS)


if __name__ == "__main__":
    print("Gerando transacoes em batch...")
    generate_batch()
