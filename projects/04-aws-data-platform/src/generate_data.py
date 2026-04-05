"""
Gera dados sinteticos simulando 4 fontes de um e-commerce.
Injeta problemas reais: nomes diferentes, emails divergentes,
valores que nao batem, clientes duplicados, padroes de churn.

Fontes geradas:
  - ERP (Parquet): customers, orders, order_items
  - CRM (JSON): contacts com aninhamento (address{}, tags[], interactions[])
  - Gateway de pagamento (CSV pipe-delimited): transactions
  - Google Analytics (JSON): sessions com eventos aninhados
"""

import json
import logging
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
from faker import Faker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

fake = Faker("pt_BR")
Faker.seed(42)
random.seed(42)

OUT = Path(__file__).resolve().parent.parent / "data" / "synthetic"

N_CUSTOMERS = 10_000
N_ORDERS = 100_000
N_CRM_CONTACTS = 8_000  # 70% overlap com ERP
N_GATEWAY_TXN = 95_000
N_SESSIONS = 50_000
N_PRODUCTS = 500

START_DATE = datetime(2022, 1, 1)
END_DATE = datetime(2024, 12, 31)


def _rand_date(start=START_DATE, end=END_DATE):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def _rand_datetime(start=START_DATE, end=END_DATE):
    d = _rand_date(start, end)
    return d.replace(hour=random.randint(6, 23), minute=random.randint(0, 59))


def _corrupt_name(name):
    """Gera variacao do nome (simula fontes diferentes)."""
    r = random.random()
    if r < 0.3:
        return name.upper()
    elif r < 0.5:
        parts = name.split()
        if len(parts) > 2:
            return f"{parts[0]} {parts[-1]}"  # remove nome do meio
        return name
    elif r < 0.7:
        parts = name.split()
        if len(parts) > 1:
            return f"{parts[0][0]}. {' '.join(parts[1:])}"  # abrevia primeiro nome
        return name
    return name


def _mess_cpf(cpf):
    """Gera variacao do CPF simulando fontes diferentes.
    Formatos possiveis: 12345678901, 123.456.789-01, 123456789-01,
    123.456.78901, 123 456 789 01, com espacos, com zeros a esquerda cortados, etc.
    """
    digits = cpf.replace(".", "").replace("-", "").replace(" ", "")
    r = random.random()
    if r < 0.20:
        return digits  # so digitos
    elif r < 0.35:
        return f"{digits[:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:]}"  # formato padrao
    elif r < 0.45:
        return f"{digits[:3]}{digits[3:6]}{digits[6:9]}-{digits[9:]}"  # so traco
    elif r < 0.55:
        return f"{digits[:3]}.{digits[3:6]}.{digits[6:9]}{digits[9:]}"  # so pontos
    elif r < 0.65:
        return f"{digits[:3]} {digits[3:6]} {digits[6:9]} {digits[9:]}"  # espacos
    elif r < 0.75:
        return f" {digits} "  # espacos nas pontas
    elif r < 0.85:
        return f"0{digits}"  # zero extra na frente
    elif r < 0.90:
        return digits.lstrip("0")  # zeros a esquerda cortados
    else:
        return f"{digits[:3]}.{digits[3:6]}.{digits[6:9]}/{digits[9:]}"  # barra em vez de traco


# --- Gerar base de clientes (verdade) ---

def _generate_base_customers():
    log.info("Gerando base de clientes...")
    customers = []
    for i in range(N_CUSTOMERS):
        created = _rand_date(START_DATE, START_DATE + timedelta(days=365))
        customers.append({
            "customer_id": i + 1,
            "name": fake.name(),
            "email": fake.email(),
            "cpf": fake.cpf(),
            "phone": fake.phone_number(),
            "address_street": fake.street_address(),
            "address_city": fake.city(),
            "address_state": fake.state_abbr(),
            "address_zip": fake.postcode(),
            "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat(),
            "created_at": created.isoformat(),
        })
        # churn: 15% dos clientes param de comprar cedo e compram menos
        is_churned = random.random() < 0.15
        customers[-1]["_churned"] = is_churned
        customers[-1]["_last_purchase_cap"] = (END_DATE - timedelta(days=random.randint(90, 300))).isoformat() if is_churned else END_DATE.isoformat()
        customers[-1]["_order_weight"] = random.uniform(0.2, 0.5) if is_churned else 1.0
    return customers


# --- Produtos ---

def _generate_products():
    categories = ["Eletrônicos", "Roupas", "Casa", "Esportes", "Livros", "Beleza", "Alimentos", "Brinquedos"]
    products = []
    for i in range(N_PRODUCTS):
        products.append({
            "product_id": i + 1,
            "name": fake.catch_phrase(),
            "category": random.choice(categories),
            "price": round(random.uniform(10, 2000), 2),
        })
    return products


# --- ERP (PostgreSQL-like) ---

def generate_erp(customers, products):
    log.info("Gerando ERP (customers + orders + items)...")

    # ERP customers: nomes em UPPERCASE, CPF com/sem mascara
    erp_customers = []
    for c in customers:
        erp_customers.append({
            "customer_id": c["customer_id"],
            "name": c["name"].upper(),  # ERP sempre uppercase
            "email": c["email"],
            "cpf": _mess_cpf(c["cpf"]),
            "phone": c["phone"],
            "city": c["address_city"],
            "state": c["address_state"],
            "created_at": c["created_at"],
        })

    pl.DataFrame(erp_customers).write_parquet(OUT / "erp_customers.parquet")
    pl.DataFrame(products).write_parquet(OUT / "erp_products.parquet")

    # Orders
    orders = []
    items = []
    item_id = 0
    oid = 0
    for _ in range(N_ORDERS * 2):  # gera mais tentativas pra compensar skips
        cust = random.choice(customers)
        cap = datetime.fromisoformat(cust["_last_purchase_cap"])
        # clientes churned tem menos chance de gerar pedido (weight < 1)
        if random.random() > cust.get("_order_weight", 1.0):
            continue
        oid += 1
        if oid > N_ORDERS:
            break
        order_date = _rand_datetime(START_DATE, cap)

        n_items = random.randint(1, 5)
        total = 0
        for _ in range(n_items):
            item_id += 1
            prod = random.choice(products)
            qty = random.randint(1, 3)
            price = prod["price"]
            total += price * qty
            items.append({
                "item_id": item_id,
                "order_id": oid,
                "product_id": prod["product_id"],
                "quantity": qty,
                "unit_price": price,
            })

        orders.append({
            "order_id": oid,
            "customer_id": cust["customer_id"],
            "order_date": order_date.isoformat(),
            "total_amount": round(total, 2),
            "status": random.choices(["completed", "cancelled", "refunded"], weights=[85, 10, 5])[0],
        })

    pl.DataFrame(orders).write_parquet(OUT / "erp_orders.parquet")
    pl.DataFrame(items).write_parquet(OUT / "erp_order_items.parquet")
    log.info("  ERP: %d customers, %d orders, %d items", len(erp_customers), len(orders), len(items))
    return orders


def generate_crm(customers):
    """CRM: JSON aninhado, 70% overlap com ERP, nomes com variacoes."""
    log.info("Gerando CRM (contacts JSON)...")

    # pega 70% dos clientes do ERP + 30% novos (leads)
    erp_overlap = random.sample(customers, int(N_CRM_CONTACTS * 0.7))
    n_leads = N_CRM_CONTACTS - len(erp_overlap)

    contacts = []
    for c in erp_overlap:
        # 15% tem email diferente do ERP
        email = fake.email() if random.random() < 0.15 else c["email"]
        contacts.append({
            "contact_id": f"CRM-{c['customer_id']:06d}",
            "_erp_customer_id": c["customer_id"],
            "first_name": _corrupt_name(c["name"]).split()[0],
            "last_name": " ".join(_corrupt_name(c["name"]).split()[1:]) or c["name"].split()[-1],
            "email": email,
            "cpf": _mess_cpf(c["cpf"]) if random.random() < 0.80 else None,  # 20% sem CPF no CRM
            "phone": c["phone"] if random.random() > 0.3 else None,
            "address": {
                "street": c["address_street"],
                "city": c["address_city"],
                "state": c["address_state"],
                "zip": c["address_zip"],
            },
            "tags": random.sample(["vip", "newsletter", "promo", "inactive", "new", "returning"], k=random.randint(0, 3)),
            "segment": random.choice(["premium", "standard", "basic"]),
            "interactions": [
                {
                    "type": random.choice(["email", "phone", "chat", "ticket"]),
                    "date": _rand_datetime().isoformat(),
                    "subject": fake.sentence(nb_words=4),
                    "resolved": random.random() > 0.2,
                }
                for _ in range(random.randint(0, 8))
            ],
            "created_at": c["created_at"],
        })

    # leads que nao existem no ERP
    for i in range(n_leads):
        contacts.append({
            "contact_id": f"CRM-L{i:06d}",
            "_erp_customer_id": None,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "cpf": _mess_cpf(fake.cpf()) if random.random() < 0.5 else None,
            "phone": fake.phone_number() if random.random() > 0.4 else None,
            "address": {
                "street": fake.street_address(),
                "city": fake.city(),
                "state": fake.state_abbr(),
                "zip": fake.postcode(),
            },
            "tags": random.sample(["lead", "newsletter", "promo"], k=random.randint(0, 2)),
            "segment": "lead",
            "interactions": [],
            "created_at": _rand_date().isoformat(),
        })

    with open(OUT / "crm_contacts.json", "w") as f:
        json.dump(contacts, f, ensure_ascii=False, indent=2)

    log.info("  CRM: %d contacts (%d overlap ERP, %d leads)", len(contacts), len(erp_overlap), n_leads)


def generate_gateway(orders, customers):
    """Gateway de pagamento: CSV pipe-delimited, valores com divergencias."""
    log.info("Gerando Gateway (transactions CSV)...")

    # pega 95% dos pedidos do ERP
    sampled = random.sample(orders, min(N_GATEWAY_TXN, int(len(orders) * 0.95)))

    cust_map = {c["customer_id"]: c for c in customers}
    rows = []
    for o in sampled:
        cust = cust_map.get(o["customer_id"], {})
        amount = o["total_amount"]

        # 5% com valor divergente (centavos de diferenca ou estorno parcial)
        if random.random() < 0.05:
            amount = round(amount + random.uniform(-5, 5), 2)

        status = "approved"
        if o["status"] == "cancelled":
            status = "declined"
        elif o["status"] == "refunded":
            status = "refunded"

        cpf_digits = cust.get("cpf", "").replace(".", "").replace("-", "").replace(" ", "")
        rows.append({
            "transaction_id": f"GW-{o['order_id']:08d}",
            "order_reference": str(o["order_id"]),
            "email": cust.get("email", ""),
            "cpf": _mess_cpf(cust["cpf"]) if cust.get("cpf") and random.random() < 0.7 else None,  # 30% sem CPF
            "amount": str(amount).replace(".", ","),  # virgula decimal
            "currency": "BRL",
            "status": status,
            "payment_method": random.choice(["credit_card", "debit_card", "pix", "boleto"]),
            "transaction_date": datetime.fromisoformat(o["order_date"]).strftime("%m/%d/%Y %H:%M"),  # formato americano
            "card_brand": random.choice(["visa", "mastercard", "elo", ""]) if "card" in random.choice(["credit_card", ""]) else "",
        })

    # escreve CSV com pipe delimiter
    header = "|".join(rows[0].keys())
    lines = [header] + ["|".join(str(v) for v in r.values()) for r in rows]
    (OUT / "gateway_transactions.csv").write_text("\n".join(lines), encoding="utf-8")

    log.info("  Gateway: %d transactions", len(rows))


def generate_analytics(customers):
    """Google Analytics: JSON com sessoes e eventos aninhados."""
    log.info("Gerando Analytics (sessions JSON)...")

    sessions = []
    for _ in range(N_SESSIONS):
        # 60% com user_id mapeavel, 40% anonimo
        if random.random() < 0.6:
            cust = random.choice(customers)
            user_id = f"UA-{cust['customer_id']:06d}"
            email = cust["email"] if random.random() < 0.3 else None
        else:
            user_id = None
            email = None

        session_start = _rand_datetime()
        n_pages = random.randint(1, 15)
        events = []
        for j in range(n_pages):
            events.append({
                "timestamp": (session_start + timedelta(minutes=j * random.randint(1, 5))).isoformat(),
                "event_type": random.choice(["page_view", "add_to_cart", "purchase", "search", "click"]),
                "page": random.choice(["/home", "/product/123", "/cart", "/checkout", "/category/eletronicos", "/search"]),
                "metadata": {
                    "device": random.choice(["mobile", "desktop", "tablet"]),
                    "browser": random.choice(["chrome", "safari", "firefox", "edge"]),
                },
            })

        sessions.append({
            "session_id": fake.uuid4(),
            "user_id": user_id,
            "email": email,
            "session_start": session_start.isoformat(),
            "session_duration_seconds": random.randint(10, 1800),
            "events": events,
            "source": random.choice(["organic", "paid", "direct", "social", "email"]),
            "converted": random.random() < 0.08,
        })

    with open(OUT / "analytics_sessions.json", "w") as f:
        json.dump(sessions, f, ensure_ascii=False)

    log.info("  Analytics: %d sessions", len(sessions))


if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)

    customers = _generate_base_customers()
    products = _generate_products()

    generate_erp(customers, products)
    orders = generate_erp.__code__  # ja gerou acima, preciso dos orders
    # re-gerar pra pegar orders
    # na verdade vou refatorar pra retornar orders

    # gerar orders separadamente
    orders_data = pl.read_parquet(OUT / "erp_orders.parquet").to_dicts()

    generate_crm(customers)
    generate_gateway(orders_data, customers)
    generate_analytics(customers)

    # salvar base de verdade (pra validacao depois)
    truth = [{"customer_id": c["customer_id"], "name": c["name"], "email": c["email"],
              "cpf": c["cpf"], "churned": c["_churned"]} for c in customers]
    pl.DataFrame(truth).write_parquet(OUT / "_ground_truth.parquet")

    log.info("Geracao concluida. Arquivos em %s", OUT)
