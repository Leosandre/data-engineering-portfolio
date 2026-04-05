"""
Reconciliacao cross-fonte: unifica clientes de 4 fontes em golden records.

Etapas:
  1. Limpeza por fonte (schema unification)
  2. Matching de clientes (CPF exato > email exato > nome fuzzy)
  3. Golden record com regras de prioridade por campo
  4. Reconciliacao de vendas (ERP vs Gateway)
  5. Linhagem campo a campo
"""

import json
import logging
import re
from pathlib import Path

import polars as pl
from thefuzz import fuzz

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE = Path(__file__).resolve().parent.parent
SYN = BASE / "data" / "synthetic"
OUT = BASE / "data" / "reconciled"


def _normalize_cpf(cpf):
    if not cpf:
        return None
    return re.sub(r"\D", "", str(cpf))


def _normalize_email(email):
    if not email:
        return None
    return str(email).strip().lower()


def _normalize_name(name):
    if not name:
        return None
    return " ".join(str(name).strip().upper().split())


# --- Etapa 1: limpeza por fonte ---

def clean_erp():
    log.info("Limpando ERP...")
    df = pl.read_parquet(SYN / "erp_customers.parquet")
    df = df.with_columns([
        pl.col("cpf").map_elements(_normalize_cpf, return_dtype=pl.Utf8).alias("cpf_norm"),
        pl.col("email").map_elements(_normalize_email, return_dtype=pl.Utf8).alias("email_norm"),
        pl.col("name").map_elements(_normalize_name, return_dtype=pl.Utf8).alias("name_norm"),
    ])
    return df


def clean_crm():
    log.info("Limpando CRM (flatten JSON)...")
    with open(SYN / "crm_contacts.json") as f:
        raw = json.load(f)

    rows = []
    for c in raw:
        addr = c.get("address") or {}
        name = f"{c.get('first_name', '')} {c.get('last_name', '')}".strip()
        rows.append({
            "contact_id": c["contact_id"],
            "erp_customer_id": c.get("_erp_customer_id"),
            "name": name,
            "email": c.get("email"),
            "cpf": c.get("cpf"),
            "phone": c.get("phone"),
            "city": addr.get("city"),
            "state": addr.get("state"),
            "segment": c.get("segment"),
            "tags": ",".join(c.get("tags", [])),
            "n_interactions": len(c.get("interactions", [])),
            "created_at": c.get("created_at"),
        })

    df = pl.DataFrame(rows)
    df = df.with_columns([
        pl.col("email").map_elements(_normalize_email, return_dtype=pl.Utf8).alias("email_norm"),
        pl.col("name").map_elements(_normalize_name, return_dtype=pl.Utf8).alias("name_norm"),
        pl.col("cpf").map_elements(_normalize_cpf, return_dtype=pl.Utf8).alias("cpf_norm"),
    ])
    return df


def clean_gateway():
    log.info("Limpando Gateway (CSV pipe-delimited)...")
    text = (SYN / "gateway_transactions.csv").read_text()
    df = pl.read_csv(text.encode(), separator="|", infer_schema_length=0)
    df = df.with_columns([
        pl.col("amount").str.replace(",", ".").cast(pl.Float64).alias("amount_brl"),
        pl.col("email").map_elements(_normalize_email, return_dtype=pl.Utf8).alias("email_norm"),
        # converter data americana MM/DD/YYYY pra datetime
        pl.col("transaction_date").str.to_datetime("%m/%d/%Y %H:%M", strict=False).alias("transaction_dt"),
    ])
    return df


# --- Etapa 2: matching de clientes ---

def match_customers(erp, crm):
    log.info("Matching de clientes (ERP x CRM)...")
    log.info("  Fase 1: CPF normalizado (match principal)")
    log.info("  Fase 2: Email exato (enriquecimento)")
    log.info("  Fase 3: Nome fuzzy (ultimo recurso)")

    erp_dict = {}
    for row in erp.iter_rows(named=True):
        erp_dict[row["customer_id"]] = row

    # indices
    cpf_index = {}
    email_index = {}
    for cid, row in erp_dict.items():
        if row["cpf_norm"] and len(row["cpf_norm"]) == 11:
            cpf_index[row["cpf_norm"]] = cid
        if row["email_norm"]:
            email_index[row["email_norm"]] = cid

    matches = {}  # crm_contact_id -> {erp_customer_id, match_types[]}
    already_matched_erp = set()

    # --- Fase 1: CPF ---
    cpf_matches = 0
    for row in crm.iter_rows(named=True):
        crm_cpf = _normalize_cpf(row.get("cpf"))
        if crm_cpf and len(crm_cpf) == 11 and crm_cpf in cpf_index:
            erp_id = cpf_index[crm_cpf]
            matches[row["contact_id"]] = {"erp_customer_id": erp_id, "match_types": ["cpf"]}
            already_matched_erp.add(erp_id)
            cpf_matches += 1
    log.info("    CPF: %d matches", cpf_matches)

    # --- Fase 2: Email (enriquece matches existentes + encontra novos) ---
    email_enriched = 0
    email_new = 0
    for row in crm.iter_rows(named=True):
        if not row["email_norm"] or row["email_norm"] not in email_index:
            continue
        erp_id = email_index[row["email_norm"]]

        if row["contact_id"] in matches:
            # ja tem match por CPF — email confirma (enriquece)
            if erp_id == matches[row["contact_id"]]["erp_customer_id"]:
                matches[row["contact_id"]]["match_types"].append("email_confirmado")
                email_enriched += 1
        else:
            # novo match so por email
            if erp_id not in already_matched_erp:
                matches[row["contact_id"]] = {"erp_customer_id": erp_id, "match_types": ["email"]}
                already_matched_erp.add(erp_id)
                email_new += 1
    log.info("    Email: %d confirmaram CPF, %d novos matches", email_enriched, email_new)

    # --- Fase 3: Nome fuzzy ---
    fuzzy_matches = 0
    for row in crm.iter_rows(named=True):
        if row["contact_id"] in matches or not row["name_norm"]:
            continue
        best_score = 0
        best_id = None
        for cid, erp_row in erp_dict.items():
            if cid in already_matched_erp:
                continue
            if row.get("state") and erp_row.get("state") and row["state"] != erp_row["state"]:
                continue
            score = fuzz.token_sort_ratio(row["name_norm"], erp_row["name_norm"])
            if score > best_score and score >= 85:
                best_score = score
                best_id = cid
        if best_id:
            matches[row["contact_id"]] = {"erp_customer_id": best_id, "match_types": [f"nome_fuzzy_{best_score}"]}
            already_matched_erp.add(best_id)
            fuzzy_matches += 1
    log.info("    Fuzzy: %d matches", fuzzy_matches)

    # monta dataframe de matches
    rows = []
    for crm_id, m in matches.items():
        rows.append({
            "crm_contact_id": crm_id,
            "erp_customer_id": m["erp_customer_id"],
            "match_types": ",".join(m["match_types"]),
            "match_confidence": "high" if "cpf" in m["match_types"] else ("medium" if "email" in m["match_types"] else "low"),
        })

    unmatched = [r["contact_id"] for r in crm.iter_rows(named=True) if r["contact_id"] not in matches]
    total = len(rows)
    log.info("  Total: %d matches, %d sem match", total, len(unmatched))
    log.info("    high (CPF): %d, medium (email): %d, low (fuzzy): %d",
             sum(1 for r in rows if r["match_confidence"] == "high"),
             sum(1 for r in rows if r["match_confidence"] == "medium"),
             sum(1 for r in rows if r["match_confidence"] == "low"))

    return pl.DataFrame(rows), unmatched


def _mask_cpf_lgpd(cpf):
    """Mascara CPF pra LGPD: 123.456.789-01 -> ***456789**"""
    if not cpf:
        return None
    digits = re.sub(r"\D", "", str(cpf))
    if len(digits) != 11:
        return None
    return f"***{digits[3:9]}**"


# --- Etapa 3: golden record ---

def build_golden_record(erp, crm, matches_df):
    log.info("Construindo golden records...")

    erp_data = {r["customer_id"]: r for r in erp.iter_rows(named=True)}
    crm_data = {r["contact_id"]: r for r in crm.iter_rows(named=True)}
    match_map = {r["crm_contact_id"]: r["erp_customer_id"] for r in matches_df.iter_rows(named=True)}

    golden = []
    lineage = []
    golden_id = 0

    erp_to_golden = {}
    for cid, e in erp_data.items():
        golden_id += 1
        erp_to_golden[cid] = golden_id

        crm_row = None
        for crm_id, erp_id in match_map.items():
            if erp_id == cid:
                crm_row = crm_data.get(crm_id)
                break

        # regras de prioridade por campo
        name = e["name"]
        name_source = "erp"
        email = crm_row["email"] if crm_row and crm_row.get("email") else e["email"]
        email_source = "crm" if crm_row and crm_row.get("email") else "erp"
        phone = crm_row["phone"] if crm_row and crm_row.get("phone") else e.get("phone")
        phone_source = "crm" if crm_row and crm_row.get("phone") else "erp"
        city = e.get("city")
        state = e.get("state")
        segment = crm_row["segment"] if crm_row else None

        # CPF: normalizado (ERP eh a fonte primaria)
        cpf_raw = e.get("cpf")
        cpf_norm = _normalize_cpf(cpf_raw)

        golden.append({
            "golden_id": golden_id,
            "name": name,
            "email": email,
            "cpf": cpf_norm,  # real — Redshift Dynamic Data Masking controla quem ve
            "cpf_masked": _mask_cpf_lgpd(cpf_norm),  # pre-mascarado pra exports
            "phone": phone,
            "city": city,
            "state": state,
            "segment": segment,
            "has_erp": True,
            "has_crm": crm_row is not None,
            "erp_customer_id": cid,
        })

        for campo, valor, fonte in [
            ("name", name, name_source), ("email", email, email_source),
            ("phone", phone, phone_source), ("city", city, "erp"), ("state", state, "erp"),
            ("cpf", cpf_norm, "erp"),
        ]:
            if valor:
                lineage.append({"golden_id": golden_id, "field": campo, "value": str(valor), "source": fonte})

    # leads do CRM sem match
    for crm_id, crm_row in crm_data.items():
        if crm_id not in match_map:
            golden_id += 1
            cpf_norm = _normalize_cpf(crm_row.get("cpf"))
            golden.append({
                "golden_id": golden_id,
                "name": crm_row.get("name"),
                "email": crm_row.get("email"),
                "cpf": cpf_norm,
                "cpf_masked": _mask_cpf_lgpd(cpf_norm),
                "phone": crm_row.get("phone"),
                "city": crm_row.get("city"),
                "state": crm_row.get("state"),
                "segment": crm_row.get("segment"),
                "has_erp": False,
                "has_crm": True,
                "erp_customer_id": None,
            })

    log.info("  Golden records: %d (%d com ERP+CRM, %d so ERP, %d so CRM)",
             len(golden),
             sum(1 for g in golden if g["has_erp"] and g["has_crm"]),
             sum(1 for g in golden if g["has_erp"] and not g["has_crm"]),
             sum(1 for g in golden if not g["has_erp"] and g["has_crm"]))

    return pl.DataFrame(golden), pl.DataFrame(lineage), erp_to_golden


# --- Etapa 4: reconciliacao de vendas ---

def reconcile_sales(erp_to_golden):
    log.info("Reconciliando vendas (ERP vs Gateway)...")

    orders = pl.read_parquet(SYN / "erp_orders.parquet")
    gateway = clean_gateway()

    # join por order_id
    gw = gateway.with_columns(
        pl.col("order_reference").cast(pl.Int64).alias("order_id")
    )

    reconciled = orders.join(gw.select("order_id", "amount_brl", "status", "payment_method", "transaction_dt"),
                             on="order_id", how="left", suffix="_gw")

    reconciled = reconciled.with_columns([
        # flag de divergencia
        pl.when(pl.col("amount_brl").is_null())
            .then(pl.lit("so_erp"))
        .when((pl.col("total_amount") - pl.col("amount_brl")).abs() < 0.05)
            .then(pl.lit("match_exato"))
        .otherwise(pl.lit("divergente"))
        .alias("reconciliation_status"),

        pl.col("customer_id").replace(erp_to_golden).alias("golden_id"),
    ])

    log.info("  Vendas reconciliadas:")
    for row in reconciled.group_by("reconciliation_status").agg(pl.len().alias("qtd")).sort("qtd", descending=True).iter_rows(named=True):
        log.info("    %s: %d", row["reconciliation_status"], row["qtd"])

    return reconciled


# --- Main ---

if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)

    erp = clean_erp()
    crm = clean_crm()

    matches_df, unmatched = match_customers(erp, crm)
    matches_df.write_parquet(OUT / "customer_matches.parquet")

    golden, lineage, erp_to_golden = build_golden_record(erp, crm, matches_df)
    golden.write_parquet(OUT / "golden_customers.parquet")
    lineage.write_parquet(OUT / "field_lineage.parquet")

    sales = reconcile_sales(erp_to_golden)
    sales.write_parquet(OUT / "reconciled_sales.parquet")

    log.info("Reconciliacao concluida. Arquivos em %s", OUT)
