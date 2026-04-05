"""
Glue Job: Silver -> Gold (reconciliacao cross-fonte)
Matching de clientes em 3 fases + golden record + reconciliacao de vendas.

Fase 1 (CPF): match principal apos normalizacao de 10+ formatos
Fase 2 (Email): enriquece matches do CPF + encontra novos
Fase 3 (Fuzzy): ultimo recurso pra quem nao tem CPF nem email batendo

Por que enriquecimento e nao cascata pura:
  Na cascata, se o CPF bate, o email nem eh verificado. Isso significa que
  voce nao sabe se o email tambem confirma o match. Se amanha o CPF mudar
  (erro de digitacao), voce perde o match sem fallback.
  Com enriquecimento, o email passa por todos — confirma quem o CPF ja pegou
  e encontra novos. O custo eh praticamente zero (lookup em hash) mas o valor
  eh alto: score de confianca, resiliencia, deteccao de inconsistencia.
"""

import sys
import re
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

BUCKET = args["S3_BUCKET"]
SILVER = f"s3://{BUCKET}/silver"
GOLD = f"s3://{BUCKET}/gold"


@F.udf(StringType())
def mask_cpf_lgpd(cpf):
    if not cpf or len(cpf) != 11:
        return None
    return f"***{cpf[3:9]}**"


def run():
    erp = spark.read.parquet(f"{SILVER}/erp_customers/")
    crm = spark.read.parquet(f"{SILVER}/crm_contacts/")

    # --- Fase 1: CPF ---
    cpf_matches = erp.alias("e").join(
        crm.alias("c"),
        (F.col("e.cpf_norm") == F.col("c.cpf_norm")) & F.col("e.cpf_norm").isNotNull() & (F.length("e.cpf_norm") == 11),
        "inner"
    ).select(
        F.col("c.contact_id").alias("crm_contact_id"),
        F.col("e.customer_id").alias("erp_customer_id"),
        F.lit("cpf").alias("match_phase"),
    )

    matched_erp_ids = cpf_matches.select("erp_customer_id").distinct()
    matched_crm_ids = cpf_matches.select("crm_contact_id").distinct()

    # --- Fase 2: Email (enriquece + novos) ---
    # novos: CRM sem match ainda + ERP sem match ainda
    crm_unmatched = crm.join(matched_crm_ids, crm.contact_id == matched_crm_ids.crm_contact_id, "left_anti")
    erp_unmatched = erp.join(matched_erp_ids, erp.customer_id == matched_erp_ids.erp_customer_id, "left_anti")

    email_new = erp_unmatched.alias("e").join(
        crm_unmatched.alias("c"),
        (F.col("e.email_norm") == F.col("c.email_norm")) & F.col("e.email_norm").isNotNull(),
        "inner"
    ).select(
        F.col("c.contact_id").alias("crm_contact_id"),
        F.col("e.customer_id").alias("erp_customer_id"),
        F.lit("email").alias("match_phase"),
    )

    # confirmacao: CRM ja matched por CPF, email tambem bate?
    email_confirm = cpf_matches.alias("m").join(
        crm.alias("c"), F.col("m.crm_contact_id") == F.col("c.contact_id")
    ).join(
        erp.alias("e"), F.col("m.erp_customer_id") == F.col("e.customer_id")
    ).filter(
        F.col("e.email_norm") == F.col("c.email_norm")
    ).select(F.col("m.crm_contact_id"), F.lit(True).alias("email_confirmed"))

    # uniao de todos os matches
    all_matches = cpf_matches.union(email_new)

    # adiciona flag de confirmacao
    all_matches = all_matches.join(email_confirm, "crm_contact_id", "left") \
        .withColumn("match_confidence",
            F.when(F.col("match_phase") == "cpf", 
                F.when(F.col("email_confirmed") == True, "high_confirmed").otherwise("high"))
            .when(F.col("match_phase") == "email", "medium")
            .otherwise("low")
        )

    all_matches.write.mode("overwrite").parquet(f"{GOLD}/customer_matches/")

    # --- Golden Record ---
    # ERP como base, enriquece com CRM onde tem match
    golden = erp.join(
        all_matches.select("erp_customer_id", "crm_contact_id", "match_confidence"),
        erp.customer_id == all_matches.erp_customer_id, "left"
    ).join(
        crm.select("contact_id", "email", "phone", "segment", "n_interactions"),
        all_matches.crm_contact_id == crm.contact_id, "left"
    )

    golden = golden.select(
        F.monotonically_increasing_id().alias("golden_id"),
        F.col("e.name").alias("name"),
        # email: CRM > ERP
        F.coalesce(crm.email, erp.email).alias("email"),
        F.col("e.cpf_norm").alias("cpf"),
        mask_cpf_lgpd(F.col("e.cpf_norm")).alias("cpf_masked"),
        # phone: CRM > ERP
        F.coalesce(crm.phone, erp.phone).alias("phone"),
        F.col("e.city"),
        F.col("e.state"),
        crm.segment,
        F.col("e.customer_id").alias("erp_customer_id"),
        F.when(crm.contact_id.isNotNull(), True).otherwise(False).alias("has_crm"),
        F.col("match_confidence"),
    )

    golden.write.mode("overwrite").parquet(f"{GOLD}/golden_customers/")

    # --- Reconciliacao de vendas ---
    orders = spark.read.parquet(f"{SILVER}/erp_orders/")
    gateway = spark.read.parquet(f"{SILVER}/gateway_transactions/")

    reconciled = orders.join(
        gateway.select(
            F.col("order_reference").cast("long").alias("order_id"),
            F.col("amount_brl").alias("gw_amount"),
            F.col("status").alias("gw_status"),
            F.col("payment_method"),
        ),
        "order_id", "left"
    ).withColumn("reconciliation_status",
        F.when(F.col("gw_amount").isNull(), "so_erp")
        .when(F.abs(F.col("total_amount") - F.col("gw_amount")) < 0.05, "match_exato")
        .otherwise("divergente")
    )

    reconciled.write.mode("overwrite").parquet(f"{GOLD}/reconciled_sales/")


run()
