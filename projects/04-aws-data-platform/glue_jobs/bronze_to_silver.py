"""
Glue Job: Bronze -> Silver (limpeza por fonte)
Le dados brutos do S3 bronze/, limpa e padroniza, grava no S3 silver/.

Cada fonte tem seu bloco de limpeza porque os problemas sao diferentes:
- ERP: nomes UPPERCASE, CPF em 10+ formatos
- CRM: JSON aninhado, campos opcionais
- Gateway: CSV pipe-delimited, virgula decimal, datas americanas
"""

import sys
import re
from awsglue.transforms import *
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
BRONZE = f"s3://{BUCKET}/bronze"
SILVER = f"s3://{BUCKET}/silver"


@F.udf(StringType())
def normalize_cpf(cpf):
    if not cpf:
        return None
    digits = re.sub(r"\D", "", str(cpf))
    # zeros cortados na frente
    digits = digits.zfill(11)
    if len(digits) != 11:
        return None
    return digits


@F.udf(StringType())
def normalize_email(email):
    if not email:
        return None
    return str(email).strip().lower()


# --- ERP ---
def clean_erp():
    customers = spark.read.parquet(f"{BRONZE}/erp/erp_customers.parquet")
    customers = customers.withColumn("cpf_norm", normalize_cpf("cpf")) \
                         .withColumn("email_norm", normalize_email("email")) \
                         .withColumn("name_norm", F.upper(F.trim("name")))
    customers.write.mode("overwrite").parquet(f"{SILVER}/erp_customers/")

    orders = spark.read.parquet(f"{BRONZE}/erp/erp_orders.parquet")
    orders.write.mode("overwrite").parquet(f"{SILVER}/erp_orders/")

    items = spark.read.parquet(f"{BRONZE}/erp/erp_order_items.parquet")
    items.write.mode("overwrite").parquet(f"{SILVER}/erp_order_items/")

    products = spark.read.parquet(f"{BRONZE}/erp/erp_products.parquet")
    products.write.mode("overwrite").parquet(f"{SILVER}/erp_products/")


# --- CRM (flatten JSON) ---
def clean_crm():
    raw = spark.read.json(f"{BRONZE}/crm/crm_contacts.json")

    crm = raw.select(
        F.col("contact_id"),
        F.col("_erp_customer_id").alias("erp_customer_id"),
        F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("name"),
        F.col("email"),
        F.col("cpf"),
        F.col("phone"),
        F.col("address.city").alias("city"),
        F.col("address.state").alias("state"),
        F.col("segment"),
        F.size("interactions").alias("n_interactions"),
        F.col("created_at"),
    )
    crm = crm.withColumn("cpf_norm", normalize_cpf("cpf")) \
              .withColumn("email_norm", normalize_email("email")) \
              .withColumn("name_norm", F.upper(F.trim("name")))
    crm.write.mode("overwrite").parquet(f"{SILVER}/crm_contacts/")


# --- Gateway ---
def clean_gateway():
    raw = spark.read.option("delimiter", "|").option("header", True).csv(f"{BRONZE}/gateway/gateway_transactions.csv")

    gw = raw.withColumn("amount_brl", F.regexp_replace("amount", ",", ".").cast("double")) \
            .withColumn("email_norm", normalize_email("email")) \
            .withColumn("cpf_norm", normalize_cpf("cpf")) \
            .withColumn("transaction_dt", F.to_timestamp("transaction_date", "MM/dd/yyyy HH:mm"))
    gw.write.mode("overwrite").parquet(f"{SILVER}/gateway_transactions/")


clean_erp()
clean_crm()
clean_gateway()
