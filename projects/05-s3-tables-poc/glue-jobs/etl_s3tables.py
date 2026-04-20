"""
Pipeline A — ETL S3 Tables (Iceberg managed)
Usa S3 Tables Iceberg REST endpoint (recomendado para Glue 5.0+).
Schema inferido do DataFrame, sem CREATE TABLE hardcoded.
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month

args = getResolvedOptions(sys.argv, ["RAW_BUCKET", "TABLE_BUCKET_ARN", "INPUT_FILE", "BATCH_MONTH"])

REGION = "us-east-1"
NAMESPACE = "poc"
TABLE_NAME = "nyc_taxi"
FULL_TABLE = f"s3tables.{NAMESPACE}.{TABLE_NAME}"

spark = (
    SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.s3tables", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.s3tables.type", "rest")
    .config("spark.sql.catalog.s3tables.uri", f"https://s3tables.{REGION}.amazonaws.com/iceberg")
    .config("spark.sql.catalog.s3tables.warehouse", args["TABLE_BUCKET_ARN"])
    .config("spark.sql.catalog.s3tables.rest.sigv4-enabled", "true")
    .config("spark.sql.catalog.s3tables.rest.signing-name", "s3tables")
    .config("spark.sql.catalog.s3tables.rest.signing-region", REGION)
    .config("spark.sql.catalog.s3tables.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.s3tables.rest-metrics-reporting-enabled", "false")
    .getOrCreate()
)

raw_path = f"s3://{args['RAW_BUCKET']}/nyc-taxi/{args['INPUT_FILE']}"

df = (
    spark.read.parquet(raw_path)
    .withColumn("year", year("tpep_pickup_datetime"))
    .withColumn("month", month("tpep_pickup_datetime"))
)

df.createOrReplaceTempView("source_data")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS s3tables.{NAMESPACE}")

# Verificar se tabela existe
table_exists = False
try:
    spark.sql(f"DESCRIBE TABLE {FULL_TABLE}")
    table_exists = True
except Exception:
    pass

if not table_exists:
    # REST endpoint não suporta CTAS — criar schema e depois inserir
    cols = ", ".join([f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields])
    spark.sql(f"CREATE TABLE {FULL_TABLE} ({cols}) USING iceberg PARTITIONED BY (year, month)")

spark.sql(f"INSERT INTO {FULL_TABLE} SELECT * FROM source_data")

count = spark.sql(f"SELECT COUNT(*) FROM {FULL_TABLE}").collect()[0][0]
batch_count = df.count()
print(f"✅ S3 Tables — batch {args['BATCH_MONTH']}: {batch_count} escritos | Total: {count}")

spark.stop()
