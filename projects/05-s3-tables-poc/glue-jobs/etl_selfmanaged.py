"""
Pipeline B — ETL Iceberg Self-Managed
Escreve Iceberg no S3 standard via GlueCatalog.
Schema inferido do DataFrame. Database name sem hífens.
Requer Glue 5.0+.
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month

args = getResolvedOptions(sys.argv, ["RAW_BUCKET", "SELFMANAGED_BUCKET", "DATABASE_NAME", "INPUT_FILE", "BATCH_MONTH"])

DB = args["DATABASE_NAME"]
WAREHOUSE = f"s3://{args['SELFMANAGED_BUCKET']}/iceberg/"
TABLE_SQL = f"glue_catalog.{DB}.nyc_taxi"

spark = (
    SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", WAREHOUSE)
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .getOrCreate()
)

raw_path = f"s3://{args['RAW_BUCKET']}/nyc-taxi/{args['INPUT_FILE']}"

df = (
    spark.read.parquet(raw_path)
    .withColumn("year", year("tpep_pickup_datetime"))
    .withColumn("month", month("tpep_pickup_datetime"))
)

df.createOrReplaceTempView("source_data")

# Verificar se tabela existe
table_exists = False
try:
    spark.sql(f"DESCRIBE TABLE {TABLE_SQL}")
    table_exists = True
except Exception:
    pass

if not table_exists:
    cols = ", ".join([f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields])
    spark.sql(f"""
        CREATE TABLE {TABLE_SQL} ({cols})
        USING iceberg
        PARTITIONED BY (year, month)
        LOCATION '{WAREHOUSE}{DB}/nyc_taxi'
    """)

spark.sql(f"INSERT INTO {TABLE_SQL} SELECT * FROM source_data")

count = spark.sql(f"SELECT COUNT(*) FROM {TABLE_SQL}").collect()[0][0]
batch_count = df.count()
print(f"✅ Self-managed — batch {args['BATCH_MONTH']}: {batch_count} escritos | Total: {count}")

spark.stop()
