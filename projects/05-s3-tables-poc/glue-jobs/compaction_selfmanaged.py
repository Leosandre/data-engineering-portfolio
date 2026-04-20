"""
Compaction manual — Pipeline B (self-managed)
Demonstra o esforço operacional necessário sem S3 Tables.
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["SELFMANAGED_BUCKET", "DATABASE_NAME"])

DB = args["DATABASE_NAME"]

spark = (
    SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{args['SELFMANAGED_BUCKET']}/iceberg/")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .getOrCreate()
)

spark.sql(f"CALL glue_catalog.system.rewrite_data_files(table => '{DB}.nyc_taxi')")
spark.sql(f"CALL glue_catalog.system.expire_snapshots(table => '{DB}.nyc_taxi', retain_last => 5)")
spark.sql(f"CALL glue_catalog.system.remove_orphan_files(table => '{DB}.nyc_taxi')")

print("✅ Compaction manual concluída: rewrite_data_files + expire_snapshots + remove_orphan_files")
spark.stop()
