"""
Métricas de Manutenção — conta arquivos via S3/S3Tables API, 
coleta maintenance status, e executa compaction manual no self-managed.
"""
import argparse
import json
import time
import boto3


def count_s3_objects(s3_client, bucket, prefix):
    """Conta objetos e coleta estatísticas de tamanho via S3 API."""
    paginator = s3_client.get_paginator("list_objects_v2")
    total_files = 0
    total_bytes = 0
    sizes = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                total_files += 1
                total_bytes += obj["Size"]
                sizes.append(obj["Size"])
    return {
        "total_files": total_files,
        "total_bytes": total_bytes,
        "avg_file_size": round(total_bytes / total_files) if total_files > 0 else 0,
        "min_file_size": min(sizes) if sizes else 0,
        "max_file_size": max(sizes) if sizes else 0,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--profile", default="default")
    args = parser.parse_args()

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    s3 = session.client("s3")
    s3tables = session.client("s3tables")
    glue = session.client("glue")
    cfn = session.client("cloudformation")
    metrics = {}

    selfmanaged_bucket = cfn.describe_stacks(StackName="s3tables-poc-01-storage")["Stacks"][0]
    selfmanaged_bucket = next(o["OutputValue"] for o in selfmanaged_bucket["Outputs"] if o["OutputKey"] == "SelfManagedBucketName")

    print("🔧 Coletando métricas de manutenção...\n")

    # === S3 Tables — maintenance status ===
    print("🅰️  S3 Tables (managed)")
    print("  🔄 Status de maintenance jobs...")
    try:
        buckets = s3tables.list_table_buckets()
        for bucket in buckets.get("tableBuckets", []):
            if "s3tables-poc" in bucket.get("name", ""):
                arn = bucket["arn"]
                tables = s3tables.list_tables(tableBucketARN=arn, namespace="poc")
                for table in tables.get("tables", []):
                    job_status = s3tables.get_table_maintenance_job_status(
                        tableBucketARN=arn, namespace="poc", name=table["name"])
                    maint = {}
                    for job_type, status in job_status.get("status", {}).items():
                        maint[job_type] = {
                            "status": status.get("status", "N/A"),
                            "lastRunTimestamp": str(status.get("lastRunTimestamp", "")),
                        }
                        print(f"     {job_type}: {status.get('status', 'N/A')}")
                    metrics["managed_maintenance"] = maint

                    # Maintenance config
                    maint_config = s3tables.get_table_maintenance_configuration(
                        tableBucketARN=arn, namespace="poc", tableName=table["name"])
                    metrics["managed_maintenance_config"] = str(maint_config.get("configuration", {}))
    except Exception as e:
        print(f"  ⚠️  {e}")
        metrics["managed_maintenance"] = {"error": str(e)}

    # === Self-managed — layout de arquivos ANTES da compaction ===
    print(f"\n🅱️  Self-managed (bucket: {selfmanaged_bucket})")
    print("  📋 Layout de arquivos (ANTES da compaction)...")
    files_before = count_s3_objects(s3, selfmanaged_bucket, "iceberg/s3tablespoc_selfmanaged/nyc_taxi/data/")
    metrics["selfmanaged_files_before"] = files_before
    print(f"     Arquivos: {files_before['total_files']}")
    print(f"     Total: {files_before['total_bytes'] / 1024**2:.1f} MB")
    print(f"     Média: {files_before['avg_file_size'] / 1024**2:.1f} MB")
    print(f"     Min: {files_before['min_file_size'] / 1024**2:.1f} MB | Max: {files_before['max_file_size'] / 1024**2:.1f} MB")

    # === Compaction manual ===
    print("\n  🔄 Executando compaction manual (Glue Job)...")
    try:
        run = glue.start_job_run(JobName="s3tables-poc-compaction-selfmanaged")
        run_id = run["JobRunId"]
        print(f"     Job Run ID: {run_id}")

        while True:
            status = glue.get_job_run(JobName="s3tables-poc-compaction-selfmanaged", RunId=run_id)
            state = status["JobRun"]["JobRunState"]
            if state in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"):
                break
            time.sleep(15)

        metrics["compaction_job"] = {
            "status": state,
            "execution_time_sec": status["JobRun"].get("ExecutionTime", 0),
            "dpu_seconds": status["JobRun"].get("DPUSeconds", 0),
            "error": status["JobRun"].get("ErrorMessage"),
        }
        print(f"     Status: {state}")
        print(f"     Tempo: {status['JobRun'].get('ExecutionTime', 0)}s")
        if state == "FAILED":
            print(f"     ❌ {status['JobRun'].get('ErrorMessage', '')[:200]}")
    except Exception as e:
        print(f"  ⚠️  {e}")
        metrics["compaction_job"] = {"error": str(e)}

    # === Layout DEPOIS da compaction ===
    print("\n  📋 Layout de arquivos (DEPOIS da compaction)...")
    files_after = count_s3_objects(s3, selfmanaged_bucket, "iceberg/s3tablespoc_selfmanaged/nyc_taxi/data/")
    metrics["selfmanaged_files_after"] = files_after
    print(f"     Arquivos: {files_after['total_files']}")
    print(f"     Total: {files_after['total_bytes'] / 1024**2:.1f} MB")
    print(f"     Média: {files_after['avg_file_size'] / 1024**2:.1f} MB")
    print(f"     Min: {files_after['min_file_size'] / 1024**2:.1f} MB | Max: {files_after['max_file_size'] / 1024**2:.1f} MB")

    # === Comparação ===
    if files_before["total_files"] > 0 and files_after["total_files"] > 0:
        reduction = ((files_before["total_files"] - files_after["total_files"]) / files_before["total_files"]) * 100
        metrics["file_reduction_pct"] = round(reduction, 1)
        print(f"\n  📊 Redução de arquivos: {files_before['total_files']} → {files_after['total_files']} ({reduction:.1f}%)")

    # === Storage total ===
    print("\n📦 Storage total por pipeline...")
    sm_total = count_s3_objects(s3, selfmanaged_bucket, "iceberg/")
    metrics["selfmanaged_storage_total"] = sm_total
    print(f"  Self-managed: {sm_total['total_bytes'] / 1024**2:.1f} MB ({sm_total['total_files']} arquivos)")

    output_path = "metrics/maintenance.json"
    with open(output_path, "w") as f:
        json.dump(metrics, f, indent=2, default=str)
    print(f"\n💾 Métricas salvas em {output_path}")


if __name__ == "__main__":
    main()
