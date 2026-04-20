"""
Schema Evolution — add column, add column 2, drop column + validação.
Usa Athena EngineExecutionTimeInMillis para tempos precisos.
"""
import argparse
import json
import time
import boto3

MANAGED_CATALOG = "s3tablescatalog/s3tables-poc-managed"
MANAGED_TABLE = "poc.nyc_taxi"
SELFMANAGED_DB = "s3tablespoc_selfmanaged"
SELFMANAGED_TABLE = "nyc_taxi"
ATHENA_WG_MANAGED = "s3tables-poc-managed"
ATHENA_WG_SELFMANAGED = "s3tables-poc-selfmanaged"


def run_athena_query(client, query, workgroup, catalog=None, database=None):
    params = {"QueryString": query, "WorkGroup": workgroup}
    ctx = {}
    if catalog:
        ctx["Catalog"] = catalog
    if database:
        ctx["Database"] = database
    if ctx:
        params["QueryExecutionContext"] = ctx

    resp = client.start_query_execution(**params)
    qid = resp["QueryExecutionId"]

    while True:
        result = client.get_query_execution(QueryExecutionId=qid)
        state = result["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    if state == "FAILED":
        return {"status": "FAILED", "error": result["QueryExecution"]["Status"].get("StateChangeReason", ""),
                "engine_ms": 0}

    stats = result["QueryExecution"].get("Statistics", {})
    return {
        "status": "SUCCEEDED",
        "engine_ms": stats.get("EngineExecutionTimeInMillis", 0),
        "query_id": qid,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--profile", default="default")
    args = parser.parse_args()

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    athena = session.client("athena")
    metrics = {"managed": {}, "selfmanaged": {}}

    operations = [
        {"name": "add_column", "desc": "ADD COLUMN (surge_multiplier DOUBLE)",
         "managed_sql": f"ALTER TABLE {MANAGED_TABLE} ADD COLUMNS (surge_multiplier DOUBLE)",
         "selfmanaged_sql": f"ALTER TABLE {SELFMANAGED_TABLE} ADD COLUMNS (surge_multiplier DOUBLE)"},
        {"name": "add_column_2", "desc": "ADD COLUMN (trip_category STRING)",
         "managed_sql": f"ALTER TABLE {MANAGED_TABLE} ADD COLUMNS (trip_category STRING)",
         "selfmanaged_sql": f"ALTER TABLE {SELFMANAGED_TABLE} ADD COLUMNS (trip_category STRING)"},
        {"name": "drop_column", "desc": "DROP COLUMN (trip_category)",
         "managed_sql": f"ALTER TABLE {MANAGED_TABLE} DROP COLUMN trip_category",
         "selfmanaged_sql": f"ALTER TABLE {SELFMANAGED_TABLE} DROP COLUMN trip_category"},
    ]

    for op in operations:
        print(f"\n🔄 {op['desc']}")
        for pipeline, sql, wg, catalog, db in [
            ("managed", op["managed_sql"], ATHENA_WG_MANAGED, MANAGED_CATALOG, "poc"),
            ("selfmanaged", op["selfmanaged_sql"], ATHENA_WG_SELFMANAGED, None, SELFMANAGED_DB),
        ]:
            icon = "🅰️" if pipeline == "managed" else "🅱️"
            result = run_athena_query(athena, sql, wg, catalog=catalog, database=db)
            metrics[pipeline][op["name"]] = result
            print(f"  {icon} {pipeline}: {result['status']} ({result['engine_ms']}ms)")
            if result["status"] == "FAILED":
                print(f"     ❌ {result.get('error', '')[:200]}")

    # Validação backward compatibility
    print("\n🔍 Validando compatibilidade retroativa...")
    for pipeline, sql, wg, catalog, db in [
        ("managed", f"SELECT VendorID, trip_distance, surge_multiplier FROM {MANAGED_TABLE} LIMIT 5",
         ATHENA_WG_MANAGED, MANAGED_CATALOG, "poc"),
        ("selfmanaged", f"SELECT VendorID, trip_distance, surge_multiplier FROM {SELFMANAGED_TABLE} LIMIT 5",
         ATHENA_WG_SELFMANAGED, None, SELFMANAGED_DB),
    ]:
        result = run_athena_query(athena, sql, wg, catalog=catalog, database=db)
        metrics[pipeline]["backward_compatibility"] = result
        icon = "✅" if result["status"] == "SUCCEEDED" else "❌"
        print(f"  {icon} {pipeline}: {result['status']} ({result['engine_ms']}ms)")

    with open("metrics/schema_evolution.json", "w") as f:
        json.dump(metrics, f, indent=2, default=str)
    print(f"\n💾 Métricas salvas em metrics/schema_evolution.json")


if __name__ == "__main__":
    main()
