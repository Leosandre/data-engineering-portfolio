"""
Time Travel — demonstra versionamento via FOR TIMESTAMP AS OF.
Compara query atual vs query em ponto anterior no tempo.
"""
import argparse
import json
import time
from datetime import datetime, timedelta, timezone
import boto3

MANAGED_CATALOG = "s3tablescatalog/s3tables-poc-managed"
MANAGED_TABLE = "nyc_taxi"
SELFMANAGED_DB = "s3tablespoc_selfmanaged"
SELFMANAGED_TABLE = "nyc_taxi"
ATHENA_WG_MANAGED = "s3tables-poc-managed"
ATHENA_WG_SELFMANAGED = "s3tables-poc-selfmanaged"


def run_athena_query(client, query, workgroup, catalog=None, database=None, fetch_results=False):
    params = {"QueryString": query, "WorkGroup": workgroup}
    ctx = {}
    if catalog:
        ctx["Catalog"] = catalog
    if database:
        ctx["Database"] = database
    if ctx:
        params["QueryExecutionContext"] = ctx

    start = time.time()
    resp = client.start_query_execution(**params)
    qid = resp["QueryExecutionId"]

    while True:
        status = client.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    elapsed_ms = (time.time() - start) * 1000
    result = {"status": state, "elapsed_ms": round(elapsed_ms, 1), "query_id": qid}

    if state == "FAILED":
        result["error"] = status["QueryExecution"]["Status"].get("StateChangeReason", "")
        return result

    stats = status["QueryExecution"].get("Statistics", {})
    result["bytes_scanned"] = stats.get("DataScannedInBytes", 0)

    if fetch_results:
        rows = client.get_query_results(QueryExecutionId=qid)
        result["rows"] = [
            [col.get("VarCharValue", "") for col in row["Data"]]
            for row in rows["ResultSet"]["Rows"]
        ]
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--profile", default="default")
    args = parser.parse_args()

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    athena = session.client("athena")
    metrics = {"managed": {}, "selfmanaged": {}}

    # Usar timestamp entre batch 3 e batch 4 para ter dados parciais
    import os, glob
    batch_files = sorted(glob.glob("metrics/batch_*_managed.json"))
    if len(batch_files) >= 4:
        mid_batch = json.load(open(batch_files[2]))  # batch 3
        completed = mid_batch["CompletedOn"]
        from dateutil.parser import parse as parse_dt
        ts = parse_dt(completed).astimezone(timezone.utc)
        past_ts = (ts + timedelta(seconds=30)).strftime("%Y-%m-%d %H:%M:%S UTC")
    else:
        past_ts = (datetime.now(timezone.utc) - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S UTC")

    pipelines = [
        {"name": "managed", "table": MANAGED_TABLE, "wg": ATHENA_WG_MANAGED,
         "catalog": MANAGED_CATALOG, "db": "poc"},
        {"name": "selfmanaged", "table": SELFMANAGED_TABLE, "wg": ATHENA_WG_SELFMANAGED,
         "catalog": None, "db": SELFMANAGED_DB},
    ]

    for p in pipelines:
        print(f"\n⏰ Time Travel — {p['name']}")

        # 1. Query versão atual
        print("  🔍 Query versão atual...")
        current_query = f"SELECT COUNT(*) as total FROM {p['table']}"
        current_result = run_athena_query(athena, current_query, p["wg"], catalog=p["catalog"], database=p["db"], fetch_results=True)
        metrics[p["name"]]["query_current"] = current_result
        total_now = current_result.get("rows", [[], [""]])[1][0] if current_result["status"] == "SUCCEEDED" else "N/A"
        print(f"     → {current_result['elapsed_ms']}ms | {total_now} registros")

        # 2. Query em ponto anterior no tempo
        print(f"  ⏪ Query em {past_ts}...")
        tt_query = f"SELECT COUNT(*) as total FROM {p['table']} FOR TIMESTAMP AS OF TIMESTAMP '{past_ts}'"
        tt_result = run_athena_query(athena, tt_query, p["wg"], catalog=p["catalog"], database=p["db"], fetch_results=True)
        metrics[p["name"]]["query_time_travel"] = tt_result
        metrics[p["name"]]["time_travel_timestamp"] = past_ts
        if tt_result["status"] == "SUCCEEDED":
            total_past = tt_result.get("rows", [[], [""]])[1][0]
            print(f"     → {tt_result['elapsed_ms']}ms | {total_past} registros")
            print(f"     📊 Diferença: {total_now} (atual) vs {total_past} (passado)")
        else:
            print(f"     ❌ {tt_result.get('error', '')[:200]}")

    output_path = "metrics/time_travel.json"
    with open(output_path, "w") as f:
        json.dump(metrics, f, indent=2, default=str)
    print(f"\n💾 Métricas salvas em {output_path}")


if __name__ == "__main__":
    main()
