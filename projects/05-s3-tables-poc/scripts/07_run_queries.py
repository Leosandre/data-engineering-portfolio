"""
Queries Comparativas — 8 queries × 3 runs × 2 pipelines.
Inclui: full scan, partition filter, group by, range filter, count distinct,
window function, multi-aggregation, e temporal filter.
Usa Athena execution stats (não wall clock) para tempos precisos.
"""
import argparse
import json
import time
import statistics
import boto3

MANAGED_CATALOG = "s3tablescatalog/s3tables-poc-managed"
MANAGED_TABLE = "nyc_taxi"
SELFMANAGED_DB = "s3tablespoc_selfmanaged"
SELFMANAGED_TABLE = "nyc_taxi"
ATHENA_WG_MANAGED = "s3tables-poc-managed"
ATHENA_WG_SELFMANAGED = "s3tables-poc-selfmanaged"

COST_PER_TB = 5.0
RUNS_PER_QUERY = 3

QUERIES = [
    {"name": "full_scan_agg",
     "label": "Full Scan + Agregação",
     "sql": "SELECT COUNT(*) as total, AVG(trip_distance) as avg_dist, SUM(total_amount) as sum_amount, AVG(tip_amount) as avg_tip FROM {table}"},
    {"name": "partition_filter",
     "label": "Filtro por Partição",
     "sql": "SELECT COUNT(*) as total, AVG(fare_amount) as avg_fare FROM {table} WHERE year=2024 AND month=6"},
    {"name": "group_by",
     "label": "GROUP BY + Agregação",
     "sql": "SELECT payment_type, COUNT(*) as total, AVG(total_amount) as avg_amount, SUM(tip_amount) as sum_tip FROM {table} GROUP BY payment_type ORDER BY total DESC"},
    {"name": "range_filter",
     "label": "Filtro por Range",
     "sql": "SELECT COUNT(*) as total, AVG(tip_amount) as avg_tip FROM {table} WHERE trip_distance > 10 AND total_amount > 50"},
    {"name": "count_distinct",
     "label": "COUNT DISTINCT",
     "sql": "SELECT COUNT(DISTINCT PULocationID) as unique_pickups, COUNT(DISTINCT DOLocationID) as unique_dropoffs FROM {table}"},
    {"name": "multi_agg_groupby",
     "label": "Multi-Agregação + GROUP BY Mês",
     "sql": "SELECT month, COUNT(*) as trips, AVG(trip_distance) as avg_dist, AVG(total_amount) as avg_amount, SUM(total_amount) as revenue, AVG(tip_amount/NULLIF(fare_amount,0)) as avg_tip_pct FROM {table} WHERE year=2024 GROUP BY month ORDER BY month"},
    {"name": "top_n_subquery",
     "label": "Top N com Subquery",
     "sql": "SELECT PULocationID, trip_count, avg_fare FROM (SELECT PULocationID, COUNT(*) as trip_count, AVG(fare_amount) as avg_fare FROM {table} WHERE year=2024 GROUP BY PULocationID) WHERE trip_count > 10000 ORDER BY trip_count DESC LIMIT 20"},
    {"name": "window_function",
     "label": "Window Function (Ranking)",
     "sql": "SELECT month, payment_type, total, RANK() OVER (PARTITION BY month ORDER BY total DESC) as rnk FROM (SELECT month, payment_type, COUNT(*) as total FROM {table} WHERE year=2024 GROUP BY month, payment_type) ORDER BY month, rnk LIMIT 50"},
]


def run_athena_query(client, query, workgroup, catalog=None, database=None):
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

    wall_clock_ms = (time.time() - start) * 1000

    if state == "FAILED":
        return {"status": "FAILED", "error": status["QueryExecution"]["Status"].get("StateChangeReason", "")}

    stats = status["QueryExecution"].get("Statistics", {})
    engine_ms = stats.get("EngineExecutionTimeInMillis", 0)
    bytes_scanned = stats.get("DataScannedInBytes", 0)
    planning_ms = stats.get("QueryPlanningTimeInMillis", 0)
    queue_ms = stats.get("QueryQueueTimeInMillis", 0)
    processing_ms = stats.get("ServiceProcessingTimeInMillis", 0)

    return {
        "status": "SUCCEEDED",
        "wall_clock_ms": round(wall_clock_ms, 1),
        "engine_ms": engine_ms,
        "planning_ms": planning_ms,
        "queue_ms": queue_ms,
        "processing_ms": processing_ms,
        "bytes_scanned": bytes_scanned,
        "cost_usd": round((bytes_scanned / (1024**4)) * COST_PER_TB, 6),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--profile", default="default")
    args = parser.parse_args()

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    athena = session.client("athena")
    results = []

    pipelines = [
        {"name": "managed", "table": MANAGED_TABLE, "wg": ATHENA_WG_MANAGED,
         "catalog": MANAGED_CATALOG, "db": "poc"},
        {"name": "selfmanaged", "table": SELFMANAGED_TABLE, "wg": ATHENA_WG_SELFMANAGED,
         "catalog": None, "db": SELFMANAGED_DB},
    ]

    for q in QUERIES:
        print(f"\n🔍 {q['label']}")

        for p in pipelines:
            sql = q["sql"].format(table=p["table"])
            runs = []

            for i in range(RUNS_PER_QUERY):
                result = run_athena_query(athena, sql, p["wg"], catalog=p["catalog"], database=p["db"])
                if result["status"] == "SUCCEEDED":
                    runs.append(result)
                icon = "🅰️" if p["name"] == "managed" else "🅱️"
                ms = result.get("engine_ms", result.get("error", "FAILED"))
                print(f"  {icon} {p['name']} run {i+1}: {ms}ms | {result.get('bytes_scanned', 0) / 1024**2:.1f}MB scanned")

            if runs:
                entry = {
                    "query": q["name"],
                    "label": q["label"],
                    "pipeline": p["name"],
                    "runs": runs,
                    "median_engine_ms": round(statistics.median([r["engine_ms"] for r in runs]), 1),
                    "median_wall_clock_ms": round(statistics.median([r["wall_clock_ms"] for r in runs]), 1),
                    "median_bytes_scanned": statistics.median([r["bytes_scanned"] for r in runs]),
                    "median_cost_usd": round(statistics.median([r["cost_usd"] for r in runs]), 6),
                    "median_planning_ms": round(statistics.median([r["planning_ms"] for r in runs]), 1),
                }
            else:
                entry = {"query": q["name"], "label": q["label"], "pipeline": p["name"],
                         "runs": [], "median_engine_ms": None, "median_wall_clock_ms": None,
                         "median_bytes_scanned": None, "median_cost_usd": None, "median_planning_ms": None,
                         "error": runs[0].get("error") if runs else "all failed"}

            results.append(entry)

    output_path = "metrics/queries.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n💾 Métricas salvas em {output_path}")

    # Resumo
    print(f"\n{'Query':<30} {'Managed (ms)':<15} {'Self-mgd (ms)':<15} {'Bytes M':<12} {'Bytes S':<12} {'Vencedor':<12}")
    print("-" * 96)
    for q in QUERIES:
        m = next((r for r in results if r["query"] == q["name"] and r["pipeline"] == "managed"), None)
        s = next((r for r in results if r["query"] == q["name"] and r["pipeline"] == "selfmanaged"), None)
        m_ms = m["median_engine_ms"] if m else None
        s_ms = s["median_engine_ms"] if s else None
        m_bytes = f'{m["median_bytes_scanned"]/1024**2:.1f}MB' if m and m["median_bytes_scanned"] else "N/A"
        s_bytes = f'{s["median_bytes_scanned"]/1024**2:.1f}MB' if s and s["median_bytes_scanned"] else "N/A"
        if m_ms and s_ms and m_ms > 0:
            winner = "🅰️ Managed" if m_ms < s_ms else "🅱️ Self-mgd"
        else:
            winner = "N/A"
        print(f"{q['label']:<30} {str(m_ms):<15} {str(s_ms):<15} {m_bytes:<12} {s_bytes:<12} {winner:<12}")


if __name__ == "__main__":
    main()
