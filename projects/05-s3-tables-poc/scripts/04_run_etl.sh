#!/usr/bin/env bash
# ============================================================
# Executa Glue Jobs — batches em cada pipeline
# Detecta automaticamente quantos meses foram baixados
# Roda sequencialmente: espera batch N terminar antes de iniciar N+1
# ============================================================
set -euo pipefail

REGION="${1:-us-east-1}"
PREFIX="${2:-s3tables-poc}"
PROFILE="${3:-default}"
AWS="aws --profile $PROFILE --region $REGION"
METRICS_DIR="./metrics"
mkdir -p "$METRICS_DIR"

JOB_MANAGED="${PREFIX}-etl-managed"
JOB_SELFMANAGED="${PREFIX}-etl-selfmanaged"

# Detectar meses disponíveis
MONTHS=$(ls data/yellow_tripdata_2024-*.parquet 2>/dev/null | sed 's/.*2024-\(.*\)\.parquet/\1/' | sort)
TOTAL=$(echo "$MONTHS" | wc -w)

echo "⚙️  Executando ETL — ${TOTAL} batches por pipeline"
echo "================================================"

wait_for_job() {
    local job_name="$1"
    local run_id="$2"
    local status="RUNNING"

    while [[ "$status" == "RUNNING" || "$status" == "STARTING" || "$status" == "WAITING" ]]; do
        sleep 15
        status=$($AWS glue get-job-run \
            --job-name "$job_name" --run-id "$run_id" \
            --query "JobRun.JobRunState" --output text)
    done
    echo "$status"
}

start_job_with_retry() {
    local job_name="$1"
    local args_json="$2"
    local run_id=""
    local retries=5

    for i in $(seq 1 $retries); do
        run_id=$($AWS glue start-job-run --job-name "$job_name" --arguments "$args_json" --query "JobRunId" --output text 2>/dev/null) && break
        echo "    ⏳ Slot ocupado, retry $i/$retries (30s)..." >&2
        sleep 30
    done
    echo "$run_id"
}

for MONTH in $MONTHS; do
    FILE="yellow_tripdata_2024-${MONTH}.parquet"
    echo ""
    echo "📦 Batch: 2024-${MONTH}"

    # Pipeline A — S3 Tables
    echo "  🅰️  Iniciando ${JOB_MANAGED}..."
    RUN_A=$(start_job_with_retry "$JOB_MANAGED" "{\"--INPUT_FILE\":\"${FILE}\",\"--BATCH_MONTH\":\"2024-${MONTH}\"}")

    # Pipeline B — Self-managed
    echo "  🅱️  Iniciando ${JOB_SELFMANAGED}..."
    RUN_B=$(start_job_with_retry "$JOB_SELFMANAGED" "{\"--INPUT_FILE\":\"${FILE}\",\"--BATCH_MONTH\":\"2024-${MONTH}\"}")

    # Aguardar AMBOS terminarem antes do próximo batch
    echo "  ⏳ Aguardando conclusão..."
    STATUS_A=$(wait_for_job "$JOB_MANAGED" "$RUN_A")
    STATUS_B=$(wait_for_job "$JOB_SELFMANAGED" "$RUN_B")
    echo "  🅰️  ${STATUS_A} | 🅱️  ${STATUS_B}"

    # Coletar métricas
    $AWS glue get-job-run --job-name "$JOB_MANAGED" --run-id "$RUN_A" \
        --query "JobRun.{StartedOn:StartedOn,CompletedOn:CompletedOn,ExecutionTime:ExecutionTime,DPUSeconds:DPUSeconds,MaxCapacity:MaxCapacity,Status:JobRunState,Error:ErrorMessage}" \
        --output json > "${METRICS_DIR}/batch_2024-${MONTH}_managed.json"

    $AWS glue get-job-run --job-name "$JOB_SELFMANAGED" --run-id "$RUN_B" \
        --query "JobRun.{StartedOn:StartedOn,CompletedOn:CompletedOn,ExecutionTime:ExecutionTime,DPUSeconds:DPUSeconds,MaxCapacity:MaxCapacity,Status:JobRunState,Error:ErrorMessage}" \
        --output json > "${METRICS_DIR}/batch_2024-${MONTH}_selfmanaged.json"

    # Se algum falhou, mostrar erro mas continuar
    if [[ "$STATUS_A" == "FAILED" ]]; then
        echo "  ❌ Job A erro: $(cat ${METRICS_DIR}/batch_2024-${MONTH}_managed.json | python3 -c 'import sys,json;print(json.load(sys.stdin).get("Error","")[:200])' 2>/dev/null)"
    fi
    if [[ "$STATUS_B" == "FAILED" ]]; then
        echo "  ❌ Job B erro: $(cat ${METRICS_DIR}/batch_2024-${MONTH}_selfmanaged.json | python3 -c 'import sys,json;print(json.load(sys.stdin).get("Error","")[:200])' 2>/dev/null)"
    fi

    # Aguardar Glue liberar o slot antes do próximo batch
    echo "  ⏳ Aguardando cooldown (30s)..."
    sleep 30
done

echo ""
echo "✅ ETL completo! Métricas salvas em ${METRICS_DIR}/"
