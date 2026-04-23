#!/usr/bin/env bash
set -euo pipefail

STACK_PREFIX="${1:-b3-pipeline}"
REGION="${2:-us-east-1}"
PROFILE="${3:-agenda-facil}"

AWS="aws --region $REGION --profile $PROFILE"

echo "=== Extraindo evidências ==="

# 1. Gerar dashboard com gráficos
echo ">>> Gerando dashboard financeiro..."
python3 src/generate_dashboard.py \
  --workgroup "${STACK_PREFIX}-wg" \
  --output docs/

# 2. Métricas do Redshift
echo ">>> Queries de validação no Redshift..."
WORKGROUP="${STACK_PREFIX}-wg"

$AWS redshift-data execute-statement \
  --workgroup-name "$WORKGROUP" --database dev \
  --sql "SELECT asset_type, COUNT(*) as records, COUNT(DISTINCT ticker) as assets, COUNT(DISTINCT trade_date) as days FROM analytics.fct_daily_quotes GROUP BY asset_type" \
  --output text | tee docs/redshift_summary.txt

# 3. Snapshot SCD2 — mudanças capturadas
$AWS redshift-data execute-statement \
  --workgroup-name "$WORKGROUP" --database dev \
  --sql "SELECT ticker, sector, industry, dbt_valid_from, dbt_valid_to FROM snapshots.snap_companies WHERE dbt_valid_to IS NOT NULL ORDER BY ticker" \
  --output text | tee docs/scd2_changes.txt

# 4. Custo real via Cost Explorer
echo ">>> Custo real do projeto..."
$AWS ce get-cost-and-usage \
  --time-period Start=$(date -d '-1 day' +%Y-%m-%d),End=$(date +%Y-%m-%d) \
  --granularity DAILY \
  --metrics UnblendedCost \
  --filter "{\"Tags\":{\"Key\":\"Project\",\"Values\":[\"$STACK_PREFIX\"]}}" \
  --output json | tee docs/cost_report.json

echo "=== Evidências salvas em docs/ ==="
ls -la docs/
