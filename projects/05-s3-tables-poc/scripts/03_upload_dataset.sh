#!/usr/bin/env bash
# ============================================================
# Upload dataset + Glue scripts para S3
# ============================================================
set -euo pipefail

REGION="${1:-us-east-1}"
PREFIX="${2:-s3tables-poc}"
PROFILE="${3:-default}"
AWS="aws --profile $PROFILE --region $REGION"

RAW_BUCKET=$($AWS cloudformation describe-stacks \
    --stack-name "${PREFIX}-01-storage" \
    --query "Stacks[0].Outputs[?OutputKey=='RawDataBucketName'].OutputValue" \
    --output text)

SCRIPTS_BUCKET=$($AWS cloudformation describe-stacks \
    --stack-name "${PREFIX}-01-storage" \
    --query "Stacks[0].Outputs[?OutputKey=='GlueScriptsBucketName'].OutputValue" \
    --output text)

echo "☁️  Upload — Raw: ${RAW_BUCKET} | Scripts: ${SCRIPTS_BUCKET}"
echo "================================================"

# Upload dataset
echo ""
echo "📤 Uploading dataset..."
$AWS s3 sync ./data/ "s3://${RAW_BUCKET}/nyc-taxi/" \
    --exclude "*" --include "*.parquet"
echo "  ✅ Dataset uploaded"

# Upload Glue scripts
echo ""
echo "📤 Uploading Glue scripts..."
$AWS s3 cp glue-jobs/etl_s3tables.py "s3://${SCRIPTS_BUCKET}/scripts/etl_s3tables.py"
$AWS s3 cp glue-jobs/etl_selfmanaged.py "s3://${SCRIPTS_BUCKET}/scripts/etl_selfmanaged.py"
$AWS s3 cp glue-jobs/compaction_selfmanaged.py "s3://${SCRIPTS_BUCKET}/scripts/compaction_selfmanaged.py"
echo "  ✅ Scripts uploaded"

echo ""
echo "✅ Upload completo!"
