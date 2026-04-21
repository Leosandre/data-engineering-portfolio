#!/usr/bin/env bash
# Verifica que nenhum recurso da POC sobrou na conta.
# Uso: bash scripts/verify_cleanup.sh <profile> <region>

set -euo pipefail

PROFILE="${1:-agenda-facil}"
REGION="${2:-us-east-1}"
PROJECT="fraud-detection-poc"

AWS="aws --profile ${PROFILE} --region ${REGION}"
ERRORS=0

check() {
    local label="$1"
    local count="$2"
    if [ "${count}" -gt 0 ]; then
        echo "  FALHA: ${label} — ${count} recurso(s) restante(s)"
        ERRORS=$((ERRORS + 1))
    else
        echo "  OK: ${label}"
    fi
}

echo "Verificando cleanup..."

# Kinesis
count=$(${AWS} kinesis list-streams --query "StreamNames[?starts_with(@, '${PROJECT}')] | length(@)" --output text 2>/dev/null || echo 0)
check "Kinesis Streams" "${count}"

# DynamoDB
count=$(${AWS} dynamodb list-tables --query "TableNames[?starts_with(@, '${PROJECT}')] | length(@)" --output text 2>/dev/null || echo 0)
check "DynamoDB Tables" "${count}"

# S3
count=$(${AWS} s3api list-buckets --query "Buckets[?starts_with(Name, '${PROJECT}')] | length(@)" --output text 2>/dev/null || echo 0)
check "S3 Buckets" "${count}"

# CloudFormation
count=$(${AWS} cloudformation list-stacks --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE --query "StackSummaries[?starts_with(StackName, '${PROJECT}')] | length(@)" --output text 2>/dev/null || echo 0)
check "CloudFormation Stacks" "${count}"

echo ""
if [ "${ERRORS}" -gt 0 ]; then
    echo "ATENCAO: ${ERRORS} recurso(s) ainda existem!"
    exit 1
else
    echo "Tudo limpo!"
fi
