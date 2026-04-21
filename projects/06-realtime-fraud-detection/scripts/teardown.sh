#!/usr/bin/env bash
# Destroi toda a infra AWS da POC.
# Uso: bash scripts/teardown.sh <profile> <region>

set -euo pipefail

PROFILE="${1:-agenda-facil}"
REGION="${2:-us-east-1}"
PROJECT="fraud-detection-poc"

AWS="aws --profile ${PROFILE} --region ${REGION}"

echo ">>> Esvaziando S3 buckets..."
for bucket in $(${AWS} s3api list-buckets --query "Buckets[?starts_with(Name, '${PROJECT}')].Name" --output text 2>/dev/null); do
    echo "    Esvaziando ${bucket}..."
    ${AWS} s3 rm "s3://${bucket}" --recursive --quiet 2>/dev/null || true
done

echo ">>> Deletando stacks CloudFormation (ordem reversa)..."
for stack in "${PROJECT}-compute" "${PROJECT}-messaging" "${PROJECT}-storage"; do
    echo "    Deletando ${stack}..."
    ${AWS} cloudformation delete-stack --stack-name "${stack}" 2>/dev/null || true
    ${AWS} cloudformation wait stack-delete-complete --stack-name "${stack}" 2>/dev/null || true
    echo "    OK: ${stack}"
done

echo ""
echo "Teardown completo!"
