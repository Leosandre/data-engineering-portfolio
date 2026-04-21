#!/usr/bin/env bash
# Deploy das stacks CloudFormation + codigo Lambda.
# Uso: bash scripts/deploy.sh <profile> <region>

set -euo pipefail

PROFILE="${1:-agenda-facil}"
REGION="${2:-us-east-1}"
PROJECT="fraud-detection-poc"

deploy_stack() {
    local stack_name="$1"
    local template="$2"

    echo ">>> Deploying ${stack_name}..."
    aws cloudformation deploy \
        --stack-name "${stack_name}" \
        --template-file "${template}" \
        --parameter-overrides ProjectName="${PROJECT}" \
        --capabilities CAPABILITY_NAMED_IAM \
        --tags Project="${PROJECT}" Environment=poc ManagedBy=cloudformation \
        --profile "${PROFILE}" \
        --region "${REGION}" \
        --no-fail-on-empty-changeset

    echo "    OK: ${stack_name}"
}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="${SCRIPT_DIR}/.."
CFN_DIR="${PROJECT_DIR}/cloudformation"

# 1. Storage (Kinesis, DynamoDB profiles, S3, Firehose)
deploy_stack "${PROJECT}-storage"   "${CFN_DIR}/01-storage.yaml"

# 2. Messaging (SNS, SQS)
deploy_stack "${PROJECT}-messaging" "${CFN_DIR}/02-messaging.yaml"

# 3. Compute (Lambda, DynamoDB state, IAM, Event Source Mapping)
deploy_stack "${PROJECT}-compute"   "${CFN_DIR}/03-compute.yaml"

# 4. Deploy codigo real da Lambda (substitui o placeholder do CloudFormation)
echo ">>> Deploying Lambda code..."
LAMBDA_DIR="${PROJECT_DIR}/lambda"
ZIP_FILE="/tmp/${PROJECT}-lambda.zip"

cd "${LAMBDA_DIR}"
zip -j "${ZIP_FILE}" fraud_detector.py > /dev/null
cd "${PROJECT_DIR}"

aws lambda update-function-code \
    --function-name "${PROJECT}-detector" \
    --zip-file "fileb://${ZIP_FILE}" \
    --profile "${PROFILE}" \
    --region "${REGION}" \
    --no-cli-pager > /dev/null

echo "    OK: Lambda code deployed"

# 5. Ativar cost allocation tag
echo ">>> Ativando cost allocation tag..."
aws ce update-cost-allocation-tags-status \
    --cost-allocation-tags-status "TagKey=Project,Status=Active" \
    --profile "${PROFILE}" \
    --region us-east-1 2>/dev/null || echo "    (tag ja ativa ou sem permissao — ok)"

echo ""
echo "=== Infra provisionada ==="
echo "  Kinesis: ${PROJECT}-transactions (3 shards), ${PROJECT}-alerts (1 shard)"
echo "  Lambda:  ${PROJECT}-detector (Python 3.12, 256MB, batch 100)"
echo "  DynamoDB: ${PROJECT}-customer-profiles, ${PROJECT}-detection-state"
echo "  S3: ${PROJECT}-lake-*, ${PROJECT}-athena-*"
echo "  Firehose: ${PROJECT}-processed-to-s3"
echo "  SNS: ${PROJECT}-fraud-alerts → SQS: ${PROJECT}-fraud-alerts-queue"
echo ""
echo "  Tag: Project=${PROJECT} (ativa no Cost Explorer)"
echo ""
echo "IMPORTANTE: a infra esta custando dinheiro!"
echo "  Fluxo: make seed → make produce → make extract → make teardown"
echo "  Janela maxima: 30 minutos de producao (2.000 TPS)"
