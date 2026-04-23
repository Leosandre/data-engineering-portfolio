#!/usr/bin/env bash
set -euo pipefail

STACK_PREFIX="${1:-b3-pipeline}"
REGION="${2:-us-east-1}"
PROFILE="${3:-agenda-facil}"

AWS="aws --region $REGION --profile $PROFILE"

echo "=== Teardown B3 Pipeline ==="

# Esvaziar bucket S3
ACCOUNT_ID=$($AWS sts get-caller-identity --query Account --output text)
BUCKET="${STACK_PREFIX}-data-${ACCOUNT_ID}"
echo ">>> Esvaziando bucket $BUCKET..."
$AWS s3 rb "s3://${BUCKET}" --force 2>/dev/null || true

# Deletar stacks em ordem reversa
for STACK in "${STACK_PREFIX}-compute" "${STACK_PREFIX}-storage" "${STACK_PREFIX}-network"; do
  echo ">>> Deletando stack: $STACK"
  $AWS cloudformation delete-stack --stack-name "$STACK" 2>/dev/null || true
  $AWS cloudformation wait stack-delete-complete --stack-name "$STACK" 2>/dev/null || true
  echo "    $STACK deletada"
done

# Deletar key pair
$AWS ec2 delete-key-pair --key-name "${STACK_PREFIX}-key" 2>/dev/null || true
rm -f "${STACK_PREFIX}-key.pem"

echo "=== Teardown completo ==="
