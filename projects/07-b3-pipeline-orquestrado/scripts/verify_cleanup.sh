#!/usr/bin/env bash
set -euo pipefail

STACK_PREFIX="${1:-b3-pipeline}"
REGION="${2:-us-east-1}"
PROFILE="${3:-agenda-facil}"

AWS="aws --region $REGION --profile $PROFILE"

echo "=== Verificando limpeza ==="

RESOURCES=$($AWS resourcegroupstaggingapi get-resources \
  --tag-filters Key=Project,Values=$STACK_PREFIX \
  --query 'ResourceTagMappingList[].ResourceARN' \
  --output text 2>/dev/null || echo "")

if [ -z "$RESOURCES" ]; then
  echo "✅ Nenhum recurso encontrado com tag Project=$STACK_PREFIX"
else
  echo "❌ Recursos ainda existem:"
  echo "$RESOURCES"
  exit 1
fi
