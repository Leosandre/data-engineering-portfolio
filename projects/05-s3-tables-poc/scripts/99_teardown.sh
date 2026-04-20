#!/usr/bin/env bash
# ============================================================
# Teardown — destrói toda a infraestrutura da POC
# ============================================================
set -euo pipefail

REGION="${1:-us-east-1}"
PREFIX="${2:-s3tables-poc}"
PROFILE="${3:-default}"
AWS="aws --profile $PROFILE --region $REGION"

STACKS=(
    "04-glue-jobs"
    "03-glue-catalog"
    "02-iam"
    "01-storage"
)

echo "🗑️  Teardown — Região: ${REGION} | Profile: ${PROFILE}"
echo "================================================"

# Esvaziar buckets antes de deletar stacks
ACCOUNT_ID=$($AWS sts get-caller-identity --query Account --output text)
BUCKETS=(
    "${PREFIX}-raw-${ACCOUNT_ID}"
    "${PREFIX}-selfmanaged-${ACCOUNT_ID}"
    "${PREFIX}-athena-results-${ACCOUNT_ID}"
    "${PREFIX}-glue-scripts-${ACCOUNT_ID}"
)

for BUCKET in "${BUCKETS[@]}"; do
    echo "  🪣 Esvaziando ${BUCKET}..."
    $AWS s3 rm "s3://${BUCKET}" --recursive 2>/dev/null || true
done

# Deletar stacks (ordem reversa)
for STACK_FILE in "${STACKS[@]}"; do
    STACK_NAME="${PREFIX}-${STACK_FILE}"
    echo ""
    echo "🗑️  Deletando: ${STACK_NAME}"
    $AWS cloudformation delete-stack --stack-name "$STACK_NAME"
    $AWS cloudformation wait stack-delete-complete --stack-name "$STACK_NAME" 2>/dev/null || true
    echo "  ✅ ${STACK_NAME} deletado"
done

# Remover catálogo federado
echo ""
echo "🔗 Removendo catálogo s3tablescatalog..."
$AWS glue delete-catalog --catalog-id s3tablescatalog 2>/dev/null || true
echo "  ✅ Catálogo removido"

echo ""
echo "✅ Teardown completo!"
