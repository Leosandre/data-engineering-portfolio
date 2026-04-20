#!/usr/bin/env bash
# ============================================================
# Deploy CloudFormation stacks (em ordem de dependência)
# ============================================================
set -euo pipefail

REGION="${1:-us-east-1}"
PREFIX="${2:-s3tables-poc}"
PROFILE="${3:-default}"
AWS="aws --profile $PROFILE --region $REGION"

STACKS=(
    "01-storage"
    "02-iam"
    "03-glue-catalog"
    "04-glue-jobs"
)

echo "🏗️  Deploy — Região: ${REGION} | Prefixo: ${PREFIX} | Profile: ${PROFILE}"
echo "================================================"

# Integração S3 Tables ↔ Glue Catalog (pré-requisito)
echo ""
echo "🔗 Verificando integração S3 Tables ↔ Glue Data Catalog..."
if $AWS glue get-catalog --catalog-id s3tablescatalog &>/dev/null; then
    echo "  ✅ Catálogo s3tablescatalog já existe"
else
    echo "  📝 Criando catálogo federado s3tablescatalog..."
    ACCOUNT_ID=$($AWS sts get-caller-identity --query Account --output text)

    cat > /tmp/s3tables-catalog.json <<EOF
{
    "Name": "s3tablescatalog",
    "CatalogInput": {
        "FederatedCatalog": {
            "Identifier": "arn:aws:s3tables:${REGION}:${ACCOUNT_ID}:bucket/*",
            "ConnectionName": "aws:s3tables"
        },
        "CreateDatabaseDefaultPermissions": [
            {
                "Principal": {
                    "DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"
                },
                "Permissions": ["ALL"]
            }
        ],
        "CreateTableDefaultPermissions": [
            {
                "Principal": {
                    "DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"
                },
                "Permissions": ["ALL"]
            }
        ],
        "AllowFullTableExternalDataAccess": "True"
    }
}
EOF
    $AWS glue create-catalog --cli-input-json file:///tmp/s3tables-catalog.json
    echo "  ✅ Catálogo s3tablescatalog criado"
fi

# Deploy das stacks
for STACK_FILE in "${STACKS[@]}"; do
    STACK_NAME="${PREFIX}-${STACK_FILE}"
    TEMPLATE="cloudformation/${STACK_FILE}.yaml"

    echo ""
    echo "📦 Deployando: ${STACK_NAME}"
    echo "   Template:  ${TEMPLATE}"

    $AWS cloudformation deploy \
        --stack-name "$STACK_NAME" \
        --template-file "$TEMPLATE" \
        --parameter-overrides ProjectName="$PREFIX" \
        --capabilities CAPABILITY_NAMED_IAM \
        --tags cost-center=stit-poc-s3tables project="$PREFIX" \
        --no-fail-on-empty-changeset

    echo "  ✅ ${STACK_NAME} deployado"
done

echo ""
echo "✅ Infraestrutura completa!"
echo ""
echo "📋 Outputs:"
for STACK_FILE in "${STACKS[@]}"; do
    STACK_NAME="${PREFIX}-${STACK_FILE}"
    $AWS cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --query "Stacks[0].Outputs[*].[OutputKey,OutputValue]" \
        --output table 2>/dev/null || true
done
