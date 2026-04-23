#!/usr/bin/env bash
set -euo pipefail

STACK_PREFIX="${1:-b3-pipeline}"
REGION="${2:-us-east-1}"
PROFILE="${3:-agenda-facil}"

AWS="aws --region $REGION --profile $PROFILE"
ACCOUNT_ID=$($AWS sts get-caller-identity --query Account --output text)

echo "=== Deploy B3 Pipeline (EC2 + Redshift) ==="
echo "Account: $ACCOUNT_ID | Region: $REGION"

# Verificar/criar key pair
KEY_NAME="${STACK_PREFIX}-key"
if ! $AWS ec2 describe-key-pairs --key-names "$KEY_NAME" &>/dev/null; then
  echo ">>> Criando key pair..."
  $AWS ec2 create-key-pair --key-name "$KEY_NAME" \
    --query 'KeyMaterial' --output text > "${KEY_NAME}.pem"
  chmod 400 "${KEY_NAME}.pem"
  echo "    Chave salva em ${KEY_NAME}.pem"
fi

# Stack 1: Network
echo ">>> Stack 01-network..."
$AWS cloudformation deploy \
  --template-file cloudformation/01-network.yaml \
  --stack-name "${STACK_PREFIX}-network" \
  --parameter-overrides ProjectTag=$STACK_PREFIX \
  --no-fail-on-empty-changeset \
  --tags Project=$STACK_PREFIX

# Stack 2: Storage + IAM
echo ">>> Stack 02-storage..."
$AWS cloudformation deploy \
  --template-file cloudformation/02-storage.yaml \
  --stack-name "${STACK_PREFIX}-storage" \
  --parameter-overrides ProjectTag=$STACK_PREFIX \
  --capabilities CAPABILITY_NAMED_IAM \
  --no-fail-on-empty-changeset \
  --tags Project=$STACK_PREFIX

# Stack 3: EC2 + Redshift
echo ">>> Stack 03-compute (EC2 + Redshift ~5 min)..."
$AWS cloudformation deploy \
  --template-file cloudformation/03-compute.yaml \
  --stack-name "${STACK_PREFIX}-compute" \
  --parameter-overrides \
    ProjectTag=$STACK_PREFIX \
    RedshiftPassword='B3pipeline#2024' \
    KeyPairName=$KEY_NAME \
  --no-fail-on-empty-changeset \
  --tags Project=$STACK_PREFIX

# Pegar outputs
EC2_IP=$($AWS cloudformation describe-stacks --stack-name "${STACK_PREFIX}-compute" \
  --query "Stacks[0].Outputs[?OutputKey=='AirflowPublicIp'].OutputValue" --output text)

echo ""
echo "=== Deploy completo ==="
echo "Airflow UI: http://${EC2_IP}:8080 (admin/admin)"
echo "SSH: ssh -i ${KEY_NAME}.pem ubuntu@${EC2_IP}"
echo ""
echo ">>> Aguarde ~3-5 min para o Airflow inicializar via UserData"
