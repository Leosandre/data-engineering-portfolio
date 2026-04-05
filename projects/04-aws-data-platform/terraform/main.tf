terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region  = var.region
  profile = var.aws_profile
}

variable "region" {
  default = "us-east-1"
}

variable "aws_profile" {
  description = "AWS CLI profile"
  type        = string
}

variable "project" {
  default = "data-platform-portfolio"
}

variable "environment" {
  default = "dev"
}

variable "redshift_admin_password" {
  description = "Senha do admin do Redshift. Passar via -var ou terraform.tfvars (NUNCA commitar)"
  type        = string
  sensitive   = true
}

locals {
  prefix = "${var.project}-${var.environment}"
  tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

data "aws_caller_identity" "current" {}

# ===========================================================================
# S3 Data Lake
# ===========================================================================

resource "aws_s3_bucket" "data_lake" {
  bucket        = "${local.prefix}-lake-${data.aws_caller_identity.current.account_id}"
  force_destroy = true
  tags          = local.tags
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ===========================================================================
# Glue (Catalog + IAM)
# ===========================================================================

resource "aws_glue_catalog_database" "bronze" {
  name = "${replace(local.prefix, "-", "_")}_bronze"
}

resource "aws_glue_catalog_database" "silver" {
  name = "${replace(local.prefix, "-", "_")}_silver"
}

resource "aws_glue_catalog_database" "gold" {
  name = "${replace(local.prefix, "-", "_")}_gold"
}

resource "aws_iam_role" "glue" {
  name = "${local.prefix}-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "${local.prefix}-glue-s3"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
      Resource = [aws_s3_bucket.data_lake.arn, "${aws_s3_bucket.data_lake.arn}/*"]
    }]
  })
}

# ===========================================================================
# Redshift Serverless
# ===========================================================================

# Role pro Redshift acessar S3 (COPY command).
# IMPORTANTE: a trust policy precisa ser pra redshift.amazonaws.com,
# nao pra glue. Descobrimos isso no deploy — usar a role do Glue
# retorna UnauthorizedException no COPY.
resource "aws_iam_role" "redshift" {
  name = "${local.prefix}-redshift-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
    }]
  })
  tags = local.tags
}

resource "aws_iam_role_policy" "redshift_s3" {
  name = "${local.prefix}-redshift-s3"
  role = aws_iam_role.redshift.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:ListBucket"]
      Resource = [aws_s3_bucket.data_lake.arn, "${aws_s3_bucket.data_lake.arn}/*"]
    }]
  })
}

resource "aws_redshiftserverless_namespace" "main" {
  namespace_name      = "${local.prefix}-ns"
  db_name             = "analytics"
  admin_username      = "admin"
  admin_user_password = var.redshift_admin_password
  iam_roles           = [aws_iam_role.redshift.arn]
  tags                = local.tags
}

resource "aws_redshiftserverless_workgroup" "main" {
  namespace_name     = aws_redshiftserverless_namespace.main.namespace_name
  workgroup_name     = "${local.prefix}-wg"
  base_capacity      = 8
  publicly_accessible = true  # necessario pra conectar de fora da VPC

  # IMPORTANTE: precisa de VPC default na conta.
  # Se nao existir: aws ec2 create-default-vpc
  tags = local.tags
}

# Security group rule pra permitir conexao na porta 5439.
# Em producao, restringir o CIDR pro IP do escritorio/VPN.
resource "aws_vpc_security_group_ingress_rule" "redshift" {
  security_group_id = tolist(aws_redshiftserverless_workgroup.main.security_group_ids)[0]
  ip_protocol       = "tcp"
  from_port         = 5439
  to_port           = 5439
  cidr_ipv4         = "0.0.0.0/0"  # restringir em producao
  description       = "Redshift Serverless - acesso externo"
  tags              = local.tags
}

# ===========================================================================
# Step Functions
# ===========================================================================

resource "aws_iam_role" "step_functions" {
  name = "${local.prefix}-sfn-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
    }]
  })
  tags = local.tags
}

resource "aws_iam_role_policy" "sfn_glue" {
  name = "${local.prefix}-sfn-glue"
  role = aws_iam_role.step_functions.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns"]
      Resource = "*"
    }]
  })
}

# ===========================================================================
# Outputs
# ===========================================================================

output "s3_bucket" {
  value = aws_s3_bucket.data_lake.bucket
}

output "glue_role_arn" {
  value = aws_iam_role.glue.arn
}

output "redshift_role_arn" {
  value = aws_iam_role.redshift.arn
}

output "redshift_endpoint" {
  value = aws_redshiftserverless_workgroup.main.endpoint
}

output "redshift_workgroup" {
  value = aws_redshiftserverless_workgroup.main.workgroup_name
}
