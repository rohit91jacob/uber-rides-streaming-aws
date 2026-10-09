terraform {
  required_version = ">= 1.9.0, < 2.0.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.68"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  # lake: landing zone (events/, tlc/) + Iceberg warehouse (warehouse/); athena: query results
  data_buckets = {
    lake   = "${var.name}-lake-${data.aws_caller_identity.current.account_id}"
    athena = "${var.name}-athena-results-${data.aws_caller_identity.current.account_id}"
  }
  glue_databases = ["uber_lake", "uber_lake_ops", "uber_staging", "uber_intermediate", "uber_marts"]
}

# ---------------------------------------------------------------------------- access logs
resource "aws_s3_bucket" "logs" {
  #checkov:skip=CKV_AWS_18:This is the access-log target bucket; logging it to itself would loop.
  #checkov:skip=CKV_AWS_144:Single-region portfolio stack; cross-region replication doubles storage cost.
  #checkov:skip=CKV_AWS_145:S3 server access logging only delivers to SSE-S3 buckets, not SSE-KMS.
  #checkov:skip=CKV2_AWS_62:No consumers of access-log object events.
  bucket        = "${var.name}-access-logs-${data.aws_caller_identity.current.account_id}"
  force_destroy = false
}

resource "aws_s3_bucket_server_side_encryption_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "logs" {
  bucket                  = aws_s3_bucket.logs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "logs" {
  bucket = aws_s3_bucket.logs.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_ownership_controls" "logs" {
  bucket = aws_s3_bucket.logs.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id
  rule {
    id     = "expire-access-logs"
    status = "Enabled"
    filter {}
    expiration {
      days = 90
    }
    noncurrent_version_expiration {
      noncurrent_days = 7
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

data "aws_iam_policy_document" "logs" {
  statement {
    sid       = "S3ServerAccessLogs"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.logs.arn}/*"]
    principals {
      type        = "Service"
      identifiers = ["logging.s3.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.logs.arn, "${aws_s3_bucket.logs.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "logs" {
  bucket = aws_s3_bucket.logs.id
  policy = data.aws_iam_policy_document.logs.json
}

# ---------------------------------------------------------------------------- data buckets
module "lake_bucket" {
  source = "../s3_bucket"

  bucket_name       = local.data_buckets.lake
  kms_key_arn       = var.kms_key_arn
  access_log_bucket = aws_s3_bucket.logs.id
  # Cost guardrail: raw landing objects are re-fetchable (TLC) or already folded into Iceberg.
  prefix_rules = [{
    id                = "landing-to-infrequent-access"
    prefix            = "landing/"
    ia_after_days     = 30
    expire_after_days = 400
  }]
}

module "athena_results_bucket" {
  source = "../s3_bucket"

  bucket_name       = local.data_buckets.athena
  kms_key_arn       = var.kms_key_arn
  access_log_bucket = aws_s3_bucket.logs.id
  prefix_rules = [{
    id                = "expire-query-results"
    prefix            = "results/"
    expire_after_days = 7
  }]
}

# ---------------------------------------------------------------------------- Glue catalog
resource "aws_glue_data_catalog_encryption_settings" "this" {
  data_catalog_encryption_settings {
    connection_password_encryption {
      aws_kms_key_id                       = var.kms_key_arn
      return_connection_password_encrypted = true
    }
    encryption_at_rest {
      catalog_encryption_mode = "SSE-KMS"
      sse_aws_kms_key_id      = var.kms_key_arn
    }
  }
}

resource "aws_glue_catalog_database" "this" {
  for_each     = toset(local.glue_databases)
  name         = each.value
  description  = "Uber rides streaming lake: ${each.value}"
  location_uri = "s3://${module.lake_bucket.name}/warehouse/${each.value}.db"
}

# ---------------------------------------------------------------------------- Athena
resource "aws_athena_workgroup" "this" {
  name          = var.name
  force_destroy = false

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    # Cost guardrail: a runaway query is cancelled after this many bytes scanned.
    bytes_scanned_cutoff_per_query = var.athena_bytes_scanned_cutoff

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${module.athena_results_bucket.name}/results/"
      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = var.kms_key_arn
      }
    }
  }
}
