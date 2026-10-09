terraform {
  required_version = ">= 1.9.0, < 2.0.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.68"
    }
  }
}

# One customer-managed key for the whole stack: S3, Kinesis, DynamoDB, SQS, SNS, CloudWatch
# Logs, Lambda environment variables, ECR and Athena results. Rotation is on.
data "aws_iam_policy_document" "key" {
  #checkov:skip=CKV_AWS_109:Key policies must use Resource "*" (it means "this key"); access is scoped by principal and service conditions.
  #checkov:skip=CKV_AWS_111:Same as above - a key policy's resource is always the key itself.
  #checkov:skip=CKV_AWS_356:Same as above - "*" in a key policy refers only to this key.
  statement {
    sid       = "AccountAdministration"
    actions   = ["kms:*"]
    resources = ["*"]
    principals {
      type        = "AWS"
      identifiers = ["arn:${var.partition}:iam::${var.account_id}:root"]
    }
  }

  statement {
    sid = "CloudWatchLogs"
    actions = [
      "kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:DescribeKey",
    ]
    resources = ["*"]
    principals {
      type        = "Service"
      identifiers = ["logs.${var.region}.amazonaws.com"]
    }
    condition {
      test     = "ArnLike"
      variable = "kms:EncryptionContext:aws:logs:arn"
      values   = ["arn:${var.partition}:logs:${var.region}:${var.account_id}:log-group:*"]
    }
  }

  statement {
    sid       = "AwsServicesInThisAccount"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey*", "kms:DescribeKey"]
    resources = ["*"]
    principals {
      type = "Service"
      identifiers = [
        "sns.amazonaws.com", "sqs.amazonaws.com", "events.amazonaws.com", "scheduler.amazonaws.com",
        "states.amazonaws.com", "budgets.amazonaws.com", "cloudwatch.amazonaws.com",
      ]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [var.account_id]
    }
  }
}

resource "aws_kms_key" "this" {
  description             = "${var.name}: encryption for the Uber rides streaming stack"
  enable_key_rotation     = true
  rotation_period_in_days = 365
  deletion_window_in_days = 30
  policy                  = data.aws_iam_policy_document.key.json
}

resource "aws_kms_alias" "this" {
  name          = "alias/${var.name}"
  target_key_id = aws_kms_key.this.key_id
}
