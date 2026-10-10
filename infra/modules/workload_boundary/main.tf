terraform {
  required_version = ">= 1.9.0, < 2.0.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.68"
    }
  }
}

# Permissions boundary for every workload role (stream consumer, batch-step Lambda, Step
# Functions, Scheduler). A role's effective permissions are the intersection of its inline
# policy and this boundary, so even if the deploy role writes a broader inline policy the
# workload can only reach this project's resources. It grants no IAM, STS or KMS-admin actions.
# Resources are matched by the deterministic "<name>-*" naming every module uses.
locals {
  arn_prefix = {
    kinesis  = "arn:${var.partition}:kinesis:${var.region}:${var.account_id}"
    dynamodb = "arn:${var.partition}:dynamodb:${var.region}:${var.account_id}"
    sqs      = "arn:${var.partition}:sqs:${var.region}:${var.account_id}"
    logs     = "arn:${var.partition}:logs:${var.region}:${var.account_id}"
    lambda   = "arn:${var.partition}:lambda:${var.region}:${var.account_id}"
    states   = "arn:${var.partition}:states:${var.region}:${var.account_id}"
    athena   = "arn:${var.partition}:athena:${var.region}:${var.account_id}"
    glue     = "arn:${var.partition}:glue:${var.region}:${var.account_id}"
  }
  data_buckets = [
    "arn:${var.partition}:s3:::${var.name}-lake-${var.account_id}",
    "arn:${var.partition}:s3:::${var.name}-athena-results-${var.account_id}",
  ]
}

data "aws_iam_policy_document" "boundary" {
  statement {
    sid = "ReadStream"
    actions = [
      "kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary", "kinesis:ListShards", "kinesis:SubscribeToShard",
    ]
    resources = ["${local.arn_prefix.kinesis}:stream/${var.name}-*"]
  }
  statement {
    sid       = "ListStreams"
    actions   = ["kinesis:ListStreams"]
    resources = ["${local.arn_prefix.kinesis}:stream/*"]
  }
  statement {
    sid = "Tables"
    actions = [
      "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:ConditionCheckItem",
      "dynamodb:Scan", "dynamodb:DescribeTable",
    ]
    resources = ["${local.arn_prefix.dynamodb}:table/${var.name}-*"]
  }
  statement {
    sid = "Queues"
    actions = [
      "sqs:SendMessage", "sqs:ReceiveMessage", "sqs:DeleteMessage",
      "sqs:GetQueueUrl", "sqs:GetQueueAttributes",
    ]
    resources = ["${local.arn_prefix.sqs}:${var.name}-*"]
  }
  statement {
    sid       = "LakeObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = [for b in local.data_buckets : "${b}/*"]
  }
  statement {
    sid       = "LakeList"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = local.data_buckets
  }
  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:CreateTable",
      "glue:UpdateTable", "glue:DeleteTable", "glue:GetPartitions", "glue:BatchCreatePartition",
    ]
    resources = concat(
      ["${local.arn_prefix.glue}:catalog"],
      var.glue_database_arns,
      [for arn in var.glue_database_arns : replace("${arn}/*", ":database/", ":table/")],
    )
  }
  statement {
    sid = "Athena"
    actions = [
      "athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults",
      "athena:StopQueryExecution", "athena:GetWorkGroup",
    ]
    resources = ["${local.arn_prefix.athena}:workgroup/${var.name}"]
  }
  statement {
    sid       = "ProjectKey"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
    resources = [var.kms_key_arn]
  }
  statement {
    sid     = "LogStreams"
    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [
      "${local.arn_prefix.logs}:log-group:/aws/lambda/${var.name}-*:*",
      "${local.arn_prefix.logs}:log-group:/aws/vendedlogs/states/${var.name}-*:*",
    ]
  }
  statement {
    sid = "StatesLogDelivery"
    #checkov:skip=CKV_AWS_111:CloudWatch Logs vended-log delivery APIs require Resource "*" (AWS documented); used by the Step Functions role.
    #checkov:skip=CKV_AWS_356:Same as above.
    actions = [
      "logs:CreateLogDelivery", "logs:GetLogDelivery", "logs:UpdateLogDelivery", "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries", "logs:PutResourcePolicy", "logs:DescribeResourcePolicies", "logs:DescribeLogGroups",
    ]
    resources = ["*"]
  }
  statement {
    sid = "XRay"
    #checkov:skip=CKV_AWS_111:X-Ray APIs do not support resource-level permissions.
    #checkov:skip=CKV_AWS_356:Same as above.
    actions   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords", "xray:GetSamplingRules", "xray:GetSamplingTargets"]
    resources = ["*"]
  }
  statement {
    sid       = "InvokeProjectFunctions"
    actions   = ["lambda:InvokeFunction"]
    resources = ["${local.arn_prefix.lambda}:function:${var.name}-*"]
  }
  statement {
    sid       = "StartProjectStateMachines"
    actions   = ["states:StartExecution"]
    resources = ["${local.arn_prefix.states}:stateMachine:${var.name}-*"]
  }
}

resource "aws_iam_policy" "boundary" {
  name        = "${var.name}-workload-boundary"
  path        = var.path
  description = "Permissions boundary for ${var.name} workload roles: project resources only, no IAM."
  policy      = data.aws_iam_policy_document.boundary.json
}
