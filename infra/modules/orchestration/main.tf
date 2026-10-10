terraform {
  required_version = ">= 1.9.0, < 2.0.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.68"
    }
  }
}

locals {
  image_uri = var.image_uri != "" ? var.image_uri : "${aws_ecr_repository.batch.repository_url}:bootstrap"
}

# ---------------------------------------------------------------------------- image registry
resource "aws_ecr_repository" "batch" {
  name                 = "${var.name}-batch"
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "KMS"
    kms_key         = var.kms_key_arn
  }
}

resource "aws_ecr_lifecycle_policy" "batch" {
  repository = aws_ecr_repository.batch.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep the 10 most recent images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 10 }
      action       = { type = "expire" }
    }]
  })
}

# ---------------------------------------------------------------------------- batch-step Lambda
data "aws_iam_policy_document" "assume_lambda" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "batch" {
  name                 = "${var.name}-batch-step"
  path                 = var.role_path
  assume_role_policy   = data.aws_iam_policy_document.assume_lambda.json
  permissions_boundary = var.permissions_boundary_arn
}

data "aws_iam_policy_document" "batch" {
  statement {
    sid       = "LakeObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${var.lake_bucket_arn}/*"]
  }
  statement {
    sid       = "LakeList"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [var.lake_bucket_arn]
  }
  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable", "glue:GetTables", "glue:CreateTable",
      "glue:UpdateTable", "glue:DeleteTable", "glue:GetPartitions", "glue:BatchCreatePartition",
    ]
    resources = concat(
      ["arn:${var.partition}:glue:${var.region}:${var.account_id}:catalog"],
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
    resources = ["arn:${var.partition}:athena:${var.region}:${var.account_id}:workgroup/${var.athena_workgroup}"]
  }
  statement {
    sid       = "AthenaResultsObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"]
    resources = ["${var.athena_results_bucket_arn}/*"]
  }
  statement {
    sid       = "AthenaResultsList"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [var.athena_results_bucket_arn]
  }
  statement {
    sid       = "OwnDlq"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.batch_dlq.arn]
  }
  statement {
    sid       = "ExportCounters"
    actions   = ["dynamodb:Scan", "dynamodb:DescribeTable"]
    resources = [var.counters_table_arn]
  }
  statement {
    sid       = "DrainDlq"
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueUrl", "sqs:GetQueueAttributes"]
    resources = [var.dlq_arn]
  }
  statement {
    sid       = "Kms"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
    resources = [var.kms_key_arn]
  }
  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.batch.arn}:*"]
  }
  statement {
    sid = "XRay"
    #checkov:skip=CKV_AWS_111:X-Ray trace segment APIs do not support resource-level permissions.
    actions   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "batch" {
  name   = "batch-step"
  role   = aws_iam_role.batch.id
  policy = data.aws_iam_policy_document.batch.json
}

resource "aws_cloudwatch_log_group" "batch" {
  name              = "/aws/lambda/${var.name}-batch-step"
  retention_in_days = var.log_retention_days
  kms_key_id        = var.kms_key_arn
}

resource "aws_sqs_queue" "batch_dlq" {
  name                              = "${var.name}-batch-step-dlq"
  message_retention_seconds         = 1209600
  kms_master_key_id                 = var.kms_key_arn
  kms_data_key_reuse_period_seconds = 3600
}

resource "aws_lambda_function" "batch" {
  #checkov:skip=CKV_AWS_117:Talks only to AWS APIs and the public TLC/Open-Meteo endpoints; a VPC would need a NAT gateway (~$32/month) for no isolation benefit.
  #checkov:skip=CKV_AWS_272:Lambda code signing is not supported for container-image functions.
  function_name                  = "${var.name}-batch-step"
  description                    = "Daily batch steps (ingest, load, gates, dbt) invoked by Step Functions"
  role                           = aws_iam_role.batch.arn
  package_type                   = "Image"
  image_uri                      = local.image_uri
  architectures                  = ["x86_64"]
  memory_size                    = 10240
  timeout                        = 900
  reserved_concurrent_executions = 2
  kms_key_arn                    = var.kms_key_arn

  ephemeral_storage {
    size = 10240
  }

  environment {
    variables = {
      UBER_AWS_MODE         = "aws"
      UBER_CATALOG_TYPE     = "glue"
      UBER_WAREHOUSE        = "s3://${var.lake_bucket_name}/warehouse"
      UBER_DATA_DIR         = "/tmp/uber"
      UBER_LANDING_PREFIX   = "landing/events"
      UBER_LANDING_BUCKET   = var.lake_bucket_name
      UBER_COUNTERS_TABLE   = var.counters_table_name
      UBER_DLQ_QUEUE        = var.dlq_name
      DBT_TARGET            = "athena"
      UBER_ATHENA_WORKGROUP = var.athena_workgroup
    }
  }

  tracing_config {
    mode = "Active"
  }

  dead_letter_config {
    target_arn = aws_sqs_queue.batch_dlq.arn
  }

  logging_config {
    log_format = "JSON"
    log_group  = aws_cloudwatch_log_group.batch.name
  }
}

# ---------------------------------------------------------------------------- Step Functions
resource "aws_cloudwatch_log_group" "sfn" {
  name              = "/aws/vendedlogs/states/${var.name}-daily-batch"
  retention_in_days = var.log_retention_days
  kms_key_id        = var.kms_key_arn
}

data "aws_iam_policy_document" "assume_states" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [var.account_id]
    }
  }
}

resource "aws_iam_role" "sfn" {
  name                 = "${var.name}-daily-batch-sfn"
  path                 = var.role_path
  assume_role_policy   = data.aws_iam_policy_document.assume_states.json
  permissions_boundary = var.permissions_boundary_arn
}

data "aws_iam_policy_document" "sfn" {
  statement {
    sid       = "InvokeBatchStep"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.batch.arn, "${aws_lambda_function.batch.arn}:*"]
  }
  statement {
    sid = "LogDelivery"
    #checkov:skip=CKV_AWS_111:CloudWatch Logs vended-log delivery APIs require Resource "*" (AWS documented).
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
    actions   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords", "xray:GetSamplingRules", "xray:GetSamplingTargets"]
    resources = ["*"]
  }
  statement {
    sid       = "Kms"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [var.kms_key_arn]
  }
}

resource "aws_iam_role_policy" "sfn" {
  name   = "daily-batch"
  role   = aws_iam_role.sfn.id
  policy = data.aws_iam_policy_document.sfn.json
}

resource "aws_sfn_state_machine" "daily_batch" {
  name     = "${var.name}-daily-batch"
  role_arn = aws_iam_role.sfn.arn
  type     = "STANDARD"
  # The same ASL file the local runner executes (src/uber_rides/orchestration/daily_batch.asl.json).
  definition = templatefile(var.asl_template_path, {
    BatchStepFunctionArn = aws_lambda_function.batch.arn
  })

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  tracing_configuration {
    enabled = true
  }

  encryption_configuration {
    type                              = "CUSTOMER_MANAGED_KMS_KEY"
    kms_key_id                        = var.kms_key_arn
    kms_data_key_reuse_period_seconds = 900
  }
}

# ---------------------------------------------------------------------------- EventBridge Scheduler
data "aws_iam_policy_document" "assume_scheduler" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [var.account_id]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name                 = "${var.name}-daily-batch-scheduler"
  path                 = var.role_path
  assume_role_policy   = data.aws_iam_policy_document.assume_scheduler.json
  permissions_boundary = var.permissions_boundary_arn
}

data "aws_iam_policy_document" "scheduler" {
  statement {
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.daily_batch.arn]
  }
}

resource "aws_iam_role_policy" "scheduler" {
  name   = "start-daily-batch"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler.json
}

resource "aws_scheduler_schedule" "daily_batch" {
  name                         = "${var.name}-daily-batch"
  description                  = "Daily: fold new TLC months and the streaming landing zone into the marts"
  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = var.schedule_timezone
  kms_key_arn                  = var.kms_key_arn

  flexible_time_window {
    mode                      = "FLEXIBLE"
    maximum_window_in_minutes = 30
  }

  target {
    arn      = aws_sfn_state_machine.daily_batch.arn
    role_arn = aws_iam_role.scheduler.arn
    input    = jsonencode({})

    retry_policy {
      maximum_retry_attempts       = 2
      maximum_event_age_in_seconds = 3600
    }
  }
}

resource "aws_cloudwatch_metric_alarm" "batch_failed" {
  alarm_name          = "${var.name}-daily-batch-failed"
  alarm_description   = "The daily batch state machine failed (quality gate or step error); see docs/runbook.md."
  namespace           = "AWS/States"
  metric_name         = "ExecutionsFailed"
  dimensions          = { StateMachineArn = aws_sfn_state_machine.daily_batch.arn }
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [var.alarm_topic_arn]
}
