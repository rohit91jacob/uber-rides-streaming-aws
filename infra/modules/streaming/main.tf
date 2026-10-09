terraform {
  required_version = ">= 1.9.0, < 2.0.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.68"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7"
    }
  }
}

data "aws_region" "current" {}

# ---------------------------------------------------------------------------- Kinesis
resource "aws_kinesis_stream" "events" {
  name             = "${var.name}-ride-events"
  shard_count      = var.shard_count
  retention_period = var.retention_hours
  encryption_type  = "KMS"
  kms_key_id       = var.kms_key_arn

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  shard_level_metrics = ["IncomingRecords", "IteratorAgeMilliseconds"]
}

# ---------------------------------------------------------------------------- DynamoDB
resource "aws_dynamodb_table" "dedupe" {
  name         = "${var.name}-ride-event-dedupe"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "event_id"

  attribute {
    name = "event_id"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.kms_key_arn
  }

  deletion_protection_enabled = true
}

resource "aws_dynamodb_table" "counters" {
  name         = "${var.name}-zone-supply-demand"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.kms_key_arn
  }

  deletion_protection_enabled = true
}

# ---------------------------------------------------------------------------- SQS
# Invalid records (written by the consumer) and failed batches (event source on-failure
# destination + async-invoke DLQ).
resource "aws_sqs_queue" "invalid_records" {
  name                              = "${var.name}-ride-events-dlq"
  message_retention_seconds         = 1209600
  kms_master_key_id                 = var.kms_key_arn
  kms_data_key_reuse_period_seconds = 3600
}

resource "aws_sqs_queue" "failed_batches" {
  name                              = "${var.name}-ride-events-failed-batches"
  message_retention_seconds         = 1209600
  kms_master_key_id                 = var.kms_key_arn
  kms_data_key_reuse_period_seconds = 3600
}

# ---------------------------------------------------------------------------- alarms topic
resource "aws_sns_topic" "alarms" {
  name              = "${var.name}-alarms"
  kms_master_key_id = var.kms_key_arn
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alarm_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alarms.arn
  protocol  = "email"
  endpoint  = var.alarm_email
}

# ---------------------------------------------------------------------------- Lambda package
# Only the stdlib-only consumer modules ship; boto3 comes with the Lambda runtime.
data "archive_file" "consumer" {
  type        = "zip"
  output_path = "${path.module}/.build/consumer.zip"

  source {
    content  = file("${var.source_dir}/__init__.py")
    filename = "uber_rides/__init__.py"
  }
  source {
    content  = file("${var.source_dir}/streaming/__init__.py")
    filename = "uber_rides/streaming/__init__.py"
  }
  source {
    content  = file("${var.source_dir}/streaming/events.py")
    filename = "uber_rides/streaming/events.py"
  }
  source {
    content  = file("${var.source_dir}/streaming/consumer.py")
    filename = "uber_rides/streaming/consumer.py"
  }
}

resource "aws_signer_signing_profile" "lambda" {
  name_prefix = replace("${var.name}_consumer_", "-", "_")
  platform_id = "AWSLambda-SHA384-ECDSA"
}

resource "aws_lambda_code_signing_config" "consumer" {
  description = "${var.name}: consumer code signing (warn until the pipeline signs artifacts)"
  allowed_publishers {
    signing_profile_version_arns = [aws_signer_signing_profile.lambda.version_arn]
  }
  policies {
    untrusted_artifact_on_deployment = "Warn"
  }
}

# ---------------------------------------------------------------------------- IAM
data "aws_iam_policy_document" "assume_lambda" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "consumer" {
  name               = "${var.name}-consumer"
  assume_role_policy = data.aws_iam_policy_document.assume_lambda.json
}

data "aws_iam_policy_document" "consumer" {
  statement {
    sid = "ReadStream"
    actions = [
      "kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary", "kinesis:ListShards", "kinesis:SubscribeToShard",
    ]
    resources = [aws_kinesis_stream.events.arn]
  }
  statement {
    sid       = "ListStreams"
    actions   = ["kinesis:ListStreams"]
    resources = ["arn:aws:kinesis:${data.aws_region.current.region}:*:stream/*"]
  }
  statement {
    sid       = "DedupeAndCount"
    actions   = ["dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:ConditionCheckItem"]
    resources = [aws_dynamodb_table.dedupe.arn, aws_dynamodb_table.counters.arn]
  }
  statement {
    sid       = "DeadLetter"
    actions   = ["sqs:SendMessage", "sqs:GetQueueAttributes"]
    resources = [aws_sqs_queue.invalid_records.arn, aws_sqs_queue.failed_batches.arn]
  }
  statement {
    sid       = "LandEvents"
    actions   = ["s3:PutObject"]
    resources = ["${var.landing_bucket_arn}/landing/events/*"]
  }
  statement {
    sid       = "Kms"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [var.kms_key_arn]
  }
  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.consumer.arn}:*"]
  }
  statement {
    sid = "XRay"
    #checkov:skip=CKV_AWS_111:X-Ray trace segment APIs do not support resource-level permissions.
    actions   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "consumer" {
  name   = "consumer"
  role   = aws_iam_role.consumer.id
  policy = data.aws_iam_policy_document.consumer.json
}

# ---------------------------------------------------------------------------- Lambda
resource "aws_cloudwatch_log_group" "consumer" {
  name              = "/aws/lambda/${var.name}-consumer"
  retention_in_days = var.log_retention_days
  kms_key_id        = var.kms_key_arn
}

resource "aws_lambda_function" "consumer" {
  #checkov:skip=CKV_AWS_117:Talks only to AWS APIs (Kinesis, DynamoDB, SQS, S3); a VPC would add NAT/endpoint cost without isolating anything.
  function_name                  = "${var.name}-consumer"
  description                    = "Kinesis ride events: validate, DLQ, land to S3, exactly-once zone counters"
  role                           = aws_iam_role.consumer.arn
  runtime                        = "python3.12"
  architectures                  = ["arm64"]
  handler                        = "uber_rides.streaming.consumer.handler"
  filename                       = data.archive_file.consumer.output_path
  source_code_hash               = data.archive_file.consumer.output_base64sha256
  memory_size                    = 512
  timeout                        = 120
  reserved_concurrent_executions = var.reserved_concurrency
  kms_key_arn                    = var.kms_key_arn
  code_signing_config_arn        = aws_lambda_code_signing_config.consumer.arn

  environment {
    variables = {
      UBER_DEDUPE_TABLE           = aws_dynamodb_table.dedupe.name
      UBER_COUNTERS_TABLE         = aws_dynamodb_table.counters.name
      UBER_DLQ_URL                = aws_sqs_queue.invalid_records.url
      UBER_LANDING_BUCKET         = var.landing_bucket_name
      UBER_LANDING_PREFIX         = "landing/events"
      UBER_COUNTER_WINDOW_MINUTES = tostring(var.counter_window_minutes)
    }
  }

  tracing_config {
    mode = "Active"
  }

  dead_letter_config {
    target_arn = aws_sqs_queue.failed_batches.arn
  }

  logging_config {
    log_format = "JSON"
    log_group  = aws_cloudwatch_log_group.consumer.name
  }
}

resource "aws_lambda_event_source_mapping" "consumer" {
  event_source_arn                   = aws_kinesis_stream.events.arn
  function_name                      = aws_lambda_function.consumer.arn
  starting_position                  = "TRIM_HORIZON"
  batch_size                         = var.batch_size
  maximum_batching_window_in_seconds = 5
  parallelization_factor             = 1
  maximum_retry_attempts             = 5
  maximum_record_age_in_seconds      = 3600
  bisect_batch_on_function_error     = true
  function_response_types            = ["ReportBatchItemFailures"]

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.failed_batches.arn
    }
  }
}

# ---------------------------------------------------------------------------- alarms
resource "aws_cloudwatch_metric_alarm" "iterator_age" {
  alarm_name          = "${var.name}-consumer-iterator-age"
  alarm_description   = "Consumer is falling behind the stream (> 5 minutes)."
  namespace           = "AWS/Lambda"
  metric_name         = "IteratorAge"
  dimensions          = { FunctionName = aws_lambda_function.consumer.function_name }
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 5
  threshold           = 300000
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alarms.arn]
}

resource "aws_cloudwatch_metric_alarm" "consumer_errors" {
  alarm_name          = "${var.name}-consumer-errors"
  alarm_description   = "Consumer invocations are failing."
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = aws_lambda_function.consumer.function_name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alarms.arn]
}

resource "aws_cloudwatch_metric_alarm" "dlq_depth" {
  for_each = {
    invalid = aws_sqs_queue.invalid_records.name
    failed  = aws_sqs_queue.failed_batches.name
  }
  alarm_name          = "${var.name}-${each.key}-queue-not-empty"
  alarm_description   = "Messages are waiting in ${each.value}; see docs/runbook.md."
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = each.value }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alarms.arn]
}
