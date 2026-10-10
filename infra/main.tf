locals {
  project = "uber-rides-streaming-aws"
  name    = "uber-rides-${var.environment}"
  # Workload roles live under this IAM path; the deploy role can only manage roles there.
  role_path = "/${local.name}/"
  tags = {
    project     = local.project
    environment = var.environment
    owner       = var.owner
    managed_by  = "terraform"
  }
}

data "aws_caller_identity" "current" {}

data "aws_partition" "current" {}

module "kms" {
  source = "./modules/kms"

  name       = local.name
  account_id = data.aws_caller_identity.current.account_id
  region     = var.region
  partition  = data.aws_partition.current.partition
}

module "workload_boundary" {
  source = "./modules/workload_boundary"

  name               = local.name
  path               = local.role_path
  kms_key_arn        = module.kms.key_arn
  glue_database_arns = module.lake.glue_database_arns
  region             = var.region
  account_id         = data.aws_caller_identity.current.account_id
  partition          = data.aws_partition.current.partition
}

module "lake" {
  source = "./modules/lake"

  name                        = local.name
  kms_key_arn                 = module.kms.key_arn
  athena_bytes_scanned_cutoff = var.athena_bytes_scanned_cutoff
}

module "streaming" {
  source = "./modules/streaming"

  name                   = local.name
  kms_key_arn            = module.kms.key_arn
  shard_count            = var.kinesis_shard_count
  retention_hours        = var.kinesis_retention_hours
  batch_size             = var.consumer_batch_size
  reserved_concurrency   = var.consumer_reserved_concurrency
  counter_window_minutes = var.counter_window_minutes
  landing_bucket_name    = module.lake.lake_bucket_name
  landing_bucket_arn     = module.lake.lake_bucket_arn
  source_dir             = "${path.root}/../src/uber_rides"
  log_retention_days     = var.log_retention_days
  alarm_email            = var.alarm_email

  role_path                = local.role_path
  permissions_boundary_arn = module.workload_boundary.arn
}

module "orchestration" {
  source = "./modules/orchestration"

  name                = local.name
  kms_key_arn         = module.kms.key_arn
  image_uri           = var.batch_image_uri
  asl_template_path   = "${path.root}/../src/uber_rides/orchestration/daily_batch.asl.json"
  schedule_expression = var.batch_schedule_expression
  schedule_timezone   = var.batch_schedule_timezone
  lake_bucket_name    = module.lake.lake_bucket_name
  lake_bucket_arn     = module.lake.lake_bucket_arn
  athena_workgroup    = module.lake.athena_workgroup_name
  glue_database_arns  = module.lake.glue_database_arns

  athena_results_bucket_arn = module.lake.athena_results_bucket_arn
  counters_table_arn        = module.streaming.counters_table_arn
  dlq_arn                   = module.streaming.dlq_arn
  dlq_name                  = module.streaming.dlq_name
  counters_table_name       = module.streaming.counters_table_name
  alarm_topic_arn           = module.streaming.alarm_topic_arn
  log_retention_days        = var.log_retention_days
  region                    = var.region
  account_id                = data.aws_caller_identity.current.account_id
  partition                 = data.aws_partition.current.partition

  role_path                = local.role_path
  permissions_boundary_arn = module.workload_boundary.arn
}

module "budget" {
  source = "./modules/budget"

  name             = local.name
  project_tag      = local.project
  limit_usd        = var.monthly_budget_usd
  alert_emails     = var.budget_alert_emails
  alarm_topic_arn  = module.streaming.alarm_topic_arn
  create_sns_alert = true
}

module "github_oidc" {
  source = "./modules/github_oidc"

  name            = local.name
  repository      = var.github_repository
  create_provider = var.create_github_oidc_provider
  account_id      = data.aws_caller_identity.current.account_id
  partition       = data.aws_partition.current.partition
  project_tag     = local.project

  workload_role_path       = local.role_path
  permissions_boundary_arn = module.workload_boundary.arn
}
