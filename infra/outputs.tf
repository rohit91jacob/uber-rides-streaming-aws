output "kinesis_stream_name" {
  description = "Ride-events stream the producer writes to (UBER_KINESIS_STREAM)."
  value       = module.streaming.stream_name
}

output "dedupe_table_name" {
  description = "DynamoDB idempotency table (UBER_DEDUPE_TABLE)."
  value       = module.streaming.dedupe_table_name
}

output "counters_table_name" {
  description = "DynamoDB zone supply/demand counters (UBER_COUNTERS_TABLE)."
  value       = module.streaming.counters_table_name
}

output "dlq_url" {
  description = "SQS dead-letter queue for invalid records (UBER_DLQ_URL)."
  value       = module.streaming.dlq_url
}

output "lake_bucket_name" {
  description = "S3 bucket holding the landing zone and the Iceberg warehouse."
  value       = module.lake.lake_bucket_name
}

output "iceberg_warehouse" {
  description = "UBER_WAREHOUSE for the Glue catalog mode."
  value       = "s3://${module.lake.lake_bucket_name}/warehouse"
}

output "athena_workgroup" {
  description = "Athena workgroup for the dbt athena target (UBER_ATHENA_WORKGROUP)."
  value       = module.lake.athena_workgroup_name
}

output "state_machine_arn" {
  description = "Daily batch Step Functions state machine."
  value       = module.orchestration.state_machine_arn
}

output "batch_ecr_repository_url" {
  description = "Push the batch-step image here."
  value       = module.orchestration.ecr_repository_url
}

output "github_deploy_role_arn" {
  description = "Role the gated deploy workflow assumes through GitHub OIDC (set as repo variable AWS_DEPLOY_ROLE_ARN)."
  value       = module.github_oidc.role_arn
}
