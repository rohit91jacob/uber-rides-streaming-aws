variable "name" {
  description = "Name prefix."
  type        = string
}

variable "kms_key_arn" {
  description = "Customer-managed KMS key."
  type        = string
}

variable "image_uri" {
  description = "Batch-step container image URI (empty = <repository>:bootstrap)."
  type        = string
}

variable "asl_template_path" {
  description = "Path to the ASL definition shared with the local runner."
  type        = string
}

variable "schedule_expression" {
  description = "EventBridge Scheduler expression."
  type        = string
}

variable "schedule_timezone" {
  description = "Schedule time zone."
  type        = string
}

variable "lake_bucket_name" {
  description = "Lake bucket name."
  type        = string
}

variable "lake_bucket_arn" {
  description = "Lake bucket ARN."
  type        = string
}

variable "athena_workgroup" {
  description = "Athena workgroup used by dbt."
  type        = string
}

variable "athena_results_bucket_arn" {
  description = "ARN of the bucket Athena writes query results to."
  type        = string
}

variable "glue_database_arns" {
  description = "Glue database ARNs the batch may read and write."
  type        = list(string)
}

variable "counters_table_arn" {
  description = "Counters table ARN (exported by the batch)."
  type        = string
}

variable "dlq_arn" {
  description = "Invalid-records queue ARN (drained into Iceberg by the batch)."
  type        = string
}

variable "alarm_topic_arn" {
  description = "SNS topic for the failure alarm."
  type        = string
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention."
  type        = number
}

variable "region" {
  description = "AWS region."
  type        = string
}

variable "account_id" {
  description = "AWS account id."
  type        = string
}

variable "partition" {
  description = "AWS partition."
  type        = string
}

variable "counters_table_name" {
  description = "Counters table name (UBER_COUNTERS_TABLE for the batch export)."
  type        = string
}

variable "dlq_name" {
  description = "Invalid-records queue name (UBER_DLQ_QUEUE for the batch drain)."
  type        = string
}

variable "role_path" {
  description = "IAM path for the workload roles; the deploy role may only manage roles under it."
  type        = string
}

variable "permissions_boundary_arn" {
  description = "Permissions boundary attached to every workload role."
  type        = string
}
