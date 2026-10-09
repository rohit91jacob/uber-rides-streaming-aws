variable "region" {
  description = "AWS region for every resource."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment name (dev, prod). Used in resource names and tags."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be dev or prod."
  }
}

variable "owner" {
  description = "Owner tag value."
  type        = string
  default     = "rohit91jacob"
}

variable "kinesis_shard_count" {
  description = "Provisioned shards for the ride-events stream (1 shard = 1 MB/s or 1,000 records/s in)."
  type        = number
  default     = 2

  validation {
    condition     = var.kinesis_shard_count >= 1 && var.kinesis_shard_count <= 50
    error_message = "kinesis_shard_count must be between 1 and 50."
  }
}

variable "kinesis_retention_hours" {
  description = "Stream retention. 24 h is included in the shard price; longer costs extra."
  type        = number
  default     = 24
}

variable "consumer_batch_size" {
  description = "Max records per consumer Lambda invocation."
  type        = number
  default     = 500
}

variable "consumer_reserved_concurrency" {
  description = "Reserved concurrency for the consumer (caps cost; >= shard count x parallelization factor)."
  type        = number
  default     = 4
}

variable "counter_window_minutes" {
  description = "Width of the real-time zone supply/demand windows."
  type        = number
  default     = 15
}

variable "batch_image_uri" {
  description = "ECR image URI for the batch-step Lambda (built from the repo Dockerfile). Empty = placeholder tag in the created repository."
  type        = string
  default     = ""
}

variable "batch_schedule_expression" {
  description = "EventBridge Scheduler expression for the daily batch."
  type        = string
  default     = "cron(30 6 * * ? *)"
}

variable "batch_schedule_timezone" {
  description = "Time zone for the batch schedule."
  type        = string
  default     = "America/New_York"
}

variable "athena_bytes_scanned_cutoff" {
  description = "Per-query data-scanned limit for the Athena workgroup (bytes). Default 10 GB."
  type        = number
  default     = 10737418240
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention."
  type        = number
  default     = 365
}

variable "monthly_budget_usd" {
  description = "Monthly cost budget for resources tagged project=uber-rides-streaming-aws."
  type        = number
  default     = 50
}

variable "budget_alert_emails" {
  description = "Addresses notified at 80% forecast / 100% actual of the budget."
  type        = list(string)
  default     = []
}

variable "alarm_email" {
  description = "Optional e-mail subscribed to the alarms topic (empty = no subscription)."
  type        = string
  default     = ""
}

variable "github_repository" {
  description = "owner/repo allowed to assume the deploy role through GitHub OIDC."
  type        = string
  default     = "rohit91jacob/uber-rides-streaming-aws"
}

variable "create_github_oidc_provider" {
  description = "Create the account's GitHub OIDC provider (false if it already exists)."
  type        = bool
  default     = true
}
