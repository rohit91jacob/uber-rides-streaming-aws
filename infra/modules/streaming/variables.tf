variable "name" {
  description = "Name prefix."
  type        = string
}

variable "kms_key_arn" {
  description = "Customer-managed KMS key."
  type        = string
}

variable "shard_count" {
  description = "Kinesis shards."
  type        = number
}

variable "retention_hours" {
  description = "Kinesis retention in hours."
  type        = number
}

variable "batch_size" {
  description = "Event source mapping batch size."
  type        = number
}

variable "reserved_concurrency" {
  description = "Consumer reserved concurrency."
  type        = number
}

variable "counter_window_minutes" {
  description = "Counter window width (minutes)."
  type        = number
}

variable "landing_bucket_name" {
  description = "Bucket the consumer lands events in."
  type        = string
}

variable "landing_bucket_arn" {
  description = "ARN of the landing bucket."
  type        = string
}

variable "source_dir" {
  description = "Path to src/uber_rides (packaged into the consumer zip)."
  type        = string
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention."
  type        = number
}

variable "alarm_email" {
  description = "Optional e-mail subscribed to the alarm topic."
  type        = string
}
