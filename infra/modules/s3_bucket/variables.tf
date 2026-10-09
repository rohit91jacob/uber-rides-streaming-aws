variable "bucket_name" {
  description = "Globally unique bucket name."
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key for default encryption."
  type        = string
}

variable "access_log_bucket" {
  description = "Bucket receiving S3 server access logs."
  type        = string
}

variable "noncurrent_version_days" {
  description = "Days to keep noncurrent object versions."
  type        = number
  default     = 7
}

variable "prefix_rules" {
  description = "Extra lifecycle rules per prefix (optional Standard-IA transition, then expiry)."
  type = list(object({
    id                = string
    prefix            = string
    ia_after_days     = optional(number)
    expire_after_days = number
  }))
  default = []
}
