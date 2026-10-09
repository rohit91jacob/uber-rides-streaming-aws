variable "name" {
  description = "Name prefix."
  type        = string
}

variable "kms_key_arn" {
  description = "Customer-managed KMS key for bucket, catalog and Athena result encryption."
  type        = string
}

variable "athena_bytes_scanned_cutoff" {
  description = "Per-query bytes-scanned limit for the Athena workgroup."
  type        = number
}
