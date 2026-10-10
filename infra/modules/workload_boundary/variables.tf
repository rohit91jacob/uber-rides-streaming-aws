variable "name" {
  description = "Name prefix shared by every workload resource (e.g. uber-rides-dev)."
  type        = string
}

variable "path" {
  description = "IAM path for the boundary policy and the workload roles (e.g. /uber-rides-dev/)."
  type        = string
}

variable "kms_key_arn" {
  description = "ARN of the project's customer-managed key."
  type        = string
}

variable "glue_database_arns" {
  description = "ARNs of the project's Glue databases."
  type        = list(string)
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
