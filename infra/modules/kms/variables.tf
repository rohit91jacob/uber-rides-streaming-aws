variable "name" {
  description = "Name prefix."
  type        = string
}

variable "account_id" {
  description = "AWS account id that owns the key."
  type        = string
}

variable "region" {
  description = "Region (for the CloudWatch Logs service principal)."
  type        = string
}

variable "partition" {
  description = "AWS partition (aws, aws-us-gov, aws-cn)."
  type        = string
}
