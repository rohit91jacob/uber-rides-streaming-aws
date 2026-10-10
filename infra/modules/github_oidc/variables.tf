variable "name" {
  description = "Name prefix."
  type        = string
}

variable "repository" {
  description = "GitHub owner/repo allowed to assume the role."
  type        = string
}

variable "create_provider" {
  description = "Create the GitHub OIDC identity provider in this account."
  type        = bool
}

variable "account_id" {
  description = "AWS account id."
  type        = string
}

variable "partition" {
  description = "AWS partition."
  type        = string
}

variable "project_tag" {
  description = "Project tag value that scopes the deploy permissions."
  type        = string
}

variable "workload_role_path" {
  description = "IAM path of the workload roles the deploy role may manage (e.g. /uber-rides-dev/)."
  type        = string

  validation {
    condition     = can(regex("^/[A-Za-z0-9+=,.@_-]+/$", var.workload_role_path))
    error_message = "workload_role_path must be a non-root IAM path such as /uber-rides-dev/."
  }
}

variable "permissions_boundary_arn" {
  description = "Permissions boundary every workload role must carry."
  type        = string
}
