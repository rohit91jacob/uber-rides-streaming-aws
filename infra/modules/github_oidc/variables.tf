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
