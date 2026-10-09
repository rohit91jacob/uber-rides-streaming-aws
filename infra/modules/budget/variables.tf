variable "name" {
  description = "Name prefix."
  type        = string
}

variable "project_tag" {
  description = "Value of the project cost-allocation tag the budget filters on."
  type        = string
}

variable "limit_usd" {
  description = "Monthly limit in USD."
  type        = number
}

variable "alert_emails" {
  description = "E-mail subscribers."
  type        = list(string)
}

variable "alarm_topic_arn" {
  description = "SNS topic that also receives budget alerts."
  type        = string
}

variable "create_sns_alert" {
  description = "Send budget alerts to the SNS topic as well."
  type        = bool
}
