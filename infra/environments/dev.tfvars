# Example values for a dev deployment. Nothing in this repository has been applied to a real
# AWS account; CI only validates. Copy and adjust before `terraform plan -var-file=...`.
region                        = "us-east-1"
environment                   = "dev"
kinesis_shard_count           = 1
consumer_reserved_concurrency = 2
monthly_budget_usd            = 25
budget_alert_emails           = []
alarm_email                   = ""
github_repository             = "rohit91jacob/uber-rides-streaming-aws"
