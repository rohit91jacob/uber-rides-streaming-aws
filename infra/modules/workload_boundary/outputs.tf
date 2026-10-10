output "arn" {
  description = "ARN of the workload permissions boundary policy."
  value       = aws_iam_policy.boundary.arn
}
