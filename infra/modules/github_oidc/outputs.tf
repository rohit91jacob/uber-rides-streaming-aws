output "role_arn" {
  description = "Role assumed by the gated deploy workflow."
  value       = aws_iam_role.deploy.arn
}
