output "state_machine_arn" {
  description = "Daily batch state machine ARN."
  value       = aws_sfn_state_machine.daily_batch.arn
}

output "ecr_repository_url" {
  description = "ECR repository for the batch-step image."
  value       = aws_ecr_repository.batch.repository_url
}

output "batch_function_name" {
  description = "Batch-step Lambda name."
  value       = aws_lambda_function.batch.function_name
}
