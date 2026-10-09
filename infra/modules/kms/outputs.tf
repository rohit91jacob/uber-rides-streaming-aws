output "key_arn" {
  description = "ARN of the customer-managed key."
  value       = aws_kms_key.this.arn
}

output "alias_name" {
  description = "Alias of the key."
  value       = aws_kms_alias.this.name
}
