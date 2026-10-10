output "lake_bucket_name" {
  description = "Bucket for the landing zone and the Iceberg warehouse."
  value       = module.lake_bucket.name
}

output "lake_bucket_arn" {
  description = "ARN of the lake bucket."
  value       = module.lake_bucket.arn
}

output "athena_results_bucket_name" {
  description = "Bucket for Athena query results."
  value       = module.athena_results_bucket.name
}

output "athena_results_bucket_arn" {
  description = "ARN of the Athena query results bucket."
  value       = module.athena_results_bucket.arn
}

output "athena_workgroup_name" {
  description = "Athena workgroup name."
  value       = aws_athena_workgroup.this.name
}

output "glue_database_arns" {
  description = "ARNs of the Glue databases (lake, ops, staging, intermediate, marts)."
  value       = [for db in aws_glue_catalog_database.this : db.arn]
}
