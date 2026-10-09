output "stream_name" {
  description = "Kinesis stream name."
  value       = aws_kinesis_stream.events.name
}

output "stream_arn" {
  description = "Kinesis stream ARN."
  value       = aws_kinesis_stream.events.arn
}

output "dedupe_table_name" {
  description = "Dedupe table name."
  value       = aws_dynamodb_table.dedupe.name
}

output "counters_table_name" {
  description = "Counters table name."
  value       = aws_dynamodb_table.counters.name
}

output "counters_table_arn" {
  description = "Counters table ARN."
  value       = aws_dynamodb_table.counters.arn
}

output "dlq_url" {
  description = "Invalid-records queue URL."
  value       = aws_sqs_queue.invalid_records.url
}

output "dlq_arn" {
  description = "Invalid-records queue ARN."
  value       = aws_sqs_queue.invalid_records.arn
}

output "alarm_topic_arn" {
  description = "SNS topic for alarms."
  value       = aws_sns_topic.alarms.arn
}

output "consumer_function_name" {
  description = "Consumer Lambda name."
  value       = aws_lambda_function.consumer.function_name
}

output "dlq_name" {
  description = "Invalid-records queue name."
  value       = aws_sqs_queue.invalid_records.name
}
