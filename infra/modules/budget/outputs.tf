output "budget_name" {
  description = "Name of the monthly budget."
  value       = aws_budgets_budget.monthly.name
}
