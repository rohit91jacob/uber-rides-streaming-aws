terraform {
  required_version = ">= 1.9.0, < 2.0.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.68"
    }
  }

  # Remote state for a real deployment (S3 native locking, no DynamoDB lock table needed).
  # `terraform init -backend-config=environments/dev.s3.tfbackend`. CI only runs
  # `terraform init -backend=false` + validate; nothing is applied from CI unless the gated
  # deploy workflow is explicitly enabled.
  backend "s3" {}
}
