provider "aws" {
  region = var.region

  # Every resource carries these tags; the budget filters on `project`.
  default_tags {
    tags = local.tags
  }
}
