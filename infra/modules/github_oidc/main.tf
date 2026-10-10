terraform {
  required_version = ">= 1.9.0, < 2.0.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.68"
    }
  }
}

# GitHub Actions -> AWS through OIDC federation: no long-lived keys anywhere. Only workflow runs
# on the main branch (or the `aws-dev` environment) of var.repository can assume the role.
resource "aws_iam_openid_connect_provider" "github" {
  count          = var.create_provider ? 1 : 0
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

locals {
  # Workload roles are arn:...:role<path><name>-<role>; the deploy role is outside that path.
  workload_role_prefix = "arn:${var.partition}:iam::${var.account_id}:role${var.workload_role_path}${var.name}-"
  workload_role_arns   = "${local.workload_role_prefix}*"
  provider_arn         = var.create_provider ? aws_iam_openid_connect_provider.github[0].arn : "arn:${var.partition}:iam::${var.account_id}:oidc-provider/token.actions.githubusercontent.com"
}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [local.provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values = [
        "repo:${var.repository}:ref:refs/heads/main",
        "repo:${var.repository}:environment:aws-dev",
      ]
    }
  }
}

resource "aws_iam_role" "deploy" {
  name                 = "${var.name}-github-deploy"
  assume_role_policy   = data.aws_iam_policy_document.assume.json
  max_session_duration = 3600
}

# Deploy permissions are limited to resources carrying this project's tag, plus the read-only
# calls Terraform needs to plan. Tighten further per account policy.
data "aws_iam_policy_document" "deploy" {
  statement {
    sid = "ReadForPlan"
    #checkov:skip=CKV_AWS_111:Describe/List/Get calls Terraform makes during plan are not resource-scopable.
    #checkov:skip=CKV_AWS_356:Same as above (read-only calls).
    actions = [
      "kinesis:Describe*", "kinesis:List*", "dynamodb:Describe*", "dynamodb:List*", "sqs:GetQueueAttributes",
      "sqs:ListQueues", "sqs:ListQueueTags", "s3:GetBucket*", "s3:GetLifecycleConfiguration", "s3:GetEncryptionConfiguration",
      "s3:GetAccelerateConfiguration", "s3:GetReplicationConfiguration", "s3:ListBucket", "lambda:Get*", "lambda:List*",
      "states:Describe*", "states:List*", "scheduler:Get*", "scheduler:List*", "glue:Get*", "athena:GetWorkGroup",
      "athena:ListTagsForResource", "iam:Get*", "iam:List*", "kms:Describe*", "kms:Get*", "kms:List*", "logs:Describe*",
      "logs:ListTagsForResource", "sns:Get*", "sns:List*", "ecr:Describe*", "ecr:GetLifecyclePolicy", "ecr:ListTagsForResource",
      "cloudwatch:Describe*", "cloudwatch:ListTagsForResource", "budgets:ViewBudget", "budgets:ListTagsForResource",
      "signer:GetSigningProfile", "signer:ListTagsForResource", "sts:GetCallerIdentity",
    ]
    resources = ["*"]
  }
  statement {
    sid = "ManageTaggedProjectResources"
    #checkov:skip=CKV_AWS_111:Write access is constrained by the aws:ResourceTag/project condition below.
    #checkov:skip=CKV_AWS_356:Same as above.
    actions = [
      "kinesis:*", "dynamodb:*", "sqs:*", "lambda:*", "states:*", "scheduler:*", "sns:*", "ecr:*",
      "cloudwatch:*", "logs:*", "athena:*",
    ]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/project"
      values   = [var.project_tag]
    }
  }
  statement {
    sid = "CreateWithProjectTag"
    #checkov:skip=CKV_AWS_111:Creation is constrained by the aws:RequestTag/project condition below.
    #checkov:skip=CKV_AWS_356:Same as above.
    actions = [
      "kinesis:CreateStream", "dynamodb:CreateTable", "sqs:CreateQueue", "lambda:CreateFunction", "states:CreateStateMachine",
      "scheduler:CreateSchedule", "sns:CreateTopic", "ecr:CreateRepository", "logs:CreateLogGroup", "athena:CreateWorkGroup",
      "lambda:CreateEventSourceMapping", "lambda:CreateCodeSigningConfig", "signer:PutSigningProfile", "budgets:ModifyBudget",
    ]
    resources = ["*"]
    condition {
      test     = "StringEquals"
      variable = "aws:RequestTag/project"
      values   = [var.project_tag]
    }
  }
  statement {
    sid     = "ProjectBucketsAndCatalog"
    actions = ["s3:*", "glue:*Database*", "glue:*Table*", "glue:PutDataCatalogEncryptionSettings"]
    resources = [
      "arn:${var.partition}:s3:::uber-rides-*",
      "arn:${var.partition}:s3:::uber-rides-*/*",
      "arn:${var.partition}:glue:*:${var.account_id}:catalog",
      "arn:${var.partition}:glue:*:${var.account_id}:database/uber_*",
      "arn:${var.partition}:glue:*:${var.account_id}:table/uber_*/*",
    ]
  }
  # Workload roles: only under the project's IAM path and name prefix, and only while they carry
  # the workload permissions boundary. A role's effective permissions are capped by the boundary,
  # so inline policies the deploy role writes cannot grant more than the boundary allows.
  statement {
    sid = "WorkloadRolePermissionsRequireBoundary"
    actions = [
      "iam:CreateRole", "iam:PutRolePolicy", "iam:AttachRolePolicy", "iam:PutRolePermissionsBoundary",
      "iam:DeleteRolePolicy", "iam:DetachRolePolicy",
    ]
    resources = [local.workload_role_arns]
    condition {
      test     = "StringEquals"
      variable = "iam:PermissionsBoundary"
      values   = [var.permissions_boundary_arn]
    }
  }
  statement {
    sid = "WorkloadRoleLifecycle"
    actions = [
      "iam:UpdateRole", "iam:UpdateRoleDescription", "iam:UpdateAssumeRolePolicy", "iam:DeleteRole",
      "iam:TagRole", "iam:UntagRole",
    ]
    resources = [local.workload_role_arns]
  }
  # PassRole only for the workload roles, and only to the service each role is for.
  statement {
    sid       = "PassLambdaRoles"
    actions   = ["iam:PassRole"]
    resources = [for r in ["consumer", "batch-step"] : "${local.workload_role_prefix}${r}"]
    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["lambda.amazonaws.com"]
    }
  }
  statement {
    sid       = "PassStatesRole"
    actions   = ["iam:PassRole"]
    resources = ["${local.workload_role_prefix}daily-batch-sfn"]
    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["states.amazonaws.com"]
    }
  }
  statement {
    sid       = "PassSchedulerRole"
    actions   = ["iam:PassRole"]
    resources = ["${local.workload_role_prefix}daily-batch-scheduler"]
    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["scheduler.amazonaws.com"]
    }
  }
  statement {
    sid       = "TerraformState"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = ["arn:${var.partition}:s3:::uber-rides-tfstate-*", "arn:${var.partition}:s3:::uber-rides-tfstate-*/*"]
  }
}

resource "aws_iam_role_policy" "deploy" {
  name   = "deploy"
  role   = aws_iam_role.deploy.id
  policy = data.aws_iam_policy_document.deploy.json
}
