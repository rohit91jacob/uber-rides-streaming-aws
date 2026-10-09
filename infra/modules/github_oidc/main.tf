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
  provider_arn = var.create_provider ? aws_iam_openid_connect_provider.github[0].arn : "arn:${var.partition}:iam::${var.account_id}:oidc-provider/token.actions.githubusercontent.com"
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
    sid     = "ProjectBucketsAndRoles"
    actions = ["s3:*", "iam:*Role*", "iam:*RolePolicy", "iam:PassRole", "glue:*Database*", "glue:*Table*", "glue:PutDataCatalogEncryptionSettings"]
    resources = [
      "arn:${var.partition}:s3:::uber-rides-*",
      "arn:${var.partition}:s3:::uber-rides-*/*",
      "arn:${var.partition}:iam::${var.account_id}:role/uber-rides-*",
      "arn:${var.partition}:glue:*:${var.account_id}:catalog",
      "arn:${var.partition}:glue:*:${var.account_id}:database/uber_*",
      "arn:${var.partition}:glue:*:${var.account_id}:table/uber_*/*",
    ]
  }
  statement {
    sid       = "UseProjectKey"
    actions   = ["kms:Encrypt", "kms:Decrypt", "kms:GenerateDataKey*", "kms:CreateGrant", "kms:DescribeKey"]
    resources = ["arn:${var.partition}:kms:*:${var.account_id}:key/*"]
    condition {
      test     = "StringEquals"
      variable = "aws:ResourceTag/project"
      values   = [var.project_tag]
    }
  }
  # The deploy role's own name matches role/uber-rides-*, so without this it could rewrite its
  # own policy or trust and escalate. Changes to this role are applied by an account admin.
  # Managed-policy attachment is denied everywhere: every role in this stack uses inline policies.
  statement {
    sid    = "NoSelfModificationOrManagedPolicies"
    effect = "Deny"
    actions = [
      "iam:AttachRolePolicy", "iam:PutRolePolicy", "iam:DeleteRolePolicy", "iam:DetachRolePolicy",
      "iam:UpdateAssumeRolePolicy", "iam:PutRolePermissionsBoundary", "iam:DeleteRolePermissionsBoundary",
      "iam:UpdateRole", "iam:DeleteRole", "iam:TagRole", "iam:UntagRole",
    ]
    resources = ["arn:${var.partition}:iam::${var.account_id}:role/${var.name}-github-deploy"]
  }
  statement {
    sid       = "NoManagedPolicyAttachment"
    effect    = "Deny"
    actions   = ["iam:AttachRolePolicy"]
    resources = ["*"]
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
