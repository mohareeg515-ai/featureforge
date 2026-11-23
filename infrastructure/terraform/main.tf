locals {
  name        = "featureforge-${var.environment}"
  bucket_name = "${local.name}-data-${data.aws_caller_identity.current.account_id}"
  s3_prefix   = "featureforge"
}

# ------------------------------------------------------------------ storage (S3)
resource "aws_s3_bucket" "data" {
  bucket        = local.bucket_name
  force_destroy = var.force_destroy_bucket
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = "athena-results/"
    }
    expiration {
      days = 7
    }
  }

  dynamic "rule" {
    for_each = var.raw_retention_days > 0 ? [1] : []
    content {
      id     = "expire-raw"
      status = "Enabled"
      filter {
        prefix = "${local.s3_prefix}/raw/"
      }
      expiration {
        days = var.raw_retention_days
      }
    }
  }
}

# ------------------------------------------------------------------ container registry
resource "aws_ecr_repository" "app" {
  name                 = local.name
  image_tag_mutability = "MUTABLE"
  force_delete         = var.environment == "dev"
  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "app" {
  repository = aws_ecr_repository.app.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "keep the last 10 images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 10 }
      action       = { type = "expire" }
    }]
  })
}

# ------------------------------------------------------------------ logging
resource "aws_cloudwatch_log_group" "app" {
  name              = "/featureforge/${var.environment}"
  retention_in_days = var.log_retention_days
}

# ------------------------------------------------------------------ IAM (least privilege)
data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

# Execution role: lets ECS pull the image and write container logs. Nothing else.
resource "aws_iam_role" "task_execution" {
  name               = "${local.name}-task-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

data "aws_iam_policy_document" "task_execution" {
  statement {
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }
  statement {
    actions   = ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"]
    resources = [aws_ecr_repository.app.arn]
  }
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.app.arn}:*"]
  }
}

resource "aws_iam_role_policy" "task_execution" {
  role   = aws_iam_role.task_execution.id
  policy = data.aws_iam_policy_document.task_execution.json
}

# Task role: what the application itself may do, restricted to its own S3 prefix.
resource "aws_iam_role" "task" {
  name               = "${local.name}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

data "aws_iam_policy_document" "task" {
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.data.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${local.s3_prefix}/*", "${local.s3_prefix}"]
    }
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${aws_s3_bucket.data.arn}/${local.s3_prefix}/*"]
  }
}

resource "aws_iam_role_policy" "task" {
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task.json
}

# ------------------------------------------------------------------ compute (ECS Fargate)
resource "aws_ecs_cluster" "main" {
  name = local.name
}

resource "aws_ecs_task_definition" "pipeline" {
  family                   = "${local.name}-pipeline"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.task_cpu
  memory                   = var.task_memory
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task.arn

  container_definitions = jsonencode([{
    name      = "pipeline"
    image     = "${aws_ecr_repository.app.repository_url}:${var.container_image_tag}"
    essential = true
    # `run` without --date uses today's UTC date: one snapshot per scheduled invocation.
    command = ["run"]
    environment = [
      { name = "FF_ENV", value = var.environment },
      { name = "FF_STORAGE_BACKEND", value = "s3" },
      { name = "FF_S3_BUCKET", value = aws_s3_bucket.data.bucket },
      { name = "FF_S3_PREFIX", value = local.s3_prefix },
      { name = "FF_DATA_ROOT", value = "/tmp/featureforge" },
      # Prefect runs in-process (temporary local API); EventBridge owns the schedule.
      { name = "PREFECT_LOGGING_LEVEL", value = "WARNING" },
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.app.name
        awslogs-region        = var.aws_region
        awslogs-stream-prefix = "pipeline"
      }
    }
  }])
}

# ------------------------------------------------------------------ schedule (single owner)
data "aws_iam_policy_document" "scheduler_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${local.name}-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume.json
}

data "aws_iam_policy_document" "scheduler" {
  statement {
    actions   = ["ecs:RunTask"]
    resources = [aws_ecs_task_definition.pipeline.arn]
    condition {
      test     = "ArnEquals"
      variable = "ecs:cluster"
      values   = [aws_ecs_cluster.main.arn]
    }
  }
  statement {
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.task.arn, aws_iam_role.task_execution.arn]
  }
}

resource "aws_iam_role_policy" "scheduler" {
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler.json
}

resource "aws_scheduler_schedule" "daily" {
  name                = "${local.name}-daily"
  state               = var.schedule_enabled ? "ENABLED" : "DISABLED"
  schedule_expression = var.schedule_expression

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_ecs_cluster.main.arn
    role_arn = aws_iam_role.scheduler.arn

    ecs_parameters {
      task_definition_arn = aws_ecs_task_definition.pipeline.arn
      launch_type         = "FARGATE"
      task_count          = 1
      network_configuration {
        subnets          = var.subnet_ids
        security_groups  = var.security_group_ids
        assign_public_ip = true # avoids a NAT gateway (~$30+/month); SG blocks all inbound
      }
    }

    retry_policy {
      maximum_retry_attempts       = 0 # retries are Prefect's job; avoid double execution
      maximum_event_age_in_seconds = 3600
    }
  }
}

# ------------------------------------------------------------------ alarms
resource "aws_cloudwatch_log_metric_filter" "run_failed" {
  name           = "${local.name}-run-failed"
  log_group_name = aws_cloudwatch_log_group.app.name
  pattern        = "{ $.message = \"run failed\" }"

  metric_transformation {
    name      = "RunFailed"
    namespace = "FeatureForge/${var.environment}"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "run_failed" {
  alarm_name          = "${local.name}-run-failed"
  namespace           = "FeatureForge/${var.environment}"
  metric_name         = "RunFailed"
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  treat_missing_data  = "notBreaching"
}

# ------------------------------------------------------------------ Athena + Glue
resource "aws_glue_catalog_database" "main" {
  name = replace("${local.name}_db", "-", "_")
}

resource "aws_glue_catalog_table" "customer_features" {
  name          = "customer_features"
  database_name = aws_glue_catalog_database.main.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification                    = "parquet"
    "projection.enabled"              = "true"
    "projection.snapshot_date.type"   = "date"
    "projection.snapshot_date.range"  = "2024-01-01,NOW"
    "projection.snapshot_date.format" = "yyyy-MM-dd"
    "storage.location.template"       = "s3://${aws_s3_bucket.data.bucket}/${local.s3_prefix}/feature_store/customer_features/snapshot_date=$${snapshot_date}"
  }

  partition_keys {
    name = "snapshot_date"
    type = "date"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data.bucket}/${local.s3_prefix}/feature_store/customer_features/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "customer_id"
      type = "string"
    }
    columns {
      name = "snapshot_ts"
      type = "timestamp"
    }
    columns {
      name = "purchases_7d"
      type = "bigint"
    }
    columns {
      name = "purchases_30d"
      type = "bigint"
    }
    columns {
      name = "purchases_90d"
      type = "bigint"
    }
    columns {
      name = "total_spend_30d"
      type = "double"
    }
    columns {
      name = "total_spend_90d"
      type = "double"
    }
    columns {
      name = "avg_order_value"
      type = "double"
    }
    columns {
      name = "days_since_last_purchase"
      type = "int"
    }
    columns {
      name = "unique_products_30d"
      type = "bigint"
    }
    columns {
      name = "purchase_frequency"
      type = "double"
    }
    columns {
      name = "customer_lifetime_value_proxy"
      type = "double"
    }
    columns {
      name = "sessions_7d"
      type = "bigint"
    }
    columns {
      name = "cart_events_7d"
      type = "bigint"
    }
  }
}

resource "aws_athena_workgroup" "main" {
  name          = local.name
  force_destroy = true
  configuration {
    enforce_workgroup_configuration    = true
    bytes_scanned_cutoff_per_query     = 10737418240 # 10 GB hard cap per query
    publish_cloudwatch_metrics_enabled = true
    result_configuration {
      output_location = "s3://${aws_s3_bucket.data.bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# ------------------------------------------------------------------ cost guardrail
resource "aws_budgets_budget" "monthly" {
  name         = "${local.name}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_alert_email]
  }
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.budget_alert_email]
  }
}

# ------------------------------------------------------------------ GitHub OIDC (optional)
resource "aws_iam_openid_connect_provider" "github" {
  count          = var.github_repository == "" ? 0 : 1
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

data "aws_iam_policy_document" "github_assume" {
  count = var.github_repository == "" ? 0 : 1
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github[0].arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    # Only the `production` GitHub Environment (manual approval) can assume this role.
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:${var.github_repository}:environment:production"]
    }
  }
}

resource "aws_iam_role" "github_deploy" {
  count              = var.github_repository == "" ? 0 : 1
  name               = "${local.name}-github-deploy"
  assume_role_policy = data.aws_iam_policy_document.github_assume[0].json
}

# Image push only. Infrastructure changes (terraform apply/destroy) are run by a human with
# their own credentials; CI never receives permission to create or destroy resources.
data "aws_iam_policy_document" "github_deploy" {
  count = var.github_repository == "" ? 0 : 1
  statement {
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }
  statement {
    actions = [
      "ecr:BatchCheckLayerAvailability", "ecr:InitiateLayerUpload", "ecr:UploadLayerPart",
      "ecr:CompleteLayerUpload", "ecr:PutImage", "ecr:BatchGetImage",
    ]
    resources = [aws_ecr_repository.app.arn]
  }
}

resource "aws_iam_role_policy" "github_deploy" {
  count  = var.github_repository == "" ? 0 : 1
  role   = aws_iam_role.github_deploy[0].id
  policy = data.aws_iam_policy_document.github_deploy[0].json
}
