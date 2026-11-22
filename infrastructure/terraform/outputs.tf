output "data_bucket" {
  value = aws_s3_bucket.data.bucket
}

output "ecr_repository_url" {
  value = aws_ecr_repository.app.repository_url
}

output "ecs_cluster" {
  value = aws_ecs_cluster.main.name
}

output "task_definition" {
  value = aws_ecs_task_definition.pipeline.family
}

output "athena_workgroup" {
  value = aws_athena_workgroup.main.name
}

output "glue_database" {
  value = aws_glue_catalog_database.main.name
}

output "github_deploy_role_arn" {
  value = try(aws_iam_role.github_deploy[0].arn, null)
}
