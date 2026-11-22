variable "aws_region" {
  type    = string
  default = "eu-central-1"
}

variable "environment" {
  type        = string
  description = "dev or prod; used in resource names"
  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be dev or prod."
  }
}

variable "subnet_ids" {
  type        = list(string)
  description = "PUBLIC subnets for the Fargate task (assign_public_ip=true). No NAT gateway is created."
}

variable "security_group_ids" {
  type        = list(string)
  description = "Security groups for the task: allow NO inbound, outbound 443 only."
}

variable "schedule_expression" {
  type        = string
  default     = "cron(0 2 * * ? *)"
  description = "EventBridge Scheduler expression (UTC). The single authoritative schedule."
}

variable "schedule_enabled" {
  type        = bool
  default     = false
  description = "Keep false until the first manual run succeeded."
}

variable "task_cpu" {
  type    = number
  default = 512
}

variable "task_memory" {
  type    = number
  default = 2048
}

variable "container_image_tag" {
  type    = string
  default = "latest"
}

variable "log_retention_days" {
  type    = number
  default = 14
}

variable "raw_retention_days" {
  type        = number
  default     = 0
  description = "0 = keep raw data forever; >0 expires raw objects after N days."
}

variable "monthly_budget_usd" {
  type    = number
  default = 10
}

variable "budget_alert_email" {
  type        = string
  description = "Email that receives AWS Budgets alerts."
}

variable "github_repository" {
  type        = string
  default     = ""
  description = "owner/repo allowed to assume the deploy role via OIDC. Empty = do not create."
}

variable "force_destroy_bucket" {
  type        = bool
  default     = false
  description = "Allow `terraform destroy` to delete a non-empty data bucket. Leave false in prod."
}
