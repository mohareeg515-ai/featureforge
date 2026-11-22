# Cost management

I could not look up live AWS pricing from the build environment, so **no prices are quoted
here**. Never assume a service is free: check the AWS pricing pages for your region before
applying. Cloud deployment is optional; everything runs locally for free.

## Cost drivers in this design

| Service | What drives the bill | Mitigation in the code |
|---|---|---|
| ECS Fargate | vCPU-seconds + GB-seconds while a task runs (short daily run) | scheduled task, no always-on service; 0.5 vCPU / 2 GB default |
| Public IPv4 on the task | AWS charges for public IPv4 addresses while attached | exists only while the task runs |
| **NAT Gateway (not created)** | hourly + per-GB; typically the largest surprise | tasks use a public subnet with `assign_public_ip`; security group blocks inbound |
| S3 | storage, requests, versioning of overwritten objects | tiny Parquet files; noncurrent versions expire after 30 days; optional raw expiry |
| CloudWatch Logs | ingestion + storage | 7-30 day retention, `PREFECT_LOGGING_LEVEL=WARNING` |
| ECR | image storage | lifecycle keeps the last 10 images |
| Athena | data scanned | workgroup scan cap (10 GB/query), results expire after 7 days, partition projection |
| Glue Data Catalog | objects/requests beyond the free tier | one database, one table |
| AWS Budgets | possibly beyond the free budgets per account | one budget per environment |
| EventBridge Scheduler | invocations (very low volume) | one schedule |

Not provisioned, on purpose: NAT Gateway, RDS/PostgreSQL, EC2, load balancers, KMS keys,
Secrets Manager, always-on Prefect server. Adding any of them needs an explicit decision.

## Recommended guardrails

1. Create the AWS account budget **before** `terraform apply`; the module also creates one
   (`monthly_budget_usd`, alerts at 80% actual and 100% forecast).
2. Keep `schedule_enabled = false` until a manual run has succeeded.
3. Use a separate dev account/region from any production-style environment.
4. Review the first month in Cost Explorer filtered by tag `Project=featureforge`.

## Cleanup (Terraform destroy guide)

Destroying deletes infrastructure and possibly data. Do it only deliberately.

```bash
cd infrastructure/terraform
# 1. stop spend immediately without deleting anything
terraform apply -var-file=envs/dev.tfvars -var schedule_enabled=false
# 2. (optional) copy data you want to keep
aws s3 sync s3://<bucket>/featureforge ./backup
# 3. review what would be destroyed
terraform plan -destroy -var-file=envs/dev.tfvars
# 4. destroy (type 'yes' only after reading the plan)
terraform destroy -var-file=envs/dev.tfvars
```

* dev uses `force_destroy_bucket = true` and ECR `force_delete`, so destroy removes the data.
* prod-style config keeps `force_destroy_bucket = false`: destroy fails until you empty the
  bucket yourself. Versioned buckets must have all versions deleted.
* Delete the GitHub OIDC provider/role by leaving `github_repository = ""` and re-applying.
* Afterwards verify in the console: no ECS tasks/clusters, S3 bucket, ECR repo, log groups,
  Glue database, Athena workgroup, budgets (if unwanted).
