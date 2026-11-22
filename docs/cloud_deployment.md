# Cloud deployment (AWS)

> **Status: Deployment prepared. Not deployed.** No AWS resources have been created by this
> project. The Terraform, Dockerfile and workflows are written and statically checked, but see
> "What has and has not been validated" below.

## Architecture and ownership

EventBridge Scheduler -> ECS Fargate task (one container, one logical batch) -> S3.
CloudWatch Logs + a failure alarm; Glue table + Athena workgroup over the Parquet snapshots;
AWS Budgets alerts. See the diagram in [architecture.md](architecture.md).

**Who owns scheduling?** EventBridge Scheduler is the *only* schedule in AWS. Prefect runs
**inside** the container (temporary local API) purely for retries, timeouts, task state and
backfill flows; no Prefect deployment/cron is created for the cloud. Do **not** also run
`featureforge serve` against the same bucket.

**How duplicate triggering is contained** (defence in depth): (1) a single schedule resource,
scheduler-level retries disabled (`maximum_retry_attempts = 0`), `task_count = 1`;
(2) idempotent publication - a second run of the same logical batch is `skipped_duplicate`;
(3) the per-snapshot lock (effective inside one container; it is *not* a distributed lock, so two
simultaneous containers rely on (1) and (2)).

**Stateless containers.** On start the CLI pulls `raw/`, `feature_store/` and `metadata/` from
`s3://<bucket>/<prefix>/` and on exit (also after failures) pushes results and reports. Data objects
are uploaded before `_metadata.json`; a partition has a single object name so Athena never sees a
partial file. S3 is not transactional across objects: an interrupted push can leave new data
without new metadata, which the checksum in `featureforge stale` detects.

## IAM (least privilege)

| Role | Can | Cannot |
|---|---|---|
| `task-execution` | pull its ECR image, write its log group | read S3 data |
| `task` | list/get/put objects under `s3://<bucket>/featureforge/*` | delete objects, touch other prefixes, any other service |
| `scheduler` | `ecs:RunTask` on the one task definition/cluster, `iam:PassRole` for the two task roles | anything else |
| `github-deploy` (optional OIDC) | push images to the one ECR repository | create/destroy infrastructure; assumable only from the `production` GitHub Environment |

No credentials exist in code, Terraform or CI. Secrets Manager is intentionally not used: the
pipeline has no secrets (the dataset is public, AWS access comes from IAM roles).

## Prerequisites you must provide

1. An AWS account and a **non-root** admin/deploy identity for yourself (never paste keys into
   chat or commit them). Configure via `aws configure sso` or environment variables locally.
2. A public subnet id and a security group with **no inbound** rules and outbound 443 only
   (the default VPC works). No NAT gateway is created (cost, see `cost_management.md`).
3. Terraform >= 1.6 and Docker locally.

## Deployment steps (each requires your explicit approval; nothing runs automatically)

```bash
cd infrastructure/terraform
cp envs/dev.tfvars.example envs/dev.tfvars       # fill in subnet, SG, email (git-ignored)
terraform init
terraform validate
terraform plan -var-file=envs/dev.tfvars -out=dev.plan   # REVIEW the plan
# only after you have reviewed the plan and the cost notes:
terraform apply dev.plan
```

Then build and push the image (manually, or via the approval-gated workflow):

```bash
aws ecr get-login-password | docker login --username AWS --password-stdin <registry>
docker build -t <ecr_repository_url>:v1 . && docker push <ecr_repository_url>:v1
```

Run once manually before enabling the schedule:

```bash
aws ecs run-task --cluster featureforge-dev --task-definition featureforge-dev-pipeline \
  --launch-type FARGATE --network-configuration \
  "awsvpcConfiguration={subnets=[<subnet>],securityGroups=[<sg>],assignPublicIp=ENABLED}"
```

Check CloudWatch logs and `s3://<bucket>/featureforge/metadata/latest.json`, then set
`schedule_enabled = true` and apply again.

## GitHub Actions with AWS OIDC

1. Apply Terraform with `github_repository = "owner/repo"` to create the OIDC provider and the
   `github-deploy` role (trust restricted to `repo:owner/repo:environment:production`).
2. In GitHub: *Settings -> Environments -> production* -> add **required reviewers**; add
   repository variables `AWS_DEPLOY_ROLE_ARN`, `AWS_REGION`, `ECR_REPOSITORY`.
3. `.github/workflows/deploy.yml` runs only via `workflow_dispatch` on `main`, pauses for
   approval, assumes the role with a short-lived OIDC token and pushes the image. CI
   (`ci.yml`) has read-only permissions, no secrets and no `id-token`, and pull requests cannot
   start the deploy workflow or satisfy the role's trust condition.
4. CI never runs `terraform apply/destroy`: infrastructure changes stay a human, local action.

## Querying with Athena

The Glue table uses **partition projection** (no crawler/`MSCK REPAIR`), range `2024-01-01..NOW`.
Use the workgroup `featureforge-<env>` (10 GB per-query scan cap):

```sql
SELECT snapshot_date, count(*) FROM customer_features
WHERE snapshot_date BETWEEN DATE '2025-04-01' AND DATE '2025-04-30' GROUP BY 1;
```

Always filter on `snapshot_date` to prune partitions.

## Teardown

See `cost_management.md` ("Cleanup"). `force_destroy_bucket=false` (prod) makes destroy fail on a
non-empty bucket on purpose.

## What has and has not been validated

| Item | Status |
|---|---|
| Terraform syntax | parsed with `python-hcl2`; static security assertions in `tests/unit/test_infra_files.py` |
| `terraform validate` / `plan` / `apply` | **not run** (no Terraform binary or provider registry access where this was built). `ci.yml` runs `fmt -check`, `init -backend=false`, `validate` |
| Docker image build | **not run** (no Docker in the build environment). `ci.yml` builds both images |
| GitHub Actions workflows | YAML parsed and security properties asserted; **never executed** |
| S3 sync logic | tested against the **moto emulator**, not real AWS |
| Glue/Athena DDL | **not run** against real Athena (timestamp type compatibility with microsecond Parquet should be checked on first query) |
| ECS/EventBridge behaviour | **not run** |

Cloud status: **Deployment prepared** (not deployed, not attempted, not verified).
