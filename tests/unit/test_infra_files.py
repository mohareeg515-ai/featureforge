"""Static checks of infrastructure/CI files.

These verify syntax and security properties of the *files*. They do NOT prove the
Terraform plan is valid against the AWS provider or that Docker builds; run
`terraform validate` and `docker build` for that (done in CI).
"""

from __future__ import annotations

import re
from pathlib import Path

import hcl2
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
TF = ROOT / "infrastructure" / "terraform"


def _tf() -> dict:
    merged: dict[str, list] = {}
    for f in sorted(TF.glob("*.tf")):
        with f.open() as fh:
            for k, v in hcl2.load(fh).items():
                merged.setdefault(k, []).extend(v)
    return merged


def _resources() -> dict[str, dict]:
    out: dict[str, dict] = {}
    for block in _tf()["resource"]:
        for rtype, named in block.items():
            for name, body in named.items():
                out[f"{rtype.strip(chr(34))}.{name.strip(chr(34))}"] = body
    return out


def test_terraform_declares_required_services():
    types = {k.split(".")[0] for k in _resources()}
    required = {
        "aws_s3_bucket",
        "aws_s3_bucket_public_access_block",
        "aws_ecr_repository",
        "aws_ecs_cluster",
        "aws_ecs_task_definition",
        "aws_scheduler_schedule",
        "aws_cloudwatch_log_group",
        "aws_athena_workgroup",
        "aws_glue_catalog_table",
        "aws_budgets_budget",
        "aws_iam_role",
        "aws_iam_openid_connect_provider",
    }
    assert required <= types


def test_no_expensive_always_on_resources():
    types = {k.split(".")[0] for k in _resources()}
    forbidden = {
        "aws_nat_gateway",
        "aws_db_instance",
        "aws_rds_cluster",
        "aws_instance",
        "aws_elasticache_cluster",
        "aws_lb",
        "aws_kms_key",
        "aws_secretsmanager_secret",
    }
    assert not (forbidden & types)


def test_no_hardcoded_credentials_or_wildcard_actions():
    text = "\n".join(p.read_text() for p in [*TF.glob("*.tf"), *TF.glob("envs/*")])
    assert not re.search(r"AKIA[0-9A-Z]{16}", text)
    assert "aws_access_key" not in text.lower() and "secret_key" not in text.lower()
    assert not re.search(r'actions\s*=\s*\[\s*"\*"\s*\]', text)


def test_bucket_is_private_encrypted_versioned_and_schedule_defaults_disabled():
    res = _resources()
    pab = res["aws_s3_bucket_public_access_block.data"]
    assert all(
        pab[k]
        for k in (
            "block_public_acls",
            "block_public_policy",
            "ignore_public_acls",
            "restrict_public_buckets",
        )
    )
    assert "aws_s3_bucket_versioning.data" in res
    assert "aws_s3_bucket_server_side_encryption_configuration.data" in res
    variables = {k.strip('"'): v for b in _tf()["variable"] for k, v in b.items()}
    assert variables["schedule_enabled"]["default"] is False
    assert variables["force_destroy_bucket"]["default"] is False


def test_single_schedule_without_scheduler_level_retries():
    schedules = [k for k in _resources() if k.startswith("aws_scheduler_schedule")]
    assert schedules == ["aws_scheduler_schedule.daily"]
    target = _resources()["aws_scheduler_schedule.daily"]["target"][0]
    assert target["retry_policy"][0]["maximum_retry_attempts"] == 0


def test_example_tfvars_contain_no_real_identifiers():
    for f in (TF / "envs").glob("*.example"):
        assert "REPLACE_ME" in f.read_text()
    assert not list(TF.glob("envs/*.tfvars"))  # only *.example files are committed


def _wf(name: str) -> dict:
    data = yaml.safe_load((ROOT / ".github" / "workflows" / name).read_text())
    data["on"] = data.pop(True, data.get("on"))  # PyYAML parses the key `on` as True
    return data


def test_ci_has_no_secrets_and_no_oidc():
    ci = _wf("ci.yml")
    assert set(ci["permissions"]) == {"contents"}
    text = (ROOT / ".github/workflows/ci.yml").read_text()
    assert "secrets." not in text and "id-token" not in text
    jobs = set(ci["jobs"])
    assert {
        "lint-and-types",
        "unit-tests",
        "integration-tests",
        "docker-build",
        "terraform-validate",
    } <= jobs


def test_deploy_is_manual_gated_and_uses_oidc_not_keys():
    wf = _wf("deploy.yml")
    assert list(wf["on"]) == ["workflow_dispatch"]  # never push / pull_request
    job = wf["jobs"]["push-image"]
    assert job["environment"] == "production"  # protected environment => manual approval
    assert "refs/heads/main" in job["if"]
    assert wf["permissions"]["id-token"] == "write"
    text = (ROOT / ".github/workflows/deploy.yml").read_text()
    assert "aws-access-key-id" not in text.lower() and "AWS_SECRET" not in text
    commands = "\n".join(step.get("run", "") for j in wf["jobs"].values() for step in j["steps"])
    assert "terraform" not in commands  # CI never applies or destroys infrastructure


@pytest.mark.parametrize("name", ["Dockerfile", "docker/Dockerfile.dashboard"])
def test_dockerfiles_pin_runtime_and_avoid_root(name):
    text = (ROOT / name).read_text()
    assert "python:3.12" in text and "uv==" in text and "--frozen" in text
    if name == "Dockerfile":
        assert "USER featureforge" in text


def test_feature_docs_are_in_sync_with_registry():
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "gen", ROOT / "scripts" / "generate_feature_docs.py"
    )
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    assert (ROOT / "docs" / "feature_definitions.md").read_text() == mod.render()
