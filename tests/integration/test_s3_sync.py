"""S3 state sync, tested against the moto *emulator* (not real AWS)."""

from __future__ import annotations

from datetime import date

import boto3
import pytest
from moto import mock_aws

from featureforge.config.layout import Layout
from featureforge.config.settings import load_settings
from featureforge.orchestration.pipeline import run_snapshot
from featureforge.storage.feature_store import FeatureStore
from featureforge.storage.s3 import S3StateSync, state_sync

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _aws_env(monkeypatch):
    for k, v in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_DEFAULT_REGION": "us-east-1",
    }.items():
        monkeypatch.setenv(k, v)


def _cfg(root, **kw):
    return load_settings(
        data_root=root,
        synthetic_customers=40,
        seed=3,
        storage_backend="s3",
        s3_bucket="ff-test",
        s3_prefix="env/dev",
        **kw,
    )


@mock_aws
def test_state_survives_between_stateless_runs(tmp_path):
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="ff-test")
    d = date(2025, 3, 1)

    first = _cfg(tmp_path / "run1")
    with state_sync(first, client=client):  # empty bucket -> pull nothing
        rec1 = run_snapshot(first, d)
    assert rec1.status == "success"
    keys = [o["Key"] for o in client.list_objects_v2(Bucket="ff-test")["Contents"]]
    assert "env/dev/feature_store/customer_features/snapshot_date=2025-03-01/part-0.parquet" in keys
    assert "env/dev/feature_store/customer_features/snapshot_date=2025-03-01/_metadata.json" in keys
    assert any(k.startswith("env/dev/raw/source=transactions/") for k in keys)
    assert not any("locks" in k or "/." in k for k in keys)  # no lock/staging debris

    second = _cfg(tmp_path / "run2")  # brand-new empty disk, as in a fresh Fargate task
    with state_sync(second, client=client):
        rec2 = run_snapshot(second, d)
        assert rec2.status == "skipped_duplicate"  # idempotency state restored from S3
        rec3 = run_snapshot(second, date(2025, 3, 8))
    assert rec3.status == "success"
    third = _cfg(tmp_path / "run3")
    with state_sync(third, client=client):
        store = FeatureStore(Layout(third.data_root).feature_store)
        assert store.list_snapshots() == [d, date(2025, 3, 8)] and store.verify(d)


@mock_aws
def test_data_uploaded_before_metadata_and_failures_still_push_diagnostics(tmp_path):
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket="ff-test")
    s = _cfg(tmp_path / "r")
    with pytest.raises(RuntimeError), state_sync(s, client=client):
        run_snapshot(s, date(2025, 3, 1))
        raise RuntimeError("boom after a successful run")
    keys = [o["Key"] for o in client.list_objects_v2(Bucket="ff-test")["Contents"]]
    assert any("/reports/quality/" in k for k in keys)  # push happens in `finally`

    sync = S3StateSync(client, "ff-test", "x")
    uploads: list[str] = []
    real = client.upload_file
    client.upload_file = lambda f, b, k, *a, **kw: (uploads.append(k), real(f, b, k, *a, **kw))[1]
    sync.push(s.data_root, dirs=("feature_store",))
    parquet_i = max(i for i, k in enumerate(uploads) if k.endswith(".parquet"))
    meta_i = min(i for i, k in enumerate(uploads) if k.endswith("_metadata.json"))
    assert parquet_i < meta_i


def test_s3_backend_requires_bucket(tmp_path):
    s = load_settings(data_root=tmp_path, storage_backend="s3")
    with pytest.raises(ValueError), state_sync(s):
        pass


def test_local_backend_is_a_noop(tmp_path):
    with state_sync(load_settings(data_root=tmp_path)):
        pass
