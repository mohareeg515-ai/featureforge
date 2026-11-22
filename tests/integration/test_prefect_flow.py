"""Real Prefect flow runs against Prefect's temporary local test server."""

from __future__ import annotations

import json
from datetime import date

import pytest
from prefect.testing.utilities import prefect_test_harness

from featureforge.config.layout import Layout
from featureforge.orchestration import flows, steps
from featureforge.orchestration.flows import (
    PREFECT_STEPS,
    RETRY_POLICY,
    backfill_flow,
    featureforge_flow,
)
from featureforge.orchestration.pipeline import STEP_NAMES
from featureforge.storage.feature_store import FeatureStore
from featureforge.utils.retry import TransientError

pytestmark = [pytest.mark.integration, pytest.mark.prefect]


@pytest.fixture(scope="module", autouse=True)
def _prefect():
    with prefect_test_harness():
        yield


def _ov(tmp_path):
    return {
        "data_root": str(tmp_path / "data"),
        "synthetic_customers": 40,
        "synthetic_end": "2025-04-15",
        "seed": 5,
    }


def test_all_nine_required_tasks_exist_with_retry_and_timeout_policies():
    assert STEP_NAMES == (
        "ingest_transactions",
        "ingest_customer_events",
        "validate_raw_data",
        "normalize_data",
        "generate_features",
        "validate_features",
        "publish_feature_snapshots",
        "update_metadata",
        "generate_quality_report",
    )
    for name in STEP_NAMES:
        t = getattr(PREFECT_STEPS, name)
        retries, delays, timeout = RETRY_POLICY[name]
        assert t.name == name and t.retries == retries and t.timeout_seconds == timeout
        assert list(t.retry_delay_seconds) == delays


def test_flow_runs_and_is_idempotent(tmp_path):
    ov = _ov(tmp_path)
    out = featureforge_flow(snapshot_date=date(2025, 3, 1), overrides=ov)
    assert out["status"] == "success" and out["orchestrator"] == "prefect"
    again = featureforge_flow(snapshot_date=date(2025, 3, 1), overrides=ov)
    assert again["status"] == "skipped_duplicate"
    store = FeatureStore(Layout(tmp_path / "data").feature_store)
    assert store.read_metadata(date(2025, 3, 1))["publish_count"] == 1


def test_transient_failure_is_retried(tmp_path, monkeypatch):
    calls = {"n": 0}
    real = steps.ingest_source

    def flaky(*a, **kw):
        calls["n"] += 1
        if calls["n"] == 1:
            raise TransientError("S3 throttled")
        return real(*a, **kw)

    monkeypatch.setattr(steps, "ingest_source", flaky)
    out = featureforge_flow(snapshot_date=date(2025, 3, 1), overrides=_ov(tmp_path))
    assert out["status"] == "success" and calls["n"] == 3  # 1 failed + retry + events ingest


def test_validation_failure_fails_fast_without_retries(tmp_path, monkeypatch):
    import polars as pl

    from featureforge.ingestion.sources import SyntheticTransactionSource

    class Bad(SyntheticTransactionSource):
        def read(self):
            return super().read().with_columns(pl.col("Quantity").cast(pl.Float64))

    _, ev = steps.get_sources(flows.load_settings(**_ov(tmp_path)))
    monkeypatch.setattr(steps, "get_sources", lambda s: (Bad(s), ev))
    calls = {"n": 0}
    real = steps.validate_raw

    def counting(*a, **kw):
        calls["n"] += 1
        return real(*a, **kw)

    monkeypatch.setattr(steps, "validate_raw", counting)
    with pytest.raises(Exception, match="critical validation failures"):
        featureforge_flow(snapshot_date=date(2025, 3, 1), overrides=_ov(tmp_path))
    assert calls["n"] == 1  # not retried: retrying cannot fix bad data
    runs = (Layout(tmp_path / "data").runs_file).read_text().splitlines()
    assert json.loads(runs[-1])["error"]["category"] == "validation"
    assert FeatureStore(Layout(tmp_path / "data").feature_store).list_snapshots() == []


def test_backfill_flow_is_parameterized_and_rerunnable(tmp_path):
    ov = _ov(tmp_path)
    s = backfill_flow(date(2025, 3, 1), date(2025, 3, 15), step_days=7, overrides=ov)
    assert s == {"2025-03-01": "success", "2025-03-08": "success", "2025-03-15": "success"}
    s2 = backfill_flow(date(2025, 3, 1), date(2025, 3, 15), step_days=7, overrides=ov)
    assert set(s2.values()) == {"skipped_duplicate"}
