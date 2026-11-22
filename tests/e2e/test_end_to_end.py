"""Local end-to-end demonstration through the public CLI (no cloud, no paid services)."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from featureforge.cli import main
from featureforge.config.layout import Layout
from featureforge.monitoring.runs import RunRegistry
from featureforge.storage.feature_store import FeatureStore

pytestmark = pytest.mark.e2e
ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def env(tmp_path, monkeypatch):
    root = tmp_path / "data"
    monkeypatch.setenv("FF_DATA_ROOT", str(root))
    monkeypatch.setenv("FF_SYNTHETIC_CUSTOMERS", "120")
    monkeypatch.setenv("FF_SEED", "11")
    return root


def test_full_local_demo(env, capsys):
    layout = Layout(env)
    store = FeatureStore(layout.feature_store)

    # 1. historical backfill over a date range (weekly cadence)
    rc = main(
        [
            "backfill",
            "--start",
            "2025-04-01",
            "--end",
            "2025-08-26",
            "--step-days",
            "7",
            "--no-prefect",
        ]
    )
    assert rc == 0
    snaps = store.list_snapshots()
    assert snaps[0] == date(2025, 4, 1) and snaps[-1] == date(2025, 8, 26) and len(snaps) == 22
    assert all(store.verify(d) for d in snaps)

    # 2. idempotent re-run of an existing batch: no new data published
    before = store.read_metadata(date(2025, 4, 1))
    assert main(["run", "--date", "2025-04-01", "--no-prefect"]) == 0
    after = store.read_metadata(date(2025, 4, 1))
    assert after["published_at"] == before["published_at"] and after["publish_count"] == 1
    runs = RunRegistry(layout.runs_file).read_all()
    assert runs[-1]["status"] == "skipped_duplicate" and len(runs) == 23

    # 3. multi-partition DuckDB query through the CLI
    capsys.readouterr()
    assert (
        main(["query", "SELECT count(DISTINCT snapshot_date) AS partitions FROM customer_features"])
        == 0
    )
    assert "22" in capsys.readouterr().out

    # 4. nothing is stale
    capsys.readouterr()
    assert main(["stale"]) == 0
    assert capsys.readouterr().out.strip().endswith("[]")

    # 5. downstream ML built only from the feature store
    capsys.readouterr()
    assert main(["ml-demo"]) == 0
    report = json.loads((layout.reports / "ml" / "ml_report.json").read_text())
    assert report["data_is_synthetic"] is True
    m = report["metrics"]
    assert m["prior_baseline"]["roc_auc"] == 0.5
    assert m["logistic_regression"]["roc_auc"] > 0.55  # learns real signal in the generator
    assert report["split"]["purge_gap_days"] == 30
    assert (
        report["split"]["train_observation_dates"][1] < report["split"]["test_observation_dates"][0]
    )
    assert 0 < report["class_balance"]["test_positive_rate"] < 1
    assert len(report["inference_example"]["predicted_probability"]) == 5

    # 6. operational dashboard renders real run artifacts only
    from streamlit.testing.v1 import AppTest

    at = AppTest.from_file(str(ROOT / "dashboards" / "app.py"), default_timeout=60)
    at.run()
    assert not at.exception
    labels = {m.label: m.value for m in at.metric}
    assert labels["Runs recorded"] == "23" and labels["Latest feature snapshot"] == "2025-08-26"


def test_dashboard_without_data_shows_message_not_fake_numbers(env):
    from streamlit.testing.v1 import AppTest

    at = AppTest.from_file(str(ROOT / "dashboards" / "app.py"), default_timeout=60)
    at.run()
    assert not at.exception and not list(at.metric)
    assert any("No pipeline runs" in i.value for i in at.info)


def test_invalid_cli_usage_is_rejected():
    with pytest.raises(SystemExit):
        main(["run", "--date", "not-a-date"])


def test_data_commands_fail_cleanly_without_data(env, capsys):
    for cmd in (["stale"], ["query", "SELECT 1"], ["ml-demo"]):
        assert main(cmd) == 1
        assert "No published snapshots" in capsys.readouterr().err
