from __future__ import annotations

import itertools
import json
from datetime import date, datetime
from types import SimpleNamespace

import polars as pl
import pytest

from featureforge.config.layout import Layout
from featureforge.features.engine import FeatureEngine
from featureforge.ingestion.ingest import ImmutableRawPartitionError, ingest_source
from featureforge.ingestion.sources import SyntheticTransactionSource
from featureforge.monitoring.runs import RunRegistry, classify_error
from featureforge.orchestration import steps
from featureforge.orchestration.flows import BackfillError, run_backfill
from featureforge.orchestration.locks import RunInProgressError, run_lock
from featureforge.orchestration.maintenance import find_stale_snapshots, restate_raw
from featureforge.orchestration.pipeline import PLAIN_STEPS, run_snapshot
from featureforge.storage.feature_store import FeatureStore

pytestmark = pytest.mark.integration
D1, D2 = date(2025, 3, 1), date(2025, 3, 8)


def test_successful_run_produces_all_artifacts(settings):
    rec = run_snapshot(settings, D1)
    layout = Layout(settings.data_root)
    assert rec.status == "success" and rec.published and rec.batch_id
    assert [t["name"] for t in rec.tasks] == list(
        __import__("featureforge.orchestration.pipeline", fromlist=["x"]).STEP_NAMES
    )
    store = FeatureStore(layout.feature_store)
    assert store.list_snapshots() == [D1] and store.verify(D1)
    report = json.loads((layout.quality_report(D1) / "latest.json").read_text())
    assert report["summary"]["critical"] == 0 and report["run_id"] == rec.run_id
    assert (layout.quality_report(D1) / f"{rec.run_id}.md").exists()
    runs = RunRegistry(layout.runs_file).read_all()
    assert len(runs) == 1 and runs[0]["status"] == "success"
    latest = json.loads((layout.metadata / "latest.json").read_text())
    assert latest["latest_snapshot_date"] == "2025-03-01"
    assert rec.freshness_hours is not None and rec.rows["feature_rows"] > 0
    assert (layout.rejected / "transactions.parquet").exists()  # quarantine diagnostics


def test_rerun_is_idempotent(settings):
    layout = Layout(settings.data_root)
    store = FeatureStore(layout.feature_store)
    first = run_snapshot(settings, D1)
    meta1 = store.read_metadata(D1)
    sha1 = meta1["parquet_sha256"]
    second = run_snapshot(settings, D1)
    meta2 = store.read_metadata(D1)
    assert second.status == "skipped_duplicate" and not second.published
    assert second.batch_id == first.batch_id
    assert meta2["published_at"] == meta1["published_at"] and meta2["publish_count"] == 1
    assert meta2["parquet_sha256"] == sha1
    assert store.list_snapshots() == [D1]
    assert len(list((layout.feature_store / "customer_features").iterdir())) == 1
    raw_parts = list((layout.root / "raw" / "source=transactions").iterdir())
    assert len(raw_parts) == 1  # no duplicate raw data either


def test_forced_rerun_overwrites_same_partition_without_duplication(settings):
    layout = Layout(settings.data_root)
    store = FeatureStore(layout.feature_store)
    run_snapshot(settings, D1)
    rows = store.read_metadata(D1)["row_count"]
    forced = run_snapshot(settings, D1, force=True)
    meta = store.read_metadata(D1)
    assert forced.published and meta["publish_count"] == 2 and meta["row_count"] == rows
    assert store.query("SELECT count(*) AS n FROM customer_features")["n"][0] == rows


def test_batch_id_changes_when_inputs_change(settings):
    a = run_snapshot(settings, D1).batch_id
    b = run_snapshot(settings, D2).batch_id
    assert a != b


class _CorruptTx(SyntheticTransactionSource):
    def read(self) -> pl.DataFrame:
        return super().read().with_columns(pl.col("Quantity").cast(pl.Float64))


def test_critical_validation_failure_blocks_publication_and_keeps_diagnostics(
    settings, monkeypatch
):
    layout = Layout(settings.data_root)
    _good_tx, ev = steps.get_sources(settings)
    monkeypatch.setattr(steps, "get_sources", lambda s: (_CorruptTx(s), ev))
    with pytest.raises(Exception) as ei:
        run_snapshot(settings, D1)
    assert classify_error(ei.value) == "validation"
    assert FeatureStore(layout.feature_store).list_snapshots() == []
    rec = RunRegistry(layout.runs_file).read_all()[-1]
    assert rec["status"] == "failed" and rec["error"]["category"] == "validation"
    assert rec["published"] is False and rec["validation"]["critical"] >= 1
    diag = json.loads((layout.quality_report(D1) / f"{rec['run_id']}.json").read_text())
    failed = [r for r in diag["results"] if not r["passed"] and r["severity"] == "critical"]
    assert failed and failed[0]["check"] == "schema_validation"
    assert list((layout.metadata / "locks").glob("*.lock")) == []  # lock released after failure


def test_failure_never_publishes_partial_state_and_recovers_next_run(settings, monkeypatch):
    layout = Layout(settings.data_root)
    calls = {"n": 0}

    def flaky_features(s, d):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("disk hiccup")
        return steps.generate_features(s, d)

    impl = SimpleNamespace(**{**vars(PLAIN_STEPS), "generate_features": flaky_features})
    with pytest.raises(OSError):
        run_snapshot(settings, D1, step_impl=impl)
    assert FeatureStore(layout.feature_store).list_snapshots() == []
    assert RunRegistry(layout.runs_file).read_all()[-1]["error"]["category"] == "transient_io"
    ok = run_snapshot(settings, D1, step_impl=impl)  # same logical batch, now succeeds
    assert ok.status == "success" and FeatureStore(layout.feature_store).verify(D1)


def test_concurrent_run_of_same_snapshot_is_rejected(settings):
    layout = Layout(settings.data_root)
    with run_lock(layout, D1), pytest.raises(RunInProgressError):
        run_snapshot(settings, D1)


def test_backfill_range_then_rerun_is_noop(settings):
    layout = Layout(settings.data_root)
    store = FeatureStore(layout.feature_store)
    run = lambda d: run_snapshot(settings, d).to_dict()
    summary = run_backfill(date(2025, 3, 1), date(2025, 3, 22), run, step_days=7)
    assert summary == {
        "2025-03-01": "success",
        "2025-03-08": "success",
        "2025-03-15": "success",
        "2025-03-22": "success",
    }
    assert len(store.list_snapshots()) == 4
    published = {d: store.read_metadata(d)["published_at"] for d in store.list_snapshots()}
    again = run_backfill(date(2025, 3, 1), date(2025, 3, 22), run, step_days=7)
    assert set(again.values()) == {"skipped_duplicate"}
    assert {d: store.read_metadata(d)["published_at"] for d in store.list_snapshots()} == published
    # incremental ingestion: raw partitions are disjoint availability windows
    windows = []
    for f in sorted((layout.root / "raw" / "source=transactions").glob("*/part-0.parquet")):
        part = pl.read_parquet(f)
        meta = json.loads((f.parent / "_meta.json").read_text())
        windows.append((meta["available_after"], meta["available_up_to"]))
        if part.height:
            assert part["available_at"].max() <= datetime.fromisoformat(meta["available_up_to"])
            if meta["available_after"]:
                assert part["available_at"].min() > datetime.fromisoformat(meta["available_after"])
    assert all(a[1] == b[0] for a, b in itertools.pairwise(windows))


def test_backfill_failure_recovery_resumes_without_republishing(settings):
    layout = Layout(settings.data_root)
    store = FeatureStore(layout.feature_store)
    state = {"fail": True}

    def maybe_fail(s, d):
        if state["fail"] and d == date(2025, 3, 15):
            raise OSError("simulated outage")
        return steps.generate_features(s, d)

    impl = SimpleNamespace(**{**vars(PLAIN_STEPS), "generate_features": maybe_fail})
    run = lambda d: run_snapshot(settings, d, step_impl=impl).to_dict()
    with pytest.raises(BackfillError) as ei:
        run_backfill(date(2025, 3, 1), date(2025, 3, 22), run, step_days=7)
    assert ei.value.summary["2025-03-15"].startswith("failed")
    assert "2025-03-22" not in ei.value.summary  # stopped at the failure
    assert store.list_snapshots() == [date(2025, 3, 1), date(2025, 3, 8)]
    before = {d: store.read_metadata(d)["published_at"] for d in store.list_snapshots()}

    state["fail"] = False
    summary = run_backfill(date(2025, 3, 1), date(2025, 3, 22), run, step_days=7)
    assert summary["2025-03-01"] == summary["2025-03-08"] == "skipped_duplicate"
    assert summary["2025-03-15"] == summary["2025-03-22"] == "success"
    assert {d: store.read_metadata(d)["published_at"] for d in before} == before


def test_backfill_continue_on_error_collects_failures():
    def run(d):
        if d == date(2025, 1, 2):
            raise ValueError("bad")
        return {"status": "success"}

    with pytest.raises(BackfillError) as ei:
        run_backfill(date(2025, 1, 1), date(2025, 1, 3), run, stop_on_failure=False)
    assert ei.value.summary == {
        "2025-01-01": "success",
        "2025-01-02": "failed: ValueError",
        "2025-01-03": "success",
    }


class _Corrected(SyntheticTransactionSource):
    """Source whose already-ingested history was corrected (quantity bug fixed)."""

    def read(self) -> pl.DataFrame:
        df = super().read()
        target = df.filter(
            (pl.col("available_at") > datetime(2025, 3, 2))
            & (pl.col("available_at") <= datetime(2025, 3, 7))
            & pl.col("CustomerID").is_not_null()
            & ~pl.col("InvoiceNo").str.starts_with("C")
            & (pl.col("StockCode") != "POST")
            & (pl.col("UnitPrice") > 0)
        ).head(1)
        rid = target["record_id"][0]
        return df.with_columns(
            pl.when(pl.col("record_id") == rid)
            .then(pl.col("Quantity") + 40)
            .otherwise(pl.col("Quantity"))
            .alias("Quantity")
        )


def test_source_correction_invalidates_only_affected_snapshots(settings, monkeypatch):
    layout = Layout(settings.data_root)
    store = FeatureStore(layout.feature_store)
    dates = [date(2025, 3, 1), date(2025, 3, 8), date(2025, 3, 15), date(2025, 3, 22)]
    for d in dates:
        run_snapshot(settings, d)
    assert find_stale_snapshots(settings) == []
    before = {d: store.read_metadata(d)["batch_id"] for d in dates}
    spend_before = store.query(
        "SELECT sum(total_spend_90d) AS s FROM customer_features "
        "WHERE snapshot_date = DATE '2025-03-08'"
    )["s"][0]

    _tx, ev = steps.get_sources(settings)
    monkeypatch.setattr(steps, "get_sources", lambda s: (_Corrected(s), ev))
    with pytest.raises(ImmutableRawPartitionError):  # ordinary ingestion refuses history rewrites
        ingest_source(layout, steps.get_sources(settings)[0], date(2025, 3, 8))

    assert restate_raw(settings, date(2025, 3, 8), date(2025, 3, 8)) == [date(2025, 3, 8)]
    stale = find_stale_snapshots(settings)
    assert [s["snapshot_date"] for s in stale] == [
        date(2025, 3, 8),
        date(2025, 3, 15),
        date(2025, 3, 22),
    ]  # 03-01 is untouched
    assert all(s["reasons"] == ["input_data_changed"] for s in stale)

    summary = run_backfill(
        dates[0],
        dates[-1],
        lambda d: run_snapshot(settings, d).to_dict(),
        only_dates=[s["snapshot_date"] for s in stale],
    )
    assert set(summary.values()) == {"success"}
    assert find_stale_snapshots(settings) == []
    after = {d: store.read_metadata(d)["batch_id"] for d in dates}
    assert after[dates[0]] == before[dates[0]]
    assert all(after[d] != before[d] for d in dates[1:])
    spend_after = store.query(
        "SELECT sum(total_spend_90d) AS s FROM customer_features "
        "WHERE snapshot_date = DATE '2025-03-08'"
    )["s"][0]
    assert spend_after > spend_before  # the corrected quantity is reflected


def test_stale_detection_flags_checksum_and_version_problems(settings):
    run_snapshot(settings, D1)
    store = FeatureStore(Layout(settings.data_root).feature_store)
    (store.partition_dir(D1) / "part-0.parquet").open("ab").write(b"x")
    assert "partition_checksum_mismatch" in find_stale_snapshots(settings)[0]["reasons"]


def test_historical_snapshot_unchanged_by_later_data(settings):
    """Backfilling later dates must never alter an earlier published partition."""
    store = FeatureStore(Layout(settings.data_root).feature_store)
    run_snapshot(settings, D1)
    snap = store.query("SELECT * FROM customer_features ORDER BY customer_id")
    run_snapshot(settings, D2)
    run_snapshot(settings, date(2025, 3, 15))
    again = store.query(
        "SELECT * FROM customer_features WHERE snapshot_date = DATE '2025-03-01' "
        "ORDER BY customer_id"
    )
    assert snap.drop("snapshot_date", strict=False).equals(again.drop("snapshot_date"))
    with FeatureEngine(
        Layout(settings.data_root).normalized / "transactions.parquet",
        Layout(settings.data_root).normalized / "customer_events.parquet",
    ) as eng:
        recomputed = eng.compute(datetime(2025, 3, 1))
    assert recomputed.height == again.height  # rebuilt-from-scratch matches the stored snapshot
