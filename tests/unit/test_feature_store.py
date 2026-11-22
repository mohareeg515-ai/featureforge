from __future__ import annotations

import json
from datetime import date, datetime, timedelta

import polars as pl
import pytest

from featureforge.features.consistency import schema_problems, value_mismatches
from featureforge.features.definitions import FEATURE_NAMES
from featureforge.features.engine import FeatureEngine
from featureforge.storage.feature_store import (
    FeatureStore,
    FeatureStoreError,
    NoSnapshotError,
    SchemaMismatchError,
    StaleFeaturesError,
    TransformationVersionError,
)
from helpers import T, ev_frame, tx_frame, write_norm


@pytest.fixture
def world(tmp_path):
    rows = []
    for i, cust in enumerate(["A", "B", "C"]):
        for k in range(4):
            rows.append(
                dict(
                    customer=cust,
                    invoice=f"{cust}{k}",
                    stock=f"1000{k}",
                    qty=1 + i,
                    price=5.0 + k,
                    ts=T - timedelta(days=3 + 9 * k + i),
                )
            )
    ev = ev_frame(
        [
            dict(customer="A", type="session_start", ts=T - timedelta(days=1)),
            dict(customer="B", type="add_to_cart", ts=T - timedelta(days=2)),
        ]
    )
    eng = FeatureEngine(*write_norm(tmp_path, tx_frame(rows), ev))
    store = FeatureStore(tmp_path / "fs")
    yield eng, store
    eng.close()


def publish(eng, store, d, **kw):
    df = eng.compute(datetime.combine(d, datetime.min.time()))
    return store.publish(
        df, d, batch_id=kw.get("batch_id", f"b-{d}"), input_fingerprint=kw.get("fp", "fp")
    )


def test_publish_layout_metadata_and_checksum(world):
    eng, store = world
    meta = publish(eng, store, date(2025, 3, 1))
    part = store.partition_dir(date(2025, 3, 1))
    assert part.name == "snapshot_date=2025-03-01"
    assert (part / "part-0.parquet").exists() and (part / "_metadata.json").exists()
    assert meta["row_count"] == 3 and meta["transformation_version"] == "1.0.0"
    assert store.verify(date(2025, 3, 1))
    with (part / "part-0.parquet").open("ab") as fh:  # corrupt the file
        fh.write(b"junk")
    assert not store.verify(date(2025, 3, 1))


def test_republish_replaces_and_counts(world):
    eng, store = world
    publish(eng, store, date(2025, 3, 1), batch_id="one")
    meta = publish(eng, store, date(2025, 3, 1), batch_id="two")
    assert meta["publish_count"] == 2 and store.read_metadata(date(2025, 3, 1))["batch_id"] == "two"
    assert store.list_snapshots() == [date(2025, 3, 1)]
    leftovers = [p.name for p in store.table_dir.iterdir() if p.name.startswith(".")]
    assert leftovers == []


def test_publish_rejects_bad_frames(world):
    eng, store = world
    df = eng.compute(T)
    with pytest.raises(SchemaMismatchError):
        store.publish(df.drop("sessions_7d"), date(2025, 3, 1), batch_id="b", input_fingerprint="f")
    with pytest.raises(SchemaMismatchError):
        store.publish(
            df.with_columns(pl.col("purchases_7d").cast(pl.Float64)),
            date(2025, 3, 1),
            batch_id="b",
            input_fingerprint="f",
        )
    with pytest.raises(SchemaMismatchError):  # snapshot_ts must equal the date's cutoff
        store.publish(df, date(2025, 3, 2), batch_id="b", input_fingerprint="f")
    with pytest.raises(SchemaMismatchError):  # duplicate entity keys
        store.publish(pl.concat([df, df]), date(2025, 3, 1), batch_id="b", input_fingerprint="f")
    assert store.list_snapshots() == []  # nothing half-published


def test_multi_partition_duckdb_query_and_pruning(world):
    eng, store = world
    for d in (date(2025, 3, 1), date(2025, 3, 2), date(2025, 3, 3)):
        publish(eng, store, d)
    all_rows = store.query(
        "SELECT count(*) AS n, count(DISTINCT snapshot_date) AS d FROM customer_features"
    )
    assert all_rows.row(0) == (9, 3)
    one = store.query(
        "SELECT snapshot_date, count(*) AS n FROM customer_features "
        "WHERE snapshot_date = DATE '2025-03-02' GROUP BY 1"
    )
    assert one.row(0) == (date(2025, 3, 2), 3)
    avg = store.query("SELECT avg(purchases_90d) AS a FROM customer_features")
    assert avg["a"][0] > 0


def test_latest_features_inference_path(world):
    eng, store = world
    publish(eng, store, date(2025, 3, 1))
    publish(eng, store, date(2025, 3, 3))
    out = store.get_latest_features(
        ["A", "GHOST"], ["purchases_30d", "avg_order_value"], as_of=datetime(2025, 3, 3, 12)
    )
    assert out.columns == ["customer_id", "snapshot_ts", "purchases_30d", "avg_order_value"]
    assert out["snapshot_ts"].unique().to_list() == [datetime(2025, 3, 3)]
    ghost = out.filter(pl.col("customer_id") == "GHOST").row(0, named=True)
    assert ghost["purchases_30d"] == 0 and ghost["avg_order_value"] is None
    earlier = store.get_latest_features(["A"], ["purchases_30d"], as_of=datetime(2025, 3, 2))
    assert earlier["snapshot_ts"][0] == datetime(2025, 3, 1)  # never a snapshot from the future


def test_freshness_and_missing_snapshot_errors(world):
    eng, store = world
    with pytest.raises(NoSnapshotError):
        store.get_latest_features(as_of=T)
    publish(eng, store, date(2025, 3, 1))
    with pytest.raises(StaleFeaturesError):
        store.get_latest_features(as_of=T + timedelta(days=10), max_age=timedelta(days=2))
    with pytest.raises(NoSnapshotError):  # as_of before the first snapshot
        store.get_latest_features(as_of=T - timedelta(days=1))
    with pytest.raises(FeatureStoreError):
        store.get_latest_features(features=["nope"], as_of=T)


def test_transformation_version_mismatch_is_detected(world):
    eng, store = world
    publish(eng, store, date(2025, 3, 1))
    meta_path = store.partition_dir(date(2025, 3, 1)) / "_metadata.json"
    meta = json.loads(meta_path.read_text())
    meta["transformation_version"] = "0.0.1"
    meta_path.write_text(json.dumps(meta))
    with pytest.raises(TransformationVersionError):
        store.get_latest_features(as_of=T)
    obs = pl.DataFrame({"customer_id": ["A"], "observation_ts": [T]})
    with pytest.raises(TransformationVersionError):
        store.get_training_features(obs)


def test_feature_history(world):
    eng, store = world
    for d in (date(2025, 3, 1), date(2025, 3, 2), date(2025, 3, 3)):
        publish(eng, store, d)
    h = store.get_feature_history("A", ["purchases_30d"], start=date(2025, 3, 2))
    assert h["snapshot_ts"].to_list() == [datetime(2025, 3, 2), datetime(2025, 3, 3)]
    assert store.get_feature_history("NOBODY", ["purchases_30d"]).is_empty()


def test_training_and_inference_paths_agree_by_value(world):
    """Same entity + cutoff through three paths must be numerically identical."""
    eng, store = world
    publish(eng, store, date(2025, 3, 1))
    feats = list(FEATURE_NAMES)
    ids = ["A", "B", "C"]
    obs = pl.DataFrame({"customer_id": ids, "observation_ts": [T] * 3})

    training = store.get_training_features(obs, feats)
    inference = store.get_latest_features(ids, feats, as_of=T)
    recomputed = eng.compute(T).filter(pl.col("customer_id").is_in(ids))

    assert schema_problems(inference) == []
    assert value_mismatches(training, inference, feats) == []
    assert value_mismatches(training, recomputed, feats) == []
    assert (
        [c for c in training.columns if c in feats]
        == feats
        == [c for c in inference.columns if c in feats]
    )
    for name in feats:  # identical dtypes, not merely identical names
        assert training.schema[name] == inference.schema[name] == recomputed.schema[name]


def test_value_mismatch_detector_actually_detects(world):
    eng, _ = world
    a = eng.compute(T)
    b = a.with_columns(pl.col("total_spend_30d") + 0.5)
    assert value_mismatches(a, b, ["total_spend_30d"]) != []
    assert (
        value_mismatches(
            a,
            a.with_columns(pl.lit(None, dtype=pl.Float64).alias("avg_order_value")),
            ["avg_order_value"],
        )
        != []
    )
