"""Point-in-time correctness: future / not-yet-available data must never leak backwards."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import polars as pl
import pytest

from featureforge.features.consistency import value_mismatches
from featureforge.features.definitions import ENTITY_KEY, FEATURE_NAMES
from featureforge.features.engine import FeatureEngine
from featureforge.storage.feature_store import FeatureStore
from helpers import T, ev_frame, tx_frame, write_norm


def _base():
    return [
        dict(customer="A", invoice="O1", stock="10001", qty=1, price=10.0, ts=T - timedelta(days=3))
    ]


def _compute(tmp_path, rows, cutoff=T, ev=None):
    with FeatureEngine(*write_norm(tmp_path, tx_frame(rows), ev)) as e:
        return e.compute(cutoff), e.input_fingerprint(cutoff)


def test_future_transactions_cannot_change_earlier_features(tmp_path):
    before, fp1 = _compute(tmp_path, _base())
    future = [
        dict(
            customer="A", invoice="F1", stock="10009", qty=50, price=99.0, ts=T + timedelta(hours=1)
        ),
        dict(customer="A", invoice="F2", stock="10010", qty=5, price=9.0, ts=T + timedelta(days=9)),
        dict(
            customer="NEW", invoice="F3", stock="10001", qty=1, price=1.0, ts=T + timedelta(days=1)
        ),
    ]
    (tmp_path / "x").mkdir()
    after, fp2 = _compute(tmp_path / "x", _base() + future)
    assert value_mismatches(before, after, list(FEATURE_NAMES)) == []
    assert before[ENTITY_KEY].to_list() == after[ENTITY_KEY].to_list()  # no new entity either
    assert fp1 == fp2  # the input fingerprint ignores the future too


def test_late_arriving_record_invisible_until_available(tmp_path):
    late = dict(
        customer="A",
        invoice="L1",
        stock="10002",
        qty=3,
        price=10.0,
        ts=T - timedelta(days=2),
        avail=T + timedelta(days=2),
    )  # event < T, known later
    d = tmp_path
    (d / "a").mkdir()
    (d / "b").mkdir()
    at_t, _ = _compute(d / "a", _base() + [late], T)
    assert at_t["purchases_7d"][0] == 1  # late order not visible at T
    later, _ = _compute(d / "b", _base() + [late], T + timedelta(days=3))
    assert later["purchases_7d"][0] == 2  # visible once available, and it counts by event time


def test_corrected_record_uses_latest_version_available_at_cutoff(tmp_path):
    v1 = dict(
        customer="A",
        invoice="O9",
        stock="10001",
        qty=1,
        price=10.0,
        record_id="R",
        ts=T - timedelta(days=5),
        avail=T - timedelta(days=5),
    )
    v2 = dict(v1, qty=5, avail=T + timedelta(days=1))  # correction published after T
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    old, fp_old = _compute(tmp_path / "a", [v1, v2], T)
    new, fp_new = _compute(tmp_path / "b", [v1, v2], T + timedelta(days=2))
    assert old["total_spend_30d"][0] == pytest.approx(10.0)
    assert new["total_spend_30d"][0] == pytest.approx(50.0)
    assert fp_old != fp_new


def test_correction_only_invalidates_fingerprints_after_its_availability(tmp_path):
    v1 = dict(
        customer="A",
        invoice="O9",
        stock="10001",
        qty=1,
        price=10.0,
        record_id="R",
        ts=T - timedelta(days=5),
        avail=T - timedelta(days=5),
    )
    v2 = dict(v1, qty=5, avail=T + timedelta(days=10))
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    _, early_without = _compute(tmp_path / "a", [v1], T)
    _, early_with = _compute(tmp_path / "b", [v1, v2], T)
    assert early_without == early_with  # earlier snapshot unaffected by a later correction


def _publish(store, eng, d: date):
    df = eng.compute(datetime.combine(d, datetime.min.time()))
    store.publish(df, d, batch_id=f"b{d}", input_fingerprint="f")


def test_training_retrieval_is_asof_and_naive_latest_join_leaks(tmp_path):
    rows = _base() + [
        dict(
            customer="A",
            invoice="O2",
            stock="10002",
            qty=2,
            price=10.0,
            ts=T + timedelta(days=1, hours=3),
        )
    ]  # happens AFTER observation
    store = FeatureStore(tmp_path / "fs")
    with FeatureEngine(*write_norm(tmp_path, tx_frame(rows))) as eng:
        _publish(store, eng, date(2025, 3, 1))
        _publish(store, eng, date(2025, 3, 3))
    obs = pl.DataFrame({"customer_id": ["A"], "observation_ts": [T + timedelta(hours=12)]})

    correct = store.get_training_features(obs, max_staleness=timedelta(days=2))
    assert correct["feature_snapshot_ts"][0] == T  # the 03-01 snapshot, not 03-03
    assert correct["purchases_7d"][0] == 1

    # INCORRECT join (documented anti-pattern): take each entity's *latest* snapshot.
    naive = store.get_latest_features(["A"], as_of=datetime(2025, 3, 4))
    assert naive["purchases_7d"][0] == 2  # leaks the order that happens after the observation
    assert naive["purchases_7d"][0] != correct["purchases_7d"][0]


def test_observation_before_any_snapshot_gets_null_not_future_data(tmp_path):
    store = FeatureStore(tmp_path / "fs")
    with FeatureEngine(*write_norm(tmp_path, tx_frame(_base()))) as eng:
        _publish(store, eng, date(2025, 3, 1))
    obs = pl.DataFrame({"customer_id": ["A"], "observation_ts": [T - timedelta(hours=1)]})
    out = store.get_training_features(obs)
    assert out["feature_snapshot_ts"][0] is None and out["purchases_7d"][0] is None


def test_staleness_tolerance_nulls_old_snapshots(tmp_path):
    store = FeatureStore(tmp_path / "fs")
    with FeatureEngine(*write_norm(tmp_path, tx_frame(_base()))) as eng:
        _publish(store, eng, date(2025, 3, 1))
    obs = pl.DataFrame({"customer_id": ["A"], "observation_ts": [T + timedelta(days=5)]})
    assert (
        store.get_training_features(obs, max_staleness=timedelta(days=1))["feature_snapshot_ts"][0]
        is None
    )
    assert (
        store.get_training_features(obs, max_staleness=timedelta(days=10))["feature_snapshot_ts"][0]
        == T
    )


def test_unknown_entity_in_valid_snapshot_follows_null_policy(tmp_path):
    store = FeatureStore(tmp_path / "fs")
    with FeatureEngine(*write_norm(tmp_path, tx_frame(_base()))) as eng:
        _publish(store, eng, date(2025, 3, 1))
    obs = pl.DataFrame({"customer_id": ["GHOST"], "observation_ts": [T]})
    out = store.get_training_features(obs)
    assert out["purchases_30d"][0] == 0 and out["avg_order_value"][0] is None
    assert out["feature_snapshot_ts"][0] == T


def test_event_features_also_respect_availability(tmp_path):
    ev = ev_frame(
        [
            dict(
                customer="A",
                type="session_start",
                ts=T - timedelta(hours=2),
                avail=T + timedelta(hours=1),
            )
        ]
    )
    df, _ = _compute(tmp_path, _base(), T, ev)
    assert df.filter(pl.col("customer_id") == "A")["sessions_7d"][0] == 0
