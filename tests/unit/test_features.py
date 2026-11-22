from __future__ import annotations

from datetime import date, datetime, timedelta

import polars as pl
import pytest

from featureforge.config.layout import Layout
from featureforge.features.consistency import schema_problems, value_mismatches
from featureforge.features.definitions import (
    ENTITY_KEY,
    FEATURE_BY_NAME,
    FEATURE_NAMES,
    FEATURES,
    feature_schema,
)
from featureforge.features.engine import FeatureEngine
from featureforge.orchestration.pipeline import run_snapshot
from helpers import T, ev_frame, tx_frame, write_norm
from reference import reference_features


def _engine(tmp_path, tx, ev=None):
    return FeatureEngine(*write_norm(tmp_path, tx, ev))


def _a_rows():
    d = lambda *a: datetime(*a)
    return [
        dict(customer="A", invoice="O1", stock="10001", qty=2, price=5.0, ts=d(2025, 2, 26, 10)),
        dict(customer="A", invoice="O1", stock="10002", qty=1, price=3.0, ts=d(2025, 2, 26, 10)),
        dict(customer="A", invoice="O2", stock="10001", qty=1, price=5.0, ts=d(2025, 2, 10, 12)),
        dict(customer="A", invoice="O3", stock="10003", qty=4, price=2.5, ts=d(2024, 12, 15, 9)),
        dict(customer="A", invoice="O4", stock="10004", qty=1, price=100.0, ts=d(2024, 10, 1, 8)),
    ]


def test_hand_computed_values(tmp_path):
    ev = ev_frame(
        [
            dict(customer="A", type="session_start", ts=datetime(2025, 2, 27, 9)),
            dict(customer="A", type="session_start", ts=datetime(2025, 2, 22, 0, 0, 0)),  # == T-7d
            dict(customer="A", type="session_start", ts=datetime(2025, 2, 21, 23, 59, 59)),  # out
            dict(customer="A", type="add_to_cart", ts=datetime(2025, 2, 28, 8)),
            dict(customer="B", type="session_start", ts=datetime(2025, 2, 28, 8)),
            dict(customer="B", type="session_start", ts=datetime(2025, 2, 27, 8)),
        ]
    )
    with _engine(tmp_path, tx_frame(_a_rows()), ev) as eng:
        df = eng.compute(T)
    a = df.filter(pl.col(ENTITY_KEY) == "A").row(0, named=True)
    tenure_days = (T - datetime(2024, 10, 1, 8)).total_seconds() / 86400
    freq = 4 / max(tenure_days / 30, 1.0)
    assert a["purchases_7d"] == 1
    assert a["purchases_30d"] == 2
    assert a["purchases_90d"] == 3
    assert a["total_spend_30d"] == pytest.approx(18.0)
    assert a["total_spend_90d"] == pytest.approx(28.0)
    assert a["avg_order_value"] == pytest.approx((13 + 5 + 10 + 100) / 4)
    assert a["days_since_last_purchase"] == 2  # 2d14h -> floor
    assert a["unique_products_30d"] == 2
    assert a["purchase_frequency"] == pytest.approx(freq)
    assert a["customer_lifetime_value_proxy"] == pytest.approx(32.0 * freq * 12)
    assert a["sessions_7d"] == 2 and a["cart_events_7d"] == 1
    b = df.filter(pl.col(ENTITY_KEY) == "B").row(0, named=True)
    assert b["sessions_7d"] == 2 and b["purchases_30d"] == 0 and b["total_spend_90d"] == 0.0
    assert b["avg_order_value"] is None and b["days_since_last_purchase"] is None
    assert b["customer_lifetime_value_proxy"] is None


@pytest.mark.parametrize(
    ("offset", "in7", "in30"),
    [
        (timedelta(0), 0, 0),  # exactly at the cutoff: excluded (event time < T)
        (-timedelta(seconds=1), 1, 1),
        (-timedelta(days=7), 1, 1),  # lower bound inclusive
        (-timedelta(days=7, seconds=1), 0, 1),
        (-timedelta(days=30), 0, 1),
        (-timedelta(days=30, seconds=1), 0, 0),
    ],
)
def test_window_boundaries(tmp_path, offset, in7, in30):
    ts = T + offset
    tx = tx_frame(
        [dict(customer="C", invoice="X1", stock="10001", qty=1, price=10.0, ts=ts, avail=ts)]
    )  # available immediately so only event time matters
    with _engine(tmp_path, tx) as eng:
        df = eng.compute(T)
    row = df.filter(pl.col(ENTITY_KEY) == "C")
    if row.is_empty():  # excluded entirely (offset 0): no eligible activity
        assert (in7, in30) == (0, 0)
        return
    assert row["purchases_7d"][0] == in7 and row["purchases_30d"][0] == in30


def test_sql_matches_independent_reference_on_synthetic_data(settings):
    for d in (date(2025, 3, 1), date(2025, 3, 15), date(2025, 4, 10)):
        run_snapshot(settings, d)
    layout = Layout(settings.data_root)
    tx = pl.read_parquet(layout.normalized / "transactions.parquet")
    ev = pl.read_parquet(layout.normalized / "customer_events.parquet")
    with FeatureEngine(
        layout.normalized / "transactions.parquet", layout.normalized / "customer_events.parquet"
    ) as eng:
        for d in (datetime(2025, 3, 1), datetime(2025, 3, 20), datetime(2025, 4, 10)):
            sql = eng.compute(d)
            ref = reference_features(tx, ev, d)
            assert sql.height == ref.height > 0
            assert value_mismatches(sql, ref, list(FEATURE_NAMES), rtol=1e-9) == []


def test_output_schema_matches_registry(tmp_path):
    with _engine(tmp_path, tx_frame(_a_rows())) as eng:
        df = eng.compute(T)
    assert schema_problems(df) == []
    assert dict(df.schema) == feature_schema()


def test_registry_is_complete_and_documented():
    names = [f.name for f in FEATURES]
    assert len(names) == len(set(names)) == 12
    required = {
        "purchases_7d",
        "purchases_30d",
        "purchases_90d",
        "total_spend_30d",
        "total_spend_90d",
        "avg_order_value",
        "days_since_last_purchase",
        "unique_products_30d",
        "purchase_frequency",
        "customer_lifetime_value_proxy",
        "sessions_7d",
        "cart_events_7d",
    }
    assert required == set(names)
    for f in FEATURES:
        assert f.description and f.source and f.calculation
        assert f.entity_key == "customer_id" and f.null_policy in {"zero", "null"}
    assert FEATURE_BY_NAME["sessions_7d"].synthetic_source
    assert not FEATURE_BY_NAME["purchases_7d"].synthetic_source


def test_null_policy_zero_features_never_null(tmp_path):
    ev = ev_frame([dict(customer="Z", type="session_start", ts=T - timedelta(days=1))])
    with _engine(tmp_path, tx_frame(_a_rows()), ev) as eng:
        df = eng.compute(T)
    zero = [f.name for f in FEATURES if f.null_policy == "zero"]
    assert all(df[c].null_count() == 0 for c in zero)
