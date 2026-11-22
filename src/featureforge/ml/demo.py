"""Downstream demonstration: will a customer purchase within the next 30 days?

Target
------
For an observation at snapshot time T, ``label = 1`` iff the customer has a (valid,
non-cancelled) order with ``invoice_ts`` in ``[T, T + horizon)``.

Leakage controls
----------------
* Features come from ``FeatureStore.get_training_features`` (point-in-time as-of join).
* Chronological split by snapshot date, with a *purge gap* of ``horizon`` days between the
  last training observation and the first test observation, so no training label window
  overlaps the test period.
* Only observation dates whose full label window lies inside the available data are used.

The data are SYNTHETIC by default: the reported metrics demonstrate the plumbing, not
real-world predictive performance.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import numpy as np
import polars as pl
from sklearn.dummy import DummyClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from featureforge.config.layout import Layout
from featureforge.config.settings import Settings
from featureforge.features.definitions import ENTITY_KEY, FEATURE_NAMES
from featureforge.storage.feature_store import FeatureStore
from featureforge.utils.fs import atomic_write_json
from featureforge.utils.timeutils import snapshot_cutoff


class InsufficientDataError(RuntimeError):
    pass


def build_labeled_observations(
    store: FeatureStore, transactions: pl.DataFrame, horizon_days: int
) -> pl.DataFrame:
    """Observation rows (customer_id, observation_ts, label) for every usable snapshot."""
    last_event = transactions["invoice_ts"].max()
    if not isinstance(last_event, datetime):
        raise InsufficientDataError("no transactions")
    data_end = last_event + timedelta(microseconds=1)
    horizon = timedelta(days=horizon_days)
    rows: list[pl.DataFrame] = []
    for d in store.list_snapshots():
        t = snapshot_cutoff(d)
        if t + horizon > data_end:  # label window not fully observed -> unusable
            continue
        entities = store.get_latest_features(as_of=t, features=["purchases_90d"])
        entities = entities.filter(pl.col("purchases_90d") > 0).select(ENTITY_KEY)
        buyers = (
            transactions.filter((pl.col("invoice_ts") >= t) & (pl.col("invoice_ts") < t + horizon))
            .select(ENTITY_KEY)
            .unique()
            .with_columns(pl.lit(1).alias("label"))
        )
        rows.append(
            entities.join(buyers, on="customer_id", how="left").with_columns(
                pl.col("label").fill_null(0).cast(pl.Int8),
                pl.lit(t).cast(pl.Datetime("us")).alias("observation_ts"),
            )
        )
    if not rows:
        raise InsufficientDataError("no snapshot has a fully observed label window")
    return pl.concat(rows)


def chronological_split(
    obs: pl.DataFrame, horizon_days: int, test_fraction: float = 0.3
) -> tuple[pl.DataFrame, pl.DataFrame]:
    times = sorted(obs["observation_ts"].unique().to_list())
    if len(times) < 4:
        raise InsufficientDataError(f"need >= 4 observation dates, have {len(times)}")
    cut = times[max(1, int(len(times) * (1 - test_fraction)))]
    purge_end = cut - timedelta(days=horizon_days)
    train = obs.filter(pl.col("observation_ts") <= purge_end)
    test = obs.filter(pl.col("observation_ts") >= cut)
    if train.is_empty() or test.is_empty():
        raise InsufficientDataError("split produced an empty train or test set")
    return train, test


def run_ml_demo(settings: Settings, horizon_days: int = 30) -> dict[str, Any]:
    layout = Layout(settings.data_root)
    store = FeatureStore(layout.feature_store)
    tx = pl.read_parquet(layout.normalized / "transactions.parquet")
    obs = build_labeled_observations(store, tx, horizon_days)
    train_obs, test_obs = chronological_split(obs, horizon_days)

    feats = list(FEATURE_NAMES)
    stale = timedelta(days=settings.max_snapshot_staleness_days)
    train = store.get_training_features(train_obs, feats, max_staleness=stale).with_columns(
        train_obs["label"]
    )
    test = store.get_training_features(test_obs, feats, max_staleness=stale).with_columns(
        test_obs["label"]
    )
    x_train, y_train = train.select(feats).to_numpy(), train["label"].to_numpy()
    x_test, y_test = test.select(feats).to_numpy(), test["label"].to_numpy()
    if len(set(y_train.tolist())) < 2 or len(set(y_test.tolist())) < 2:
        raise InsufficientDataError("train or test labels contain a single class")

    model = make_pipeline(
        SimpleImputer(strategy="median"),
        StandardScaler(),
        LogisticRegression(class_weight="balanced", max_iter=1000),
    )
    model.fit(x_train, y_train)
    dummy = DummyClassifier(strategy="prior").fit(x_train, y_train)
    p_model, p_dummy = model.predict_proba(x_test)[:, 1], dummy.predict_proba(x_test)[:, 1]

    coefs = model[-1].coef_[0]
    latest = store.get_latest_features(
        customer_ids=test["customer_id"].unique().head(5).to_list(), features=feats
    )
    inference = model.predict_proba(latest.select(feats).to_numpy())[:, 1]
    report: dict[str, Any] = {
        "target": f"purchase within the next {horizon_days} days",
        "data_is_synthetic": settings.source_kind == "synthetic",
        "split": {
            "train_observation_dates": [
                str(train_obs["observation_ts"].min()),
                str(train_obs["observation_ts"].max()),
            ],
            "test_observation_dates": [
                str(test_obs["observation_ts"].min()),
                str(test_obs["observation_ts"].max()),
            ],
            "purge_gap_days": horizon_days,
        },
        "rows": {"train": train.height, "test": test.height},
        "class_balance": {
            "train_positive_rate": round(float(y_train.mean()), 4),
            "test_positive_rate": round(float(y_test.mean()), 4),
        },
        "metrics": {
            "logistic_regression": {
                "roc_auc": round(float(roc_auc_score(y_test, p_model)), 4),
                "average_precision": round(float(average_precision_score(y_test, p_model)), 4),
            },
            "prior_baseline": {
                "roc_auc": round(float(roc_auc_score(y_test, p_dummy)), 4),
                "average_precision": round(float(average_precision_score(y_test, p_dummy)), 4),
            },
            "note": "Average precision of a no-skill model equals the test positive rate.",
        },
        "standardized_coefficients": {
            f: round(float(c), 4) for f, c in zip(feats, coefs, strict=True)
        },
        "inference_example": {
            "features_snapshot_ts": str(latest["snapshot_ts"][0]),
            "customers": latest["customer_id"].to_list(),
            "predicted_probability": [round(float(p), 4) for p in np.asarray(inference)],
        },
    }
    atomic_write_json(layout.reports / "ml" / "ml_report.json", report)
    return report
