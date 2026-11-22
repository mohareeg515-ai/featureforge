"""Stage-level validation suites: raw -> normalized -> features."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pandera.polars as pa
import polars as pl

from featureforge.config.settings import QualityThresholds
from featureforge.features.consistency import schema_problems
from featureforge.features.definitions import ENTITY_KEY, SNAPSHOT_TS, feature_schema
from featureforge.ingestion.schemas import EVENT_TYPES, SOURCE_EVENT_COLUMNS, SOURCE_TX_COLUMNS
from featureforge.validation import checks as c
from featureforge.validation.results import CheckResult, Severity, ValidationReport

TX_RAW, EV_RAW = "raw.transactions", "raw.customer_events"
TX_NORM, EV_NORM = "normalized.transactions", "normalized.customer_events"
FEATS = "features.customer_features"

_NOT_NULL = {
    "InvoiceNo",
    "StockCode",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "available_at",
    "record_id",
}


def raw_transactions_schema() -> pa.DataFrameSchema:
    cols: dict[str, Any] = {
        name: pa.Column(dtype, nullable=name not in _NOT_NULL, required=True)
        for name, dtype in SOURCE_TX_COLUMNS.items()
    }
    cols["ingestion_date"] = pa.Column(pl.Date, nullable=False, required=True)
    return pa.DataFrameSchema(cols, strict=False)


def raw_events_schema() -> pa.DataFrameSchema:
    cols: dict[str, Any] = {
        name: pa.Column(dtype, nullable=name in {"customer_id"}, required=True)
        for name, dtype in SOURCE_EVENT_COLUMNS.items()
    }
    cols["ingestion_date"] = pa.Column(pl.Date, nullable=False, required=True)
    return pa.DataFrameSchema(cols, strict=False)


def validate_raw(
    tx: pl.DataFrame,
    ev: pl.DataFrame,
    *,
    th: QualityThresholds,
    tx_volume: tuple[int, list[int]],
    coverage: dict[str, list[tuple[datetime | None, datetime]]],
) -> ValidationReport:
    """Checks on raw (immutable, uncleaned) data. ``tx_volume`` = (latest delta, earlier deltas)."""
    rep = ValidationReport("validate_raw_data")
    rep.extend(
        [
            c.check_schema_pandera(TX_RAW, tx, raw_transactions_schema()),
            c.check_schema_pandera(EV_RAW, ev, raw_events_schema()),
        ]
    )
    if not rep.passed:  # downstream checks would only produce noise
        return rep
    rep.extend(
        [
            c.check_nonempty(TX_RAW, tx),
            c.check_missing(
                TX_RAW, tx, "CustomerID", th.missing_customer_warn, th.missing_customer_critical
            ),
            c.check_duplicates(
                TX_RAW,
                tx,
                ["InvoiceNo", "StockCode", "CustomerID", "InvoiceDate", "Quantity", "UnitPrice"],
                th.duplicate_warn,
                th.duplicate_critical,
                name="duplicate_records",
            ),
            c.check_duplicates(
                TX_RAW,
                tx,
                ["record_id", "available_at"],
                0.0,
                0.0,
                name="duplicate_record_versions",
            ),
            c.check_missing(
                EV_RAW, ev, "customer_id", th.missing_customer_warn, th.missing_customer_critical
            ),
            c.check_duplicates(
                EV_RAW,
                ev,
                ["event_id"],
                th.duplicate_warn,
                th.duplicate_critical,
                name="duplicate_events",
            ),
            c.check_allowed_values(EV_RAW, ev, "event_type", list(EVENT_TYPES), Severity.WARNING),
            c.check_volume(
                TX_RAW,
                tx_volume[0],
                tx_volume[1],
                th.volume_ratio_low,
                th.volume_ratio_high,
                th.min_history_for_volume,
                name="volume_anomaly[latest_delta]",
            ),
            c.check_coverage_chain(TX_RAW, coverage["transactions"]),
            c.check_coverage_chain(EV_RAW, coverage["customer_events"]),
        ]
    )
    return rep


def validate_normalized(
    tx: pl.DataFrame,
    ev: pl.DataFrame,
    *,
    cutoff: datetime,
    rejected_tx: int,
    rejected_ev: int,
    raw_tx_rows: int,
    raw_ev_rows: int,
    th: QualityThresholds,
) -> ValidationReport:
    rep = ValidationReport("normalize_data")
    tx_e = tx.filter((pl.col("invoice_ts") < cutoff) & (pl.col("available_at") <= cutoff))
    ev_e = ev.filter((pl.col("event_ts") < cutoff) & (pl.col("available_at") <= cutoff))
    rep.extend(
        [
            c.check_nonempty(TX_NORM, tx_e),
            c.check_missing(TX_NORM, tx, "customer_id", 0.0, 0.0),
            c.check_range(TX_NORM, tx, "quantity", minimum=0, inclusive_min=False),
            c.check_range(TX_NORM, tx, "unit_price", minimum=0, inclusive_min=False),
            c.check_duplicates(
                TX_NORM,
                tx,
                ["record_id", "available_at"],
                0.0,
                0.0,
                name="duplicate_record_versions",
            ),
            c.check_rejected_fraction(
                TX_NORM, rejected_tx, raw_tx_rows, th.rejected_warn, th.rejected_critical
            ),
            c.check_rejected_fraction(
                EV_NORM, rejected_ev, raw_ev_rows, th.rejected_warn, th.rejected_critical
            ),
            c.check_referential_integrity(
                EV_NORM, ev_e, tx_e, "customer_id", th.orphan_event_warn, th.orphan_event_critical
            ),
            c.check_freshness(
                TX_NORM,
                tx_e["available_at"].max() if tx_e.height else None,  # type: ignore[arg-type]
                cutoff,
                th.freshness_warn_days,
                th.freshness_critical_days,
                name="freshness[latest_available_transaction]",
            ),
        ]
    )
    return rep


def validate_features(
    df: pl.DataFrame,
    *,
    cutoff: datetime,
    previous_rows: list[int],
    th: QualityThresholds,
) -> ValidationReport:
    rep = ValidationReport("validate_features")
    problems = schema_problems(df)
    rep.extend(
        [
            CheckResult(
                FEATS,
                "schema_and_null_policy",
                not problems,
                Severity.CRITICAL if problems else Severity.INFO,
                len(problems),
                df.width,
                {"problems": problems},
            ),
            c.check_dtypes(FEATS, df, feature_schema()),
        ]
    )
    if problems:
        return rep
    rep.extend(
        [
            c.check_nonempty(FEATS, df),
            c.check_duplicates(FEATS, df, [ENTITY_KEY], 0.0, 0.0, name="duplicate_entity_keys"),
            CheckResult(
                FEATS,
                "snapshot_ts_equals_cutoff",
                bool((df[SNAPSHOT_TS] == cutoff).all()) if df.height else True,
                Severity.CRITICAL,
                0,
                df.height,
                {"cutoff": str(cutoff)},
            ),
        ]
    )
    for col in [
        "purchases_7d",
        "purchases_30d",
        "purchases_90d",
        "total_spend_30d",
        "total_spend_90d",
        "unique_products_30d",
        "sessions_7d",
        "cart_events_7d",
        "avg_order_value",
        "days_since_last_purchase",
    ]:
        rep.results.append(c.check_range(FEATS, df, col, minimum=0))
    rep.results.append(
        c.check_range(FEATS, df, "purchase_frequency", minimum=0, inclusive_min=False)
    )
    for col in ["avg_order_value", "purchase_frequency"]:
        rep.results.append(c.check_missing(FEATS, df, col, 1.0, 1.0))
    rep.extend(
        [
            c.check_feature_consistency(FEATS, df),
            c.check_volume(
                FEATS,
                df.height,
                previous_rows,
                th.volume_ratio_low,
                th.volume_ratio_high,
                th.min_history_for_volume,
                name="volume_anomaly[entity_count]",
            ),
        ]
    )
    return rep
