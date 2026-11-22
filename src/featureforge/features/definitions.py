"""Central feature registry: the single source of truth for feature semantics.

Every feature is computed by ``sql/customer_features.sql`` (one query for all features,
so the definition cannot drift between modules). This registry documents each feature
and drives schema validation, null-handling and the generated docs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import polars as pl

#: Bump when feature SQL or definitions change in a way that alters values or schema.
TRANSFORMATION_VERSION = "1.0.0"

ENTITY_KEY = "customer_id"
SNAPSHOT_TS = "snapshot_ts"


@dataclass(frozen=True)
class FeatureDefinition:
    name: str
    description: str
    dtype: pl.DataType
    null_policy: Literal["zero", "null"]
    source: str
    calculation: str
    entity_key: str = ENTITY_KEY
    synthetic_source: bool = False

    @property
    def null_policy_text(self) -> str:
        return (
            "0 when the customer has no eligible activity"
            if self.null_policy == "zero"
            else "NULL when it cannot be computed (no purchase history)"
        )


_TX = "normalized.transactions (UCI Online Retail schema)"
_EV = "normalized.customer_events (SYNTHETIC)"

FEATURES: tuple[FeatureDefinition, ...] = (
    FeatureDefinition(
        "purchases_7d",
        "Distinct orders in the 7 days before the snapshot.",
        pl.Int64(),
        "zero",
        _TX,
        "count(distinct invoice_no) where invoice_ts in [T-7d, T)",
    ),
    FeatureDefinition(
        "purchases_30d",
        "Distinct orders in the 30 days before the snapshot.",
        pl.Int64(),
        "zero",
        _TX,
        "count(distinct invoice_no) where invoice_ts in [T-30d, T)",
    ),
    FeatureDefinition(
        "purchases_90d",
        "Distinct orders in the 90 days before the snapshot.",
        pl.Int64(),
        "zero",
        _TX,
        "count(distinct invoice_no) where invoice_ts in [T-90d, T)",
    ),
    FeatureDefinition(
        "total_spend_30d",
        "Sum of quantity*unit_price over the last 30 days.",
        pl.Float64(),
        "zero",
        _TX,
        "sum(quantity*unit_price) where invoice_ts in [T-30d, T)",
    ),
    FeatureDefinition(
        "total_spend_90d",
        "Sum of quantity*unit_price over the last 90 days.",
        pl.Float64(),
        "zero",
        _TX,
        "sum(quantity*unit_price) where invoice_ts in [T-90d, T)",
    ),
    FeatureDefinition(
        "avg_order_value",
        "Mean order value over the customer's whole eligible history.",
        pl.Float64(),
        "null",
        _TX,
        "sum(order_value)/count(orders) for invoice_ts < T",
    ),
    FeatureDefinition(
        "days_since_last_purchase",
        "Whole days between the last order and the snapshot.",
        pl.Int32(),
        "null",
        _TX,
        "floor((T - max(invoice_ts)) / 1 day)",
    ),
    FeatureDefinition(
        "unique_products_30d",
        "Distinct stock codes bought in the last 30 days.",
        pl.Int64(),
        "zero",
        _TX,
        "count(distinct stock_code) where invoice_ts in [T-30d, T)",
    ),
    FeatureDefinition(
        "purchase_frequency",
        "Orders per 30 days since first purchase (tenure floor: 30 days).",
        pl.Float64(),
        "null",
        _TX,
        "n_orders / greatest((T - first_order_ts)/30d, 1)",
    ),
    FeatureDefinition(
        "customer_lifetime_value_proxy",
        "PROXY (not a statistical CLV model): avg_order_value * purchase_frequency * 12.",
        pl.Float64(),
        "null",
        _TX,
        "avg_order_value * purchase_frequency * 12",
    ),
    FeatureDefinition(
        "sessions_7d",
        "Synthetic session_start events in the last 7 days.",
        pl.Int64(),
        "zero",
        _EV,
        "count(*) where event_type='session_start' and event_ts in [T-7d, T)",
        synthetic_source=True,
    ),
    FeatureDefinition(
        "cart_events_7d",
        "Synthetic add_to_cart events in the last 7 days.",
        pl.Int64(),
        "zero",
        _EV,
        "count(*) where event_type='add_to_cart' and event_ts in [T-7d, T)",
        synthetic_source=True,
    ),
)

FEATURE_NAMES: tuple[str, ...] = tuple(f.name for f in FEATURES)
FEATURE_BY_NAME: dict[str, FeatureDefinition] = {f.name: f for f in FEATURES}


def feature_schema() -> dict[str, pl.DataType]:
    """Expected polars schema of a published snapshot (entity key, snapshot ts, features)."""
    schema: dict[str, pl.DataType] = {ENTITY_KEY: pl.String(), SNAPSHOT_TS: pl.Datetime("us")}
    schema.update({f.name: f.dtype for f in FEATURES})
    return schema
