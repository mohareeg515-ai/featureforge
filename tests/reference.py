"""Independent Polars re-implementation of the features (for cross-checking the SQL)."""

from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl


def reference_features(tx: pl.DataFrame, ev: pl.DataFrame, cutoff: datetime) -> pl.DataFrame:
    t = (
        tx.filter((pl.col("invoice_ts") < cutoff) & (pl.col("available_at") <= cutoff))
        .sort("available_at", descending=True)
        .unique(subset="record_id", keep="first")
    )
    orders = t.group_by(["customer_id", "invoice_no"]).agg(
        pl.col("invoice_ts").min().alias("order_ts"),
        (pl.col("quantity") * pl.col("unit_price")).sum().alias("value"),
    )

    def win(days: int) -> pl.Expr:
        return pl.col("order_ts") >= cutoff - timedelta(days=days)

    o = orders.group_by("customer_id").agg(
        win(7).sum().alias("purchases_7d"),
        win(30).sum().alias("purchases_30d"),
        win(90).sum().alias("purchases_90d"),
        pl.col("value").filter(win(30)).sum().alias("total_spend_30d"),
        pl.col("value").filter(win(90)).sum().alias("total_spend_90d"),
        pl.col("value").mean().alias("avg_order_value"),
        pl.col("order_ts").max().alias("last"),
        pl.col("order_ts").min().alias("first"),
        pl.len().alias("n"),
    )
    o = o.with_columns(
        ((cutoff - pl.col("last")).dt.total_seconds() // 86400)
        .cast(pl.Int32)
        .alias("days_since_last_purchase"),
        (
            pl.col("n")
            / ((cutoff - pl.col("first")).dt.total_seconds() / 86400 / 30).clip(lower_bound=1.0)
        ).alias("purchase_frequency"),
    ).with_columns(
        (pl.col("avg_order_value") * pl.col("purchase_frequency") * 12).alias(
            "customer_lifetime_value_proxy"
        )
    )
    prod = (
        t.filter(pl.col("invoice_ts") >= cutoff - timedelta(days=30))
        .group_by("customer_id")
        .agg(pl.col("stock_code").n_unique().alias("unique_products_30d"))
    )
    e = (
        ev.filter((pl.col("event_ts") < cutoff) & (pl.col("available_at") <= cutoff))
        .sort("available_at", descending=True)
        .unique(subset="event_id", keep="first")
        .filter(pl.col("event_ts") >= cutoff - timedelta(days=7))
    )
    ea = e.group_by("customer_id").agg(
        (pl.col("event_type") == "session_start").sum().alias("sessions_7d"),
        (pl.col("event_type") == "add_to_cart").sum().alias("cart_events_7d"),
    )
    ids = pl.concat([o.select("customer_id"), ev_ids(ev, cutoff)]).unique()
    out = (
        ids.join(o, on="customer_id", how="left")
        .join(prod, on="customer_id", how="left")
        .join(ea, on="customer_id", how="left")
    )
    zero = [
        "purchases_7d",
        "purchases_30d",
        "purchases_90d",
        "total_spend_30d",
        "total_spend_90d",
        "unique_products_30d",
        "sessions_7d",
        "cart_events_7d",
    ]
    out = out.with_columns([pl.col(c).fill_null(0) for c in zero])
    return out.with_columns(pl.lit(cutoff).alias("snapshot_ts"))


def ev_ids(ev: pl.DataFrame, cutoff: datetime) -> pl.DataFrame:
    """Customers with any eligible event in the last 7 days (mirrors ev_agg's entity set)."""
    e = ev.filter((pl.col("event_ts") < cutoff) & (pl.col("available_at") <= cutoff))
    return e.select("customer_id").unique()
