"""Deterministic SYNTHETIC data generators (fixed seeds).

Everything produced here carries ``is_synthetic = True`` and the ``source_name``
prefix ``synthetic``. It mimics the *schema* and some well-known quirks of the UCI
Online Retail dataset (guest orders, cancellations, non-product codes, duplicates) but
must never be presented as real observations.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

import numpy as np
import polars as pl

from featureforge.ingestion.schemas import SOURCE_EVENT_COLUMNS, SOURCE_TX_COLUMNS
from featureforge.processing.cleaning import clean_customer_id

GENERATOR_VERSION = "1"
CATALOG_SIZE = 400
_US_PER_DAY = 86_400_000_000
_COUNTRIES = ["France", "Germany", "Spain", "Netherlands", "Jordan"]


def _to_us(d: date) -> int:
    return (datetime.combine(d, time.min) - datetime(1970, 1, 1)) // timedelta(microseconds=1)


def _ts(us: np.ndarray) -> pl.Series:
    return pl.Series(us.astype(np.int64)).cast(pl.Datetime("us"))


def generate_transactions(
    *,
    seed: int,
    n_customers: int,
    start: date,
    end: date,
    lag_hours: int = 2,
    inject_defects: bool = True,
    late_fraction: float = 0.05,
) -> pl.DataFrame:
    """Generate synthetic order lines in the Online Retail layout (see module docstring)."""
    rng = np.random.default_rng(seed)
    n_days = (end - start).days
    if n_days <= 0:
        raise ValueError("end must be after start")

    # Product catalog with Zipf-like popularity and fixed prices.
    codes = np.array([f"{85000 + i}{'A' if i % 5 == 0 else ''}" for i in range(CATALOG_SIZE)])
    prices = np.clip(np.round(rng.lognormal(np.log(2.5), 0.6, CATALOG_SIZE), 2), 0.2, 50.0)
    pop = 1.0 / (np.arange(CATALOG_SIZE) + 1) ** 0.8
    pop = rng.permutation(pop / pop.sum())

    # Customers: latent purchase rates (orders/day), staggered join dates, a home country.
    cust_ids = np.arange(n_customers) + 12000
    rate = rng.gamma(1.5, 0.04, n_customers)
    join_day = rng.integers(0, max(1, int(n_days * 0.6)), n_customers)
    country = np.where(
        rng.random(n_customers) < 0.9, "United Kingdom", rng.choice(_COUNTRIES, n_customers)
    )

    days = np.arange(n_days)
    lam = np.where(days[:, None] >= join_day[None, :], rate[None, :], 0.0)
    counts = rng.poisson(lam)
    d_idx, c_idx = np.nonzero(counts)
    reps = counts[d_idx, c_idx]
    d_idx, c_idx = np.repeat(d_idx, reps), np.repeat(c_idx, reps)
    n_orders = len(d_idx)

    secs = rng.integers(8 * 3600, 18 * 3600, n_orders)
    order_us = (
        _to_us(start) + d_idx.astype(np.int64) * _US_PER_DAY + secs.astype(np.int64) * 1_000_000
    )
    order = np.argsort(order_us, kind="stable")
    d_idx, c_idx, order_us = d_idx[order], c_idx[order], order_us[order]
    invoice_no = 536000 + np.arange(n_orders)

    late = rng.random(n_orders) < late_fraction
    delay_us = np.where(
        late,
        rng.integers(1, 6, n_orders) * _US_PER_DAY,
        lag_hours * 3_600_000_000,
    ).astype(np.int64)
    avail_us = order_us + delay_us

    n_lines = 1 + rng.poisson(2.0, n_orders)
    line_order = np.repeat(np.arange(n_orders), n_lines)
    prod = rng.choice(CATALOG_SIZE, size=len(line_order), p=pop)
    qty = 1 + rng.poisson(1.5, len(line_order))

    df = pl.DataFrame(
        {
            "InvoiceNo": invoice_no[line_order].astype(str),
            "StockCode": codes[prod],
            "Description": [f"SYNTHETIC PRODUCT {p}" for p in prod],
            "Quantity": qty.astype(np.int64),
            "InvoiceDate": _ts(order_us[line_order]),
            "UnitPrice": prices[prod],
            "CustomerID": cust_ids[c_idx][line_order].astype(str),
            "Country": country[c_idx][line_order],
            "available_at": _ts(avail_us[line_order]),
        }
    )

    if inject_defects and n_orders:
        df = _inject_defects(df, rng, lag_hours)

    return _finalize_transactions(df, source_name="synthetic_transactions", is_synthetic=True)


def _inject_defects(df: pl.DataFrame, rng: np.random.Generator, lag_hours: int) -> pl.DataFrame:
    """Add Online-Retail-like quirks so the cleaning/quality layers have real work to do."""
    invoices = df.get_column("InvoiceNo").unique(maintain_order=True).to_numpy()
    n = len(invoices)

    guest = set(rng.choice(invoices, max(1, int(0.06 * n)), replace=False).tolist())
    df = df.with_columns(
        pl.when(pl.col("InvoiceNo").is_in(list(guest)))
        .then(None)
        .otherwise(pl.col("CustomerID"))
        .alias("CustomerID")
    )

    cancel_inv = rng.choice(invoices, max(1, int(0.02 * n)), replace=False).tolist()
    cancels = df.filter(pl.col("InvoiceNo").is_in(cancel_inv)).with_columns(
        ("C" + pl.col("InvoiceNo")).alias("InvoiceNo"),
        (-pl.col("Quantity")).alias("Quantity"),
        (pl.col("InvoiceDate") + pl.duration(days=2)).alias("InvoiceDate"),
        (pl.col("available_at") + pl.duration(days=2)).alias("available_at"),
    )

    k = len(df)
    zero_price = rng.random(k) < 0.003
    post = rng.random(k) < 0.003
    df = df.with_columns(
        pl.when(pl.Series(zero_price)).then(0.0).otherwise(pl.col("UnitPrice")).alias("UnitPrice"),
        pl.when(pl.Series(post))
        .then(pl.lit("POST"))
        .otherwise(pl.col("StockCode"))
        .alias("StockCode"),
    )
    dup_rows = df.filter(pl.Series(rng.random(k) < 0.005))
    return pl.concat([df, cancels, dup_rows])


def _finalize_transactions(
    df: pl.DataFrame, *, source_name: str, is_synthetic: bool
) -> pl.DataFrame:
    return (
        df.with_columns(
            pl.int_range(pl.len())
            .over(
                ["InvoiceNo", "StockCode", "CustomerID", "InvoiceDate"],
                order_by=pl.int_range(pl.len()),
            )
            .cast(pl.Int64)
            .alias("line_seq")
        )
        .with_columns(
            pl.concat_str(
                [
                    pl.col("InvoiceNo"),
                    pl.col("StockCode"),
                    pl.col("CustomerID").fill_null(""),
                    pl.col("InvoiceDate").dt.strftime("%Y-%m-%dT%H:%M:%S"),
                    pl.col("line_seq").cast(pl.String),
                ],
                separator="|",
            ).alias("record_id"),
            pl.lit(is_synthetic).alias("is_synthetic"),
            pl.lit(source_name).alias("source_name"),
        )
        .select(list(SOURCE_TX_COLUMNS))
        .sort(["InvoiceDate", "InvoiceNo", "StockCode", "line_seq"])
    )


def derive_events(
    transactions: pl.DataFrame,
    *,
    seed: int,
    lag_minutes: int = 5,
    delayed_fraction: float = 0.03,
) -> pl.DataFrame:
    """Create SYNTHETIC session/cart events consistent with the given orders.

    The real Online Retail dataset has no clickstream; these events are generated
    (sessions and carts precede orders, plus background browsing) and flagged synthetic.
    """
    orders = (
        transactions.with_columns(clean_customer_id(pl.col("CustomerID")).alias("cid"))
        .filter(
            pl.col("cid").is_not_null()
            & (pl.col("Quantity") > 0)
            & ~pl.col("InvoiceNo").str.starts_with("C")
        )
        .select("cid", "InvoiceDate")
        .unique()
        .sort("InvoiceDate", "cid")
    )
    empty = pl.DataFrame(schema=SOURCE_EVENT_COLUMNS)
    if orders.is_empty():
        return empty

    rng = np.random.default_rng(seed + 1)
    cust = orders.get_column("cid").to_numpy()
    ts = orders.get_column("InvoiceDate").dt.epoch("us").to_numpy().astype(np.int64)
    n = len(ts)
    lo, hi = int(ts.min()), int(ts.max()) + _US_PER_DAY
    period_days = max(1.0, (hi - lo) / _US_PER_DAY)

    k_sess = 1 + rng.poisson(1.2, n)
    s_cust = np.repeat(cust, k_sess)
    s_ts = np.repeat(ts, k_sess) - rng.integers(0, 3 * _US_PER_DAY, int(k_sess.sum()))

    k_cart = 1 + rng.poisson(1.0, n)
    c_cust = np.repeat(cust, k_cart)
    c_ts = np.repeat(ts, k_cart) - rng.integers(60_000_000, 7_200_000_000, int(k_cart.sum()))

    uniq = np.unique(cust)
    b_s = rng.poisson(period_days * 0.02, len(uniq))
    b_c = rng.poisson(period_days * 0.01, len(uniq))
    bs_cust, bs_ts = np.repeat(uniq, b_s), lo + rng.integers(0, hi - lo, int(b_s.sum()))
    bc_cust, bc_ts = np.repeat(uniq, b_c), lo + rng.integers(0, hi - lo, int(b_c.sum()))

    ev_cust = np.concatenate([s_cust, bs_cust, c_cust, bc_cust])
    ev_ts = np.concatenate([s_ts, bs_ts, c_ts, bc_ts]).astype(np.int64)
    ev_type = np.concatenate(
        [
            np.full(len(s_cust) + len(bs_cust), "session_start"),
            np.full(len(c_cust) + len(bc_cust), "add_to_cart"),
        ]
    )
    delayed = rng.random(len(ev_ts)) < delayed_fraction
    extra = np.where(delayed, rng.integers(1, 48, len(ev_ts)) * 3_600_000_000, 0)
    avail = ev_ts + lag_minutes * 60_000_000 + extra

    df = pl.DataFrame(
        {
            "customer_id": ev_cust,
            "event_type": ev_type,
            "event_ts": _ts(ev_ts),
            "available_at": _ts(avail),
        }
    ).sort(["event_ts", "customer_id", "event_type"])
    return df.with_columns(
        pl.format("EVT{}", pl.int_range(pl.len()).cast(pl.String).str.zfill(8)).alias("event_id"),
        pl.lit(True).alias("is_synthetic"),
        pl.lit("synthetic_events").alias("source_name"),
    ).select(list(SOURCE_EVENT_COLUMNS))
