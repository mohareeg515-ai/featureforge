"""Cleaning and standardization of raw data (Polars lazy expressions).

Rows that fail a business rule are *quarantined* (never silently dropped) with a
``reject_reason`` so that quality reports and investigations have diagnostics.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl

from featureforge.config.layout import Layout
from featureforge.ingestion.ingest import scan_raw
from featureforge.ingestion.schemas import EVENT_SOURCE_NAME, EVENT_TYPES, TX_SOURCE_NAME
from featureforge.processing.cleaning import clean_customer_id
from featureforge.utils.fs import write_parquet_atomic

DEDUPE_COLS = [
    "customer_id",
    "invoice_no",
    "stock_code",
    "invoice_ts",
    "quantity",
    "unit_price",
    "available_at",
]


@dataclass
class NormalizeResult:
    rows_in: int = 0
    rows_clean: int = 0
    rows_rejected: int = 0
    reasons: dict[str, int] = field(default_factory=dict)


def normalize_transactions(
    raw: pl.LazyFrame, product_code_regex: str
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """Return (clean, rejected) lazy frames in the normalized snake_case schema."""
    df = raw.select(
        "record_id",
        pl.col("InvoiceNo").str.strip_chars().str.to_uppercase().alias("invoice_no"),
        pl.col("StockCode").str.strip_chars().str.to_uppercase().alias("stock_code"),
        pl.col("Description").str.strip_chars().alias("description"),
        pl.col("Quantity").cast(pl.Int64).alias("quantity"),
        pl.col("UnitPrice").cast(pl.Float64).alias("unit_price"),
        pl.col("InvoiceDate").alias("invoice_ts"),
        "available_at",
        clean_customer_id(pl.col("CustomerID")).alias("customer_id"),
        pl.col("Country").str.strip_chars().alias("country"),
        "is_synthetic",
        "source_name",
        "ingestion_date",
    )
    reason = pl.coalesce(
        pl.when(pl.col("customer_id").is_null()).then(pl.lit("missing_customer_id")),
        pl.when(pl.col("invoice_ts").is_null() | pl.col("available_at").is_null()).then(
            pl.lit("missing_timestamp")
        ),
        pl.when(pl.col("quantity").is_null() | pl.col("unit_price").is_null()).then(
            pl.lit("missing_quantity_or_price")
        ),
        pl.when(pl.col("invoice_no").str.starts_with("C")).then(pl.lit("cancellation")),
        pl.when(pl.col("quantity") <= 0).then(pl.lit("non_positive_quantity")),
        pl.when(pl.col("unit_price") <= 0).then(pl.lit("non_positive_price")),
        pl.when(~pl.col("stock_code").str.contains(product_code_regex)).then(
            pl.lit("non_product_stock_code")
        ),
    ).alias("reject_reason")
    df = df.with_columns(reason)
    valid = df.filter(pl.col("reject_reason").is_null()).sort(["ingestion_date", "record_id"])
    first = pl.struct(DEDUPE_COLS).is_first_distinct()
    valid = valid.with_columns(first.alias("_first"))
    dups = valid.filter(~pl.col("_first")).with_columns(
        pl.lit("exact_duplicate").alias("reject_reason")
    )
    clean = valid.filter(pl.col("_first")).drop("reject_reason", "_first")
    rejected = pl.concat([df.filter(pl.col("reject_reason").is_not_null()), dups.drop("_first")])
    return clean.sort(["invoice_ts", "record_id"]), rejected


def normalize_events(raw: pl.LazyFrame) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    df = raw.select(
        "event_id",
        clean_customer_id(pl.col("customer_id")).alias("customer_id"),
        pl.col("event_type").str.strip_chars().str.to_lowercase(),
        "event_ts",
        "available_at",
        "is_synthetic",
        "source_name",
        "ingestion_date",
    )
    reason = pl.coalesce(
        pl.when(pl.col("customer_id").is_null()).then(pl.lit("missing_customer_id")),
        pl.when(pl.col("event_ts").is_null() | pl.col("available_at").is_null()).then(
            pl.lit("missing_timestamp")
        ),
        pl.when(~pl.col("event_type").is_in(list(EVENT_TYPES))).then(pl.lit("unknown_event_type")),
    ).alias("reject_reason")
    df = df.with_columns(reason)
    valid = df.filter(pl.col("reject_reason").is_null()).sort(["ingestion_date", "event_id"])
    valid = valid.with_columns(
        pl.struct(["event_id", "available_at"]).is_first_distinct().alias("_first")
    )
    dups = valid.filter(~pl.col("_first")).with_columns(
        pl.lit("exact_duplicate").alias("reject_reason")
    )
    clean = valid.filter(pl.col("_first")).drop("reject_reason", "_first")
    rejected = pl.concat([df.filter(pl.col("reject_reason").is_not_null()), dups.drop("_first")])
    return clean.sort(["event_ts", "event_id"]), rejected


def normalize_all(layout: Layout, product_code_regex: str) -> dict[str, NormalizeResult]:
    """Normalize every raw partition and atomically replace ``normalized/*`` tables."""
    results: dict[str, NormalizeResult] = {}
    jobs = {
        TX_SOURCE_NAME: lambda lf: normalize_transactions(lf, product_code_regex),
        EVENT_SOURCE_NAME: normalize_events,
    }
    for source, fn in jobs.items():
        raw = scan_raw(layout, source)
        rows_in = int(raw.select(pl.len()).collect().item())
        clean_lf, rejected_lf = fn(raw)
        clean, rejected = clean_lf.collect(), rejected_lf.collect()
        write_parquet_atomic(clean, layout.normalized / f"{source}.parquet")
        write_parquet_atomic(rejected, layout.rejected / f"{source}.parquet")
        reasons = (
            dict(
                zip(
                    *rejected.group_by("reject_reason").len().to_dict(as_series=False).values(),
                    strict=True,
                )
            )
            if rejected.height
            else {}
        )
        results[source] = NormalizeResult(rows_in, clean.height, rejected.height, reasons)
    return results
