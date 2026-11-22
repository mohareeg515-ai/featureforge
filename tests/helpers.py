"""Deterministic fixtures for tests (hand-built normalized tables)."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

TX_SCHEMA = {
    "record_id": pl.String,
    "invoice_no": pl.String,
    "stock_code": pl.String,
    "description": pl.String,
    "quantity": pl.Int64,
    "unit_price": pl.Float64,
    "invoice_ts": pl.Datetime("us"),
    "available_at": pl.Datetime("us"),
    "customer_id": pl.String,
    "country": pl.String,
    "is_synthetic": pl.Boolean,
    "source_name": pl.String,
    "ingestion_date": pl.Date,
}
EV_SCHEMA = {
    "event_id": pl.String,
    "customer_id": pl.String,
    "event_type": pl.String,
    "event_ts": pl.Datetime("us"),
    "available_at": pl.Datetime("us"),
    "is_synthetic": pl.Boolean,
    "source_name": pl.String,
    "ingestion_date": pl.Date,
}
T = datetime(2025, 3, 1)


def tx_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    """rows: customer, invoice, stock, qty, price, ts, [avail], [record_id]."""
    out = []
    for i, r in enumerate(rows):
        ts = r["ts"]
        out.append(
            {
                "record_id": r.get("record_id", f"{r['invoice']}|{r['stock']}|{r['customer']}|{i}"),
                "invoice_no": r["invoice"],
                "stock_code": r["stock"],
                "description": "x",
                "quantity": r["qty"],
                "unit_price": float(r["price"]),
                "invoice_ts": ts,
                "available_at": r.get("avail", ts + timedelta(hours=1)),
                "customer_id": r["customer"],
                "country": "United Kingdom",
                "is_synthetic": True,
                "source_name": "test",
                "ingestion_date": date(2025, 1, 1),
            }
        )
    return pl.DataFrame(out, schema=TX_SCHEMA)


def ev_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    out = []
    for i, r in enumerate(rows):
        out.append(
            {
                "event_id": r.get("event_id", f"E{i}"),
                "customer_id": r["customer"],
                "event_type": r["type"],
                "event_ts": r["ts"],
                "available_at": r.get("avail", r["ts"] + timedelta(minutes=5)),
                "is_synthetic": True,
                "source_name": "test",
                "ingestion_date": date(2025, 1, 1),
            }
        )
    return pl.DataFrame(out, schema=EV_SCHEMA)


def write_norm(tmp: Path, tx: pl.DataFrame, ev: pl.DataFrame | None = None) -> tuple[Path, Path]:
    tx_path, ev_path = tmp / "transactions.parquet", tmp / "customer_events.parquet"
    tx.write_parquet(tx_path)
    (ev if ev is not None else pl.DataFrame(schema=EV_SCHEMA)).write_parquet(ev_path)
    return tx_path, ev_path
