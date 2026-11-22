"""Schemas of raw source data (original Online Retail column names are preserved)."""

from __future__ import annotations

import polars as pl

SOURCE_TX_COLUMNS: dict[str, pl.DataType] = {
    "InvoiceNo": pl.String(),
    "StockCode": pl.String(),
    "Description": pl.String(),
    "Quantity": pl.Int64(),
    "InvoiceDate": pl.Datetime("us"),
    "UnitPrice": pl.Float64(),
    "CustomerID": pl.String(),
    "Country": pl.String(),
    # Added by the source reader:
    "record_id": pl.String(),
    "line_seq": pl.Int64(),
    "available_at": pl.Datetime("us"),
    "is_synthetic": pl.Boolean(),
    "source_name": pl.String(),
}

SOURCE_EVENT_COLUMNS: dict[str, pl.DataType] = {
    "event_id": pl.String(),
    "customer_id": pl.String(),
    "event_type": pl.String(),
    "event_ts": pl.Datetime("us"),
    "available_at": pl.Datetime("us"),
    "is_synthetic": pl.Boolean(),
    "source_name": pl.String(),
}

RETAIL_REQUIRED = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
]

EVENT_TYPES = ("session_start", "add_to_cart")
TX_SOURCE_NAME = "transactions"
EVENT_SOURCE_NAME = "customer_events"
