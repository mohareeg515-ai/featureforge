"""Reusable cleaning expressions."""

from __future__ import annotations

import polars as pl

_NULL_LIKE = ["", "nan", "none", "null"]


def clean_customer_id(col: pl.Expr) -> pl.Expr:
    """Standardize customer identifiers: '17850.0' -> '17850'; blanks/NaN -> NULL."""
    s = col.cast(pl.String).str.strip_chars().str.replace(r"\.0+$", "")
    return pl.when(s.str.to_lowercase().is_in(_NULL_LIKE)).then(None).otherwise(s)
