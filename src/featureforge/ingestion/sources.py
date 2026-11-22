"""Source readers. A reader returns the *full* source (with ``available_at``);
incremental slicing by availability time is done by :mod:`featureforge.ingestion.ingest`."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

import polars as pl

from featureforge.config.settings import Settings
from featureforge.ingestion.schemas import (
    RETAIL_REQUIRED,
    SOURCE_TX_COLUMNS,
)
from featureforge.ingestion.synthetic import (
    GENERATOR_VERSION,
    _finalize_transactions,
    derive_events,
    generate_transactions,
)

UCI_URL = "https://archive.ics.uci.edu/dataset/352/online+retail"


class SourceSchemaError(ValueError):
    """The source file does not have the expected columns."""


class SourceReader(Protocol):
    name: str
    is_synthetic: bool

    def info(self) -> dict[str, Any]: ...
    def read(self) -> pl.DataFrame: ...


class SyntheticTransactionSource:
    name = "transactions"
    is_synthetic = True

    def __init__(self, settings: Settings) -> None:
        self._s = settings
        self._cache: pl.DataFrame | None = None

    def info(self) -> dict[str, Any]:
        s = self._s
        return {
            "kind": "synthetic",
            "generator_version": GENERATOR_VERSION,
            "seed": s.seed,
            "n_customers": s.synthetic_customers,
            "period": [s.synthetic_start.isoformat(), s.synthetic_end.isoformat()],
            "inject_defects": s.inject_defects,
            "note": "SYNTHETIC data. Not real observations.",
        }

    def read(self) -> pl.DataFrame:
        if self._cache is None:
            s = self._s
            self._cache = generate_transactions(
                seed=s.seed,
                n_customers=s.synthetic_customers,
                start=s.synthetic_start,
                end=s.synthetic_end,
                lag_hours=s.transaction_lag_hours,
                inject_defects=s.inject_defects,
            )
        return self._cache


class RetailFileTransactionSource:
    """Reads a local copy of the UCI Online Retail dataset (CSV or XLSX)."""

    name = "transactions"
    is_synthetic = False

    def __init__(self, path: Path, lag_hours: int = 2) -> None:
        self._path = Path(path)
        self._lag_hours = lag_hours

    def info(self) -> dict[str, Any]:
        return {
            "kind": "retail_file",
            "file": self._path.name,
            "source_url": UCI_URL,
            "license_note": "UCI lists CC BY 4.0 for this dataset; verify on the source page.",
            "availability_assumption": f"available_at = InvoiceDate + {self._lag_hours}h "
            "(the dataset carries no ingestion timestamps)",
        }

    def read(self) -> pl.DataFrame:
        if not self._path.exists():
            raise FileNotFoundError(self._path)
        if self._path.suffix.lower() in {".xlsx", ".xls"}:
            raw = pl.read_excel(self._path).cast(pl.String)
        else:
            raw = pl.read_csv(self._path, infer_schema_length=0, encoding="utf8-lossy")
        by_lower = {c.strip().lower(): c for c in raw.columns}
        missing = [c for c in RETAIL_REQUIRED if c.lower() not in by_lower]
        if missing:
            raise SourceSchemaError(f"{self._path.name}: missing columns {missing}")
        raw = raw.select([pl.col(by_lower[c.lower()]).alias(c) for c in RETAIL_REQUIRED])
        ts = pl.col("InvoiceDate").str.strip_chars()
        parsed = pl.coalesce(
            ts.str.to_datetime("%m/%d/%Y %H:%M", time_unit="us", strict=False),
            ts.str.to_datetime("%Y-%m-%d %H:%M:%S%.f", time_unit="us", strict=False),
            ts.str.to_datetime("%Y-%m-%d %H:%M", time_unit="us", strict=False),
            ts.str.to_datetime("%Y-%m-%dT%H:%M:%S", time_unit="us", strict=False),
        )
        df = raw.with_columns(
            pl.col("Quantity").cast(pl.Float64, strict=False).cast(pl.Int64, strict=False),
            pl.col("UnitPrice").cast(pl.Float64, strict=False),
            parsed.alias("InvoiceDate"),
        ).with_columns(
            (pl.col("InvoiceDate") + pl.duration(hours=self._lag_hours)).alias("available_at")
        )
        return _finalize_transactions(df, source_name="uci_online_retail", is_synthetic=False)


class DerivedEventSource:
    """SYNTHETIC customer events derived from the transaction source's orders."""

    name = "customer_events"
    is_synthetic = True

    def __init__(self, tx_source: SourceReader, seed: int, lag_minutes: int) -> None:
        self._tx = tx_source
        self._seed = seed
        self._lag = lag_minutes
        self._cache: pl.DataFrame | None = None

    def info(self) -> dict[str, Any]:
        return {
            "kind": "synthetic",
            "generator_version": GENERATOR_VERSION,
            "seed": self._seed,
            "derived_from": self._tx.name,
            "note": "SYNTHETIC session/cart events; the real dataset has no clickstream.",
        }

    def read(self) -> pl.DataFrame:
        if self._cache is None:
            self._cache = derive_events(self._tx.read(), seed=self._seed, lag_minutes=self._lag)
        return self._cache


def build_sources(settings: Settings) -> tuple[SourceReader, SourceReader]:
    tx: SourceReader
    if settings.source_kind == "synthetic":
        tx = SyntheticTransactionSource(settings)
    else:
        if settings.retail_file is None:
            raise ValueError("FF_RETAIL_FILE must be set when source_kind='retail_file'")
        tx = RetailFileTransactionSource(settings.retail_file, settings.transaction_lag_hours)
    return tx, DerivedEventSource(tx, settings.seed, settings.events_lag_minutes)


def assert_source_schema(df: pl.DataFrame) -> None:
    missing = [c for c in SOURCE_TX_COLUMNS if c not in df.columns]
    if missing:
        raise SourceSchemaError(f"source frame missing columns {missing}")
