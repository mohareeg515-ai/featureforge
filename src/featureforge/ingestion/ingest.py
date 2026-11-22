"""Incremental ingestion into immutable, partitioned raw storage.

Contract
--------
For snapshot date ``D`` (cutoff ``T = D 00:00``) we ingest the *delta*
``watermark < available_at <= T`` where ``watermark`` is the ``available_up_to`` of the
latest earlier raw partition. Corrections to history must therefore arrive as new records
with a *later* ``available_at`` (bitemporal versioning); a source that silently rewrites
already-ingested availability windows is rejected (``ImmutableRawPartitionError``).
Re-running the same date with unchanged source data is a no-op.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from featureforge import __version__
from featureforge.config.layout import Layout
from featureforge.ingestion.sources import SourceReader
from featureforge.utils.fs import (
    atomic_replace_dir,
    atomic_write_json,
    new_staging_dir,
    recover_dir,
)
from featureforge.utils.hashing import sha256_file
from featureforge.utils.logging import get_logger, log_event
from featureforge.utils.timeutils import snapshot_cutoff, utcnow

_log = get_logger("ingest")

PARQUET_NAME = "part-0.parquet"
META_NAME = "_meta.json"


class ImmutableRawPartitionError(RuntimeError):
    """Raised when re-ingestion would change an already published raw partition."""


@dataclass(frozen=True)
class IngestResult:
    source: str
    ingestion_date: date
    rows: int
    path: Path
    skipped_existing: bool
    available_after: datetime | None
    available_up_to: datetime
    duration_s: float


def content_fingerprint(df: pl.DataFrame) -> str:
    """Order-independent, polars-version-independent fingerprint of a frame."""
    if df.is_empty():
        return "empty"
    keys = (
        df.select(pl.concat_str([pl.col(c).cast(pl.String) for c in df.columns], separator="|"))
        .to_series()
        .sort()
    )
    h = hashlib.sha256()
    for k in keys:
        h.update(str(k).encode())
        h.update(b"\n")
    return f"{df.height}:{h.hexdigest()[:24]}"


def partition_dates(layout: Layout, source: str) -> list[date]:
    base = layout.raw_source_dir(source)
    if not base.exists():
        return []
    out = []
    for p in base.glob("ingestion_date=*"):
        if (p / META_NAME).exists():
            out.append(date.fromisoformat(p.name.split("=", 1)[1]))
    return sorted(out)


def read_meta(layout: Layout, source: str, d: date) -> dict[str, Any]:
    result: dict[str, Any] = json.loads((layout.raw(source, d) / META_NAME).read_text())
    return result


def previous_watermark(layout: Layout, source: str, before: date) -> datetime | None:
    marks = [
        datetime.fromisoformat(read_meta(layout, source, d)["available_up_to"])
        for d in partition_dates(layout, source)
        if d < before
    ]
    return max(marks) if marks else None


def ingest_source(
    layout: Layout,
    reader: SourceReader,
    ingestion_date: date,
    *,
    force: bool = False,
) -> IngestResult:
    started = time.perf_counter()
    cutoff = snapshot_cutoff(ingestion_date)
    target = layout.raw(reader.name, ingestion_date)
    recover_dir(target)
    watermark = previous_watermark(layout, reader.name, ingestion_date)

    full = reader.read()
    delta = full.filter(pl.col("available_at") <= cutoff)
    if watermark is not None:
        delta = delta.filter(pl.col("available_at") > watermark)
    delta = delta.with_columns(pl.lit(ingestion_date).alias("ingestion_date"))
    fingerprint = content_fingerprint(delta.drop("ingestion_date"))

    if (target / META_NAME).exists():
        existing = json.loads((target / META_NAME).read_text())
        if existing["content_fingerprint"] == fingerprint and not force:
            log_event(
                _log,
                "raw partition unchanged; skipping",
                source=reader.name,
                ingestion_date=str(ingestion_date),
            )
            return IngestResult(
                reader.name,
                ingestion_date,
                delta.height,
                target,
                True,
                watermark,
                cutoff,
                time.perf_counter() - started,
            )
        if existing["content_fingerprint"] != fingerprint and not force:
            raise ImmutableRawPartitionError(
                f"{reader.name} ingestion_date={ingestion_date}: source content changed for an "
                "already ingested window. Publish corrections with a later available_at, "
                "or re-run with force=True."
            )

    staging = new_staging_dir(target)
    parquet = staging / PARQUET_NAME
    delta.write_parquet(parquet, compression="zstd")
    event_col = "InvoiceDate" if "InvoiceDate" in delta.columns else "event_ts"
    meta = {
        "source_name": reader.name,
        "is_synthetic": reader.is_synthetic,
        "source_info": reader.info(),
        "ingestion_date": ingestion_date.isoformat(),
        "ingested_at": utcnow().isoformat(),
        "available_after": watermark.isoformat() if watermark else None,
        "available_up_to": cutoff.isoformat(),
        "row_count": delta.height,
        "min_event_ts": str(delta[event_col].min()) if delta.height else None,
        "max_event_ts": str(delta[event_col].max()) if delta.height else None,
        "max_available_at": str(delta["available_at"].max()) if delta.height else None,
        "content_fingerprint": fingerprint,
        "parquet_sha256": sha256_file(parquet),
        "schema": {c: str(t) for c, t in delta.schema.items()},
        "featureforge_version": __version__,
    }
    atomic_write_json(staging / META_NAME, meta)
    atomic_replace_dir(staging, target)
    log_event(
        _log,
        "raw partition written",
        source=reader.name,
        ingestion_date=str(ingestion_date),
        rows=delta.height,
    )
    return IngestResult(
        reader.name,
        ingestion_date,
        delta.height,
        target,
        False,
        watermark,
        cutoff,
        time.perf_counter() - started,
    )


def scan_raw(layout: Layout, source: str, up_to: date | None = None) -> pl.LazyFrame:
    """Lazy scan over raw partitions of a source (optionally only ``ingestion_date <= up_to``)."""
    files = sorted(
        f
        for f in layout.raw_source_dir(source).glob(f"ingestion_date=*/{PARQUET_NAME}")
        if up_to is None or date.fromisoformat(f.parent.name.split("=", 1)[1]) <= up_to
    )
    if not files:
        raise FileNotFoundError(f"no raw partitions for source {source!r}")
    return pl.scan_parquet([str(f) for f in files])
