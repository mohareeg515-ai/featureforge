"""Detect snapshots invalidated by corrections to historical source data."""

from __future__ import annotations

from datetime import date
from typing import Any

from featureforge.config.layout import Layout
from featureforge.config.settings import Settings
from featureforge.features.definitions import TRANSFORMATION_VERSION
from featureforge.features.engine import FeatureEngine
from featureforge.ingestion.ingest import ingest_source, partition_dates
from featureforge.ingestion.schemas import EVENT_SOURCE_NAME, TX_SOURCE_NAME
from featureforge.orchestration import steps
from featureforge.processing.normalize import normalize_all
from featureforge.storage.feature_store import FeatureStore
from featureforge.utils.timeutils import snapshot_cutoff


def find_stale_snapshots(
    settings: Settings, dates: list[date] | None = None
) -> list[dict[str, Any]]:
    """Return published snapshots whose inputs (as eligible at their cutoff) have changed.

    A correction that becomes *available* at time ``a`` only affects snapshots with
    ``cutoff >= a``; earlier snapshots keep an identical fingerprint and stay valid.
    Snapshots built with an older ``TRANSFORMATION_VERSION`` are stale too.
    Run the pipeline for a recent date first so ``normalized/`` contains the corrections.
    """
    layout = Layout(settings.data_root)
    store = FeatureStore(layout.feature_store)
    stale: list[dict[str, Any]] = []
    targets = dates or store.list_snapshots()
    with FeatureEngine(
        layout.normalized / "transactions.parquet",
        layout.normalized / "customer_events.parquet",
        memory_limit=settings.duckdb_memory_limit,
        threads=settings.duckdb_threads,
    ) as engine:
        for d in targets:
            meta = store.read_metadata(d)
            if meta is None:
                continue
            current = engine.input_fingerprint(snapshot_cutoff(d))
            reasons = []
            if current != meta["input_fingerprint"]:
                reasons.append("input_data_changed")
            if meta["transformation_version"] != TRANSFORMATION_VERSION:
                reasons.append("transformation_version_changed")
            if not store.verify(d):
                reasons.append("partition_checksum_mismatch")
            if reasons:
                stale.append({"snapshot_date": d, "reasons": reasons})
    return stale


def restate_raw(settings: Settings, start: date, end: date) -> list[date]:
    """Re-ingest (``force``) the raw windows ``start..end`` after the source was corrected.

    This is the explicit *restatement* path: ordinary ingestion refuses to change an already
    ingested window. After restating, ``normalized/`` is rebuilt; run
    ``find_stale_snapshots`` and rebuild exactly the affected feature partitions.
    """
    layout = Layout(settings.data_root)
    tx, ev = steps.get_sources(settings)
    done = [d for d in partition_dates(layout, TX_SOURCE_NAME) if start <= d <= end]
    for d in done:
        ingest_source(layout, tx, d, force=True)
        if d in partition_dates(layout, EVENT_SOURCE_NAME):
            ingest_source(layout, ev, d, force=True)
    normalize_all(layout, settings.product_code_regex)
    return done
