"""The nine pipeline steps as plain, individually testable functions.

They hold no orchestration logic: Prefect wraps them as tasks (``flows.py``) and the
plain runner calls them directly (``pipeline.py``). Steps exchange small dataclasses and
file paths (not large frames) so they work in any executor.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from featureforge.config.layout import Layout
from featureforge.config.settings import Settings
from featureforge.features.definitions import TRANSFORMATION_VERSION
from featureforge.features.engine import FeatureEngine
from featureforge.ingestion.ingest import (
    IngestResult,
    ingest_source,
    partition_dates,
    read_meta,
    scan_raw,
)
from featureforge.ingestion.schemas import EVENT_SOURCE_NAME, TX_SOURCE_NAME
from featureforge.ingestion.sources import SourceReader, build_sources
from featureforge.monitoring.runs import RunRecord, RunRegistry
from featureforge.processing.normalize import NormalizeResult, normalize_all
from featureforge.storage.feature_store import FeatureStore
from featureforge.utils.fs import atomic_write_json, write_parquet_atomic
from featureforge.utils.hashing import stable_hash
from featureforge.utils.logging import get_logger, log_event
from featureforge.utils.timeutils import snapshot_cutoff
from featureforge.validation.results import (
    CriticalValidationError,
    ValidationReport,
    merge_reports,
    write_quality_report,
)
from featureforge.validation.suites import validate_features as _validate_features
from featureforge.validation.suites import validate_normalized, validate_raw

_log = get_logger("steps")
_SOURCE_CACHE: dict[str, tuple[SourceReader, SourceReader]] = {}


def get_sources(settings: Settings) -> tuple[SourceReader, SourceReader]:
    """Build (and cache per settings) the transaction and event sources."""
    key = settings.model_dump_json()
    if key not in _SOURCE_CACHE:
        _SOURCE_CACHE.clear()
        _SOURCE_CACHE[key] = build_sources(settings)
    return _SOURCE_CACHE[key]


@dataclass(frozen=True)
class NormalizeOutcome:
    results: dict[str, NormalizeResult]
    report: ValidationReport
    latest_available_at: str | None


@dataclass(frozen=True)
class FeatureBatch:
    snapshot_date: date
    batch_id: str
    input_fingerprint: str
    features_path: Path
    rows: int


@dataclass(frozen=True)
class PublishOutcome:
    status: str  # "published" | "skipped_duplicate"
    meta: dict[str, Any] = field(default_factory=dict)


# 1 & 2 ---------------------------------------------------------------- ingestion
def ingest_transactions(
    settings: Settings, snapshot_date: date, force: bool = False
) -> IngestResult:
    tx, _ = get_sources(settings)
    return ingest_source(Layout(settings.data_root), tx, snapshot_date, force=force)


def ingest_customer_events(
    settings: Settings, snapshot_date: date, force: bool = False
) -> IngestResult:
    _, ev = get_sources(settings)
    return ingest_source(Layout(settings.data_root), ev, snapshot_date, force=force)


# 3 --------------------------------------------------------------- raw validation
def validate_raw_data(settings: Settings, snapshot_date: date) -> ValidationReport:
    layout = Layout(settings.data_root)
    tx = scan_raw(layout, TX_SOURCE_NAME, snapshot_date).collect()
    ev = scan_raw(layout, EVENT_SOURCE_NAME, snapshot_date).collect()
    dates = [d for d in partition_dates(layout, TX_SOURCE_NAME) if d <= snapshot_date]
    metas = [read_meta(layout, TX_SOURCE_NAME, d) for d in dates]
    counts = [int(m["row_count"]) for m in metas]
    coverage = {
        src: [
            (
                _dt(read_meta(layout, src, d)["available_after"]),
                _dt(read_meta(layout, src, d)["available_up_to"]),
            )
            for d in partition_dates(layout, src)
            if d <= snapshot_date
        ]
        for src in (TX_SOURCE_NAME, EVENT_SOURCE_NAME)
    }
    report = validate_raw(
        tx,
        ev,
        th=settings.thresholds,
        tx_volume=(counts[-1] if counts else 0, counts[:-1]),
        coverage=coverage,
    )
    report.raise_if_critical()
    return report


def _dt(value: str | None) -> Any:
    from datetime import datetime

    return datetime.fromisoformat(value) if value else None


# 4 ------------------------------------------------------------------ normalize
def normalize_data(settings: Settings, snapshot_date: date) -> NormalizeOutcome:
    layout = Layout(settings.data_root)
    results = normalize_all(layout, settings.product_code_regex)
    cutoff = snapshot_cutoff(snapshot_date)
    tx = pl.read_parquet(layout.normalized / f"{TX_SOURCE_NAME}.parquet")
    ev = pl.read_parquet(layout.normalized / f"{EVENT_SOURCE_NAME}.parquet")
    report = validate_normalized(
        tx,
        ev,
        cutoff=cutoff,
        rejected_tx=results[TX_SOURCE_NAME].rows_rejected,
        rejected_ev=results[EVENT_SOURCE_NAME].rows_rejected,
        raw_tx_rows=results[TX_SOURCE_NAME].rows_in,
        raw_ev_rows=results[EVENT_SOURCE_NAME].rows_in,
        th=settings.thresholds,
    )
    eligible = tx.filter((pl.col("invoice_ts") < cutoff) & (pl.col("available_at") <= cutoff))
    latest = eligible["available_at"].max()
    outcome = NormalizeOutcome(results, report, str(latest) if latest is not None else None)
    report.raise_if_critical()
    return outcome


# 5 -------------------------------------------------------------------- features
def generate_features(settings: Settings, snapshot_date: date) -> FeatureBatch:
    layout = Layout(settings.data_root)
    cutoff = snapshot_cutoff(snapshot_date)
    with FeatureEngine(
        layout.normalized / f"{TX_SOURCE_NAME}.parquet",
        layout.normalized / f"{EVENT_SOURCE_NAME}.parquet",
        memory_limit=settings.duckdb_memory_limit,
        threads=settings.duckdb_threads,
        temp_directory=layout.staging / "duckdb_tmp",
    ) as engine:
        fingerprint = engine.input_fingerprint(cutoff)
        df = engine.compute(cutoff)
    batch_id = stable_hash(snapshot_date.isoformat(), TRANSFORMATION_VERSION, fingerprint)
    path = (
        layout.staging / "features" / f"snapshot_date={snapshot_date:%Y-%m-%d}" / "features.parquet"
    )
    write_parquet_atomic(df, path)
    log_event(
        _log,
        "features generated",
        snapshot_date=str(snapshot_date),
        rows=df.height,
        batch_id=batch_id,
    )
    return FeatureBatch(snapshot_date, batch_id, fingerprint, path, df.height)


# 6 --------------------------------------------------------------- feature validation
def validate_features(settings: Settings, batch: FeatureBatch) -> ValidationReport:
    store = FeatureStore(Layout(settings.data_root).feature_store)
    previous = [
        int(m["row_count"])
        for d in store.list_snapshots()
        if d < batch.snapshot_date and (m := store.read_metadata(d))
    ]
    df = pl.read_parquet(batch.features_path)
    report = _validate_features(
        df,
        cutoff=snapshot_cutoff(batch.snapshot_date),
        previous_rows=previous,
        th=settings.thresholds,
    )
    report.raise_if_critical()
    return report


# 7 ------------------------------------------------------------------- publish
def publish_feature_snapshots(
    settings: Settings, batch: FeatureBatch, run_id: str, force: bool = False
) -> PublishOutcome:
    """Publish the validated snapshot. Same logical batch twice => no-op (idempotent)."""
    store = FeatureStore(Layout(settings.data_root).feature_store)
    existing = store.read_metadata(batch.snapshot_date)
    if (
        existing
        and existing["batch_id"] == batch.batch_id
        and store.verify(batch.snapshot_date)
        and not force
    ):
        log_event(_log, "duplicate batch; publication skipped", batch_id=batch.batch_id)
        return PublishOutcome("skipped_duplicate", existing)
    df = pl.read_parquet(batch.features_path)
    meta = store.publish(
        df,
        batch.snapshot_date,
        batch_id=batch.batch_id,
        input_fingerprint=batch.input_fingerprint,
        run_id=run_id,
    )
    return PublishOutcome("published", meta)


# 8 --------------------------------------------------------------- run metadata
def update_metadata(settings: Settings, record: RunRecord) -> None:
    """Append the run record and refresh ``metadata/latest.json``."""
    layout = Layout(settings.data_root)
    RunRegistry(layout.runs_file).append(record)
    store = FeatureStore(layout.feature_store)
    snaps = store.list_snapshots()
    latest = {
        "updated_by_run": record.run_id,
        "latest_snapshot_date": snaps[-1].isoformat() if snaps else None,
        "snapshot_count": len(snaps),
        "last_run_status": record.status,
        "freshness_hours": record.freshness_hours,
    }
    atomic_write_json(layout.metadata / "latest.json", latest)


# 9 -------------------------------------------------------------- quality report
def generate_quality_report(
    settings: Settings, snapshot_date: date, run_id: str, reports: list[ValidationReport]
) -> Path:
    layout = Layout(settings.data_root)
    payload = merge_reports(reports)
    payload.update({"run_id": run_id, "snapshot_date": snapshot_date.isoformat()})
    directory = layout.quality_report(snapshot_date)
    path = write_quality_report(directory, run_id, payload)
    # `latest` pointer for convenience; the per-run file is the immutable diagnostic artifact.
    write_quality_report(directory, "latest", payload)
    return path


def failure_reports(exc: BaseException) -> list[ValidationReport]:
    return [exc.report] if isinstance(exc, CriticalValidationError) else []


def load_json(path: Path) -> Any:
    return json.loads(path.read_text())


def timed(fn: Any, *args: Any, **kwargs: Any) -> tuple[Any, float]:
    started = time.perf_counter()
    out = fn(*args, **kwargs)
    return out, time.perf_counter() - started
