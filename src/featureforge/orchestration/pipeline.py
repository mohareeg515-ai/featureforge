"""Single-snapshot pipeline logic, independent of the executor.

``run_snapshot`` sequences the nine steps and handles run-state tracking, failure
propagation and diagnostics. The *same* sequence is executed by Prefect (steps wrapped
as tasks) or directly in Python (tests, ``--no-prefect``).
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date
from types import SimpleNamespace
from typing import Any

from featureforge.config.layout import Layout
from featureforge.config.settings import Settings
from featureforge.monitoring.runs import RunRecord, classify_error, new_run_id
from featureforge.orchestration import steps
from featureforge.orchestration.locks import run_lock
from featureforge.utils.logging import bind_run_id, configure_logging, get_logger, log_event
from featureforge.utils.timeutils import utcnow
from featureforge.validation.results import ValidationReport

_log = get_logger("pipeline")

STEP_NAMES = (
    "ingest_transactions",
    "ingest_customer_events",
    "validate_raw_data",
    "normalize_data",
    "generate_features",
    "validate_features",
    "publish_feature_snapshots",
    "update_metadata",
    "generate_quality_report",
)

PLAIN_STEPS = SimpleNamespace(**{n: getattr(steps, n) for n in STEP_NAMES})


@dataclass
class _Tracker:
    record: RunRecord

    def run(self, name: str, fn: Any, *args: Any, **kwargs: Any) -> Any:
        started = time.perf_counter()
        status = "success"
        try:
            return fn(*args, **kwargs)
        except BaseException:
            status = "failed"
            raise
        finally:
            self.record.tasks.append(
                {
                    "name": name,
                    "status": status,
                    "duration_s": round(time.perf_counter() - started, 4),
                }
            )


def run_snapshot(
    settings: Settings,
    snapshot_date: date,
    *,
    force: bool = False,
    step_impl: Any = PLAIN_STEPS,
    orchestrator: str = "python",
    run_id: str | None = None,
) -> RunRecord:
    """Run the full pipeline for one snapshot date. Raises on failure (after recording it)."""
    configure_logging(settings.log_level)
    layout = Layout(settings.data_root)
    run_id = run_id or new_run_id()
    record = RunRecord(
        run_id=run_id, snapshot_date=snapshot_date.isoformat(), orchestrator=orchestrator
    )
    reports: list[ValidationReport] = []
    started = time.perf_counter()
    tracker = _Tracker(record)
    s = step_impl

    with bind_run_id(run_id), run_lock(layout, snapshot_date):
        log_event(_log, "run started", snapshot_date=str(snapshot_date), force=force)
        try:
            tx = tracker.run(
                "ingest_transactions", s.ingest_transactions, settings, snapshot_date, force
            )
            ev = tracker.run(
                "ingest_customer_events", s.ingest_customer_events, settings, snapshot_date, force
            )
            record.rows.update({"raw_transactions_delta": tx.rows, "raw_events_delta": ev.rows})
            reports.append(
                tracker.run("validate_raw_data", s.validate_raw_data, settings, snapshot_date)
            )
            norm = tracker.run("normalize_data", s.normalize_data, settings, snapshot_date)
            reports.append(norm.report)
            n_tx = norm.results["transactions"]
            record.rows.update(
                {
                    "normalized_transactions": n_tx.rows_clean,
                    "rejected_transactions": n_tx.rows_rejected,
                }
            )
            if norm.latest_available_at:
                record.freshness_hours = round(
                    (
                        settings_cutoff(snapshot_date) - _parse(norm.latest_available_at)
                    ).total_seconds()
                    / 3600,
                    3,
                )
            batch = tracker.run("generate_features", s.generate_features, settings, snapshot_date)
            record.batch_id = batch.batch_id
            reports.append(tracker.run("validate_features", s.validate_features, settings, batch))
            record.rows["feature_rows"] = batch.rows
            outcome = tracker.run(
                "publish_feature_snapshots",
                s.publish_feature_snapshots,
                settings,
                batch,
                run_id,
                force,
            )
            record.published = outcome.status == "published"
            record.status = "success" if record.published else "skipped_duplicate"
            _finish(record, reports, started)
            tracker.run("update_metadata", s.update_metadata, settings, record)
            path = tracker.run(
                "generate_quality_report",
                s.generate_quality_report,
                settings,
                snapshot_date,
                run_id,
                reports,
            )
            record.quality_report = str(path)
            log_event(_log, "run finished", status=record.status, batch_id=record.batch_id)
            return record
        except BaseException as exc:
            record.status = "failed"
            record.error = {
                "category": classify_error(exc),
                "type": type(exc).__name__,
                "message": str(exc)[:500],
            }
            reports.extend(steps.failure_reports(exc))
            _finish(record, reports, started)
            # Failure bookkeeping bypasses retries/executor so it cannot mask the real error.
            steps.update_metadata(settings, record)
            if reports:
                record.quality_report = str(
                    steps.generate_quality_report(settings, snapshot_date, run_id, reports)
                )
            _log.error(
                "run failed",
                exc_info=True,
                extra={"ctx": {"error_category": record.error["category"]}},
            )
            raise


def settings_cutoff(d: date) -> Any:
    from featureforge.utils.timeutils import snapshot_cutoff

    return snapshot_cutoff(d)


def _parse(value: str) -> Any:
    from datetime import datetime

    return datetime.fromisoformat(value)


def _finish(record: RunRecord, reports: list[ValidationReport], started: float) -> None:
    record.finished_at = utcnow().isoformat()
    record.duration_s = round(time.perf_counter() - started, 4)
    record.validation = {
        "checks": sum(len(r.results) for r in reports),
        "warnings": sum(len(r.warnings) for r in reports),
        "critical": sum(len(r.critical_failures) for r in reports),
    }
