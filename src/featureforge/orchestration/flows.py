"""Prefect flows: the same pipeline, executed as Prefect tasks.

Scheduling policy: *one* authoritative scheduler per environment.
  * local/dev  -> ``featureforge serve --cron ...`` (Prefect owns the schedule)
  * AWS        -> EventBridge Scheduler starts the ECS task; Prefect is used for
                  in-run orchestration (retries, timeouts, state) only.
Idempotent publication (batch ids) and the per-snapshot lock make an accidental double
trigger harmless.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

# Prefect usage analytics write to its local SQLite DB at start-up and can collide with the
# first flow run ("database is locked", harmless but noisy). Off by default; override via env.
os.environ.setdefault("PREFECT_SERVER_ANALYTICS_ENABLED", "false")

from prefect import flow, task
from prefect.cache_policies import NO_CACHE

from featureforge.config.settings import Settings, load_settings
from featureforge.ingestion.sources import SourceSchemaError
from featureforge.orchestration import steps
from featureforge.orchestration.maintenance import find_stale_snapshots
from featureforge.orchestration.pipeline import STEP_NAMES, run_snapshot
from featureforge.utils.retry import TransientError
from featureforge.utils.timeutils import date_range, utcnow

_NON_RETRYABLE = (ValueError, SourceSchemaError, FileNotFoundError)

# name -> (retries, retry delays in seconds, timeout in seconds)
RETRY_POLICY: dict[str, tuple[int, list[int], int]] = {
    "ingest_transactions": (3, [2, 10, 30], 900),
    "ingest_customer_events": (3, [2, 10, 30], 900),
    "validate_raw_data": (1, [5], 600),
    "normalize_data": (1, [5], 900),
    "generate_features": (1, [5], 1800),
    "validate_features": (1, [5], 600),
    "publish_feature_snapshots": (2, [5, 15], 600),
    "update_metadata": (2, [2, 10], 120),
    "generate_quality_report": (2, [2, 10], 300),
}


def retry_if_transient(task_obj: Any, task_run: Any, state: Any) -> bool:
    """Retry only transient I/O failures. Validation/data/config errors fail fast."""
    exc = state.result(raise_on_failure=False)
    if isinstance(exc, _NON_RETRYABLE):
        return False
    return isinstance(exc, TransientError | TimeoutError | ConnectionError | OSError)


def _make_tasks() -> SimpleNamespace:
    made = {}
    for name in STEP_NAMES:
        retries, delays, timeout = RETRY_POLICY[name]
        made[name] = task(
            getattr(steps, name),
            name=name,
            retries=retries,
            retry_delay_seconds=delays,
            timeout_seconds=timeout,
            retry_condition_fn=retry_if_transient,
            cache_policy=NO_CACHE,
            persist_result=False,
        )
    return SimpleNamespace(**made)


PREFECT_STEPS = _make_tasks()


def _settings(config_file: str | None, overrides: dict[str, Any] | None) -> Settings:
    return load_settings(Path(config_file) if config_file else None, **(overrides or {}))


@flow(name="featureforge-snapshot")  # type: ignore[untyped-decorator]
def featureforge_flow(
    snapshot_date: date | None = None,
    force: bool = False,
    config_file: str | None = None,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build and publish the feature snapshot for ``snapshot_date`` (default: today, UTC)."""
    settings = _settings(config_file, overrides)
    day = snapshot_date or utcnow().date()
    record = run_snapshot(
        settings, day, force=force, step_impl=PREFECT_STEPS, orchestrator="prefect"
    )
    return record.to_dict()


class BackfillError(RuntimeError):
    def __init__(self, summary: dict[str, str]) -> None:
        self.summary = summary
        failed = [d for d, s in summary.items() if s.startswith("failed")]
        super().__init__(f"backfill failed for {failed}")


def run_backfill(
    start: date,
    end: date,
    run_one: Callable[[date], dict[str, Any]],
    *,
    step_days: int = 1,
    stop_on_failure: bool = True,
    only_dates: list[date] | None = None,
) -> dict[str, str]:
    """Sequentially run ``run_one`` over a date range (ascending, order matters for
    incremental ingestion). Completed dates are idempotent no-ops, so a failed backfill
    is recovered by simply re-running the same range."""
    dates = only_dates if only_dates is not None else date_range(start, end, step_days)
    summary: dict[str, str] = {}
    for d in sorted(dates):
        try:
            summary[d.isoformat()] = str(run_one(d)["status"])
        except Exception as exc:
            summary[d.isoformat()] = f"failed: {type(exc).__name__}"
            if stop_on_failure:
                raise BackfillError(summary) from exc
    if any(s.startswith("failed") for s in summary.values()):
        raise BackfillError(summary)
    return summary


@flow(name="featureforge-backfill")  # type: ignore[untyped-decorator]
def backfill_flow(
    start: date,
    end: date,
    step_days: int = 1,
    force: bool = False,
    stop_on_failure: bool = True,
    only_stale: bool = False,
    config_file: str | None = None,
    overrides: dict[str, Any] | None = None,
) -> dict[str, str]:
    """Historical backfill. With ``only_stale`` only snapshots invalidated by data
    corrections (or a new transformation version) in [start, end] are rebuilt."""
    settings = _settings(config_file, overrides)
    only: list[date] | None = None
    if only_stale:
        only = [
            s["snapshot_date"]
            for s in find_stale_snapshots(settings)
            if start <= s["snapshot_date"] <= end
        ]
    return run_backfill(
        start,
        end,
        lambda d: featureforge_flow(
            snapshot_date=d, force=force, config_file=config_file, overrides=overrides
        ),
        step_days=step_days,
        stop_on_failure=stop_on_failure,
        only_dates=only,
    )


def serve_scheduled(cron: str = "0 2 * * *", config_file: str | None = None) -> None:
    """Local/dev scheduler owned by Prefect (do NOT combine with EventBridge)."""
    featureforge_flow.serve(
        name="featureforge-daily",
        cron=cron,
        parameters={"config_file": config_file},
    )


__all__ = [
    "PREFECT_STEPS",
    "RETRY_POLICY",
    "BackfillError",
    "backfill_flow",
    "featureforge_flow",
    "retry_if_transient",
    "run_backfill",
    "serve_scheduled",
    "timedelta",
]
