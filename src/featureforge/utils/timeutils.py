"""Time conventions.

All timestamps are naive UTC. The snapshot for date ``D`` is computed at the *cutoff*
``D 00:00:00``: it may use records with ``event_ts < cutoff`` and ``available_at <= cutoff``.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta


def snapshot_cutoff(snapshot_date: date) -> datetime:
    return datetime.combine(snapshot_date, time.min)


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def date_range(start: date, end: date, step_days: int = 1) -> list[date]:
    """Inclusive date range."""
    if end < start:
        raise ValueError("end must be >= start")
    out: list[date] = []
    d = start
    while d <= end:
        out.append(d)
        d += timedelta(days=step_days)
    return out
