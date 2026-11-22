"""Run registry (append-only JSONL), error classification and freshness helpers."""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from pydantic import ValidationError as PydanticValidationError

from featureforge.ingestion.ingest import ImmutableRawPartitionError
from featureforge.ingestion.sources import SourceSchemaError
from featureforge.orchestration.locks import RunInProgressError
from featureforge.utils.retry import TransientError
from featureforge.utils.timeutils import utcnow
from featureforge.validation.results import CriticalValidationError


def new_run_id() -> str:
    return f"run-{utcnow():%Y%m%d%H%M%S}-{uuid.uuid4().hex[:6]}"


def classify_error(exc: BaseException) -> str:
    """Coarse error taxonomy used in run records and failure summaries."""
    if isinstance(exc, CriticalValidationError):
        return "validation"
    if isinstance(exc, ImmutableRawPartitionError):
        return "data_integrity"
    if isinstance(exc, SourceSchemaError):
        return "source_schema"
    if isinstance(exc, RunInProgressError):
        return "concurrency"
    if isinstance(exc, FileNotFoundError):
        return "missing_input"
    if isinstance(exc, TransientError | TimeoutError | ConnectionError | OSError):
        return "transient_io"
    if isinstance(exc, PydanticValidationError | ValueError):
        return "configuration_or_data"
    return "unknown"


@dataclass
class RunRecord:
    run_id: str
    snapshot_date: str
    status: str = "running"  # success | skipped_duplicate | failed
    batch_id: str | None = None
    started_at: str = field(default_factory=lambda: utcnow().isoformat())
    finished_at: str | None = None
    duration_s: float | None = None
    tasks: list[dict[str, Any]] = field(default_factory=list)
    rows: dict[str, int] = field(default_factory=dict)
    validation: dict[str, int] = field(
        default_factory=lambda: {"checks": 0, "warnings": 0, "critical": 0}
    )
    freshness_hours: float | None = None
    published: bool = False
    quality_report: str | None = None
    error: dict[str, str] | None = None
    orchestrator: str = "python"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class RunRegistry:
    def __init__(self, path: Path) -> None:
        self.path = path

    def append(self, record: RunRecord) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record.to_dict(), default=str) + "\n")

    def read_all(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        out = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                out.append(json.loads(line))
        return out

    def last_for(self, snapshot_date: date) -> dict[str, Any] | None:
        rows = [r for r in self.read_all() if r["snapshot_date"] == snapshot_date.isoformat()]
        return rows[-1] if rows else None
