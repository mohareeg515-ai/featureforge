"""Single place that defines where every dataset lives under the data root."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass(frozen=True)
class Layout:
    root: Path

    def raw(self, source: str, ingestion_date: date) -> Path:
        return self.root / "raw" / f"source={source}" / f"ingestion_date={ingestion_date:%Y-%m-%d}"

    def raw_source_dir(self, source: str) -> Path:
        return self.root / "raw" / f"source={source}"

    @property
    def normalized(self) -> Path:
        return self.root / "normalized"

    @property
    def rejected(self) -> Path:
        return self.root / "rejected"

    @property
    def feature_store(self) -> Path:
        return self.root / "feature_store"

    @property
    def metadata(self) -> Path:
        return self.root / "metadata"

    @property
    def reports(self) -> Path:
        return self.root / "reports"

    @property
    def staging(self) -> Path:
        return self.root / "staging"

    @property
    def runs_file(self) -> Path:
        return self.metadata / "runs.jsonl"

    @property
    def locks(self) -> Path:
        return self.metadata / "locks"

    def quality_report(self, snapshot_date: date) -> Path:
        return self.reports / "quality" / f"snapshot_date={snapshot_date:%Y-%m-%d}"
