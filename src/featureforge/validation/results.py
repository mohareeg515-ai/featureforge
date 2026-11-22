"""Validation result model shared by every check."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

from featureforge.utils.fs import atomic_write_json, atomic_write_text
from featureforge.utils.timeutils import utcnow


class Severity(StrEnum):
    INFO = "info"  # check passed
    WARNING = "warning"  # failed, publication continues
    CRITICAL = "critical"  # failed, publication is blocked


@dataclass
class CheckResult:
    dataset: str
    check: str
    passed: bool
    severity: Severity
    affected_records: int
    total_records: int
    diagnostics: dict[str, Any] = field(default_factory=dict)
    executed_at: str = field(default_factory=lambda: utcnow().isoformat())

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["severity"] = self.severity.value
        return d


class CriticalValidationError(RuntimeError):
    """Raised when a critical check fails; nothing may be published."""

    def __init__(self, report: ValidationReport) -> None:
        self.report = report
        failed = [f"{r.dataset}.{r.check}" for r in report.critical_failures]
        super().__init__(f"critical validation failures: {failed}")


@dataclass
class ValidationReport:
    stage: str
    results: list[CheckResult] = field(default_factory=list)

    def extend(self, results: list[CheckResult]) -> None:
        self.results.extend(results)

    @property
    def critical_failures(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity is Severity.CRITICAL]

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed and r.severity is Severity.WARNING]

    @property
    def passed(self) -> bool:
        return not self.critical_failures

    def raise_if_critical(self) -> None:
        if self.critical_failures:
            raise CriticalValidationError(self)

    def summary(self) -> dict[str, int]:
        return {
            "checks": len(self.results),
            "passed": sum(r.passed for r in self.results),
            "warnings": len(self.warnings),
            "critical": len(self.critical_failures),
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "summary": self.summary(),
            "results": [r.to_dict() for r in self.results],
        }


def merge_reports(reports: list[ValidationReport]) -> dict[str, Any]:
    results = [r.to_dict() for rep in reports for r in rep.results]
    return {
        "stages": [rep.stage for rep in reports],
        "summary": {
            "checks": len(results),
            "passed": sum(1 for r in results if r["passed"]),
            "warnings": sum(1 for r in results if not r["passed"] and r["severity"] == "warning"),
            "critical": sum(1 for r in results if not r["passed"] and r["severity"] == "critical"),
        },
        "results": results,
    }


def write_quality_report(directory: Path, name: str, payload: dict[str, Any]) -> Path:
    """Write ``<name>.json`` and a human-readable ``<name>.md``."""
    json_path = directory / f"{name}.json"
    atomic_write_json(json_path, payload)
    lines = [
        f"# Quality report: {name}",
        "",
        f"Summary: `{json.dumps(payload['summary'])}`",
        "",
        "| Dataset | Check | Status | Severity | Affected | Total |",
        "|---|---|---|---|---:|---:|",
    ]
    for r in payload["results"]:
        status = "PASS" if r["passed"] else "FAIL"
        lines.append(
            f"| {r['dataset']} | {r['check']} | {status} | {r['severity']} "
            f"| {r['affected_records']} | {r['total_records']} |"
        )
    atomic_write_text(directory / f"{name}.md", "\n".join(lines) + "\n")
    return json_path
