"""Reusable, individually testable data quality checks."""

from __future__ import annotations

import itertools
from datetime import date, datetime
from statistics import median
from typing import Any

import pandera.polars as pa
import polars as pl

from featureforge.validation.results import CheckResult, Severity


def _result(
    dataset: str,
    check: str,
    affected: int,
    total: int,
    severity_if_fail: Severity,
    failed: bool,
    **diag: Any,
) -> CheckResult:
    return CheckResult(
        dataset=dataset,
        check=check,
        passed=not failed,
        severity=severity_if_fail if failed else Severity.INFO,
        affected_records=affected,
        total_records=total,
        diagnostics=diag,
    )


def _by_fraction(
    dataset: str, check: str, affected: int, total: int, warn: float, critical: float, **diag: Any
) -> CheckResult:
    frac = affected / total if total else 0.0
    if frac > critical:
        sev, failed = Severity.CRITICAL, True
    elif frac > warn:
        sev, failed = Severity.WARNING, True
    else:
        sev, failed = Severity.INFO, False
    return _result(
        dataset,
        check,
        affected,
        total,
        sev,
        failed,
        fraction=round(frac, 6),
        warn_threshold=warn,
        critical_threshold=critical,
        **diag,
    )


# 1. Missing values
def check_missing(
    dataset: str, df: pl.DataFrame, column: str, warn: float, critical: float
) -> CheckResult:
    if column not in df.columns:
        return _result(
            dataset,
            f"missing_values[{column}]",
            0,
            df.height,
            Severity.CRITICAL,
            True,
            reason="column absent",
        )
    return _by_fraction(
        dataset,
        f"missing_values[{column}]",
        df[column].null_count(),
        df.height,
        warn,
        critical,
        column=column,
    )


# 2. Duplicates
def check_duplicates(
    dataset: str,
    df: pl.DataFrame,
    subset: list[str],
    warn: float,
    critical: float,
    name: str = "duplicates",
) -> CheckResult:
    dup = int(df.select(subset).is_duplicated().sum()) if df.height else 0
    extra = df.height - df.select(subset).n_unique() if df.height else 0
    return _by_fraction(
        dataset, name, extra, df.height, warn, critical, subset=subset, rows_in_duplicate_groups=dup
    )


# 3. Schema (names + nullability + dtype) using Pandera
def check_schema_pandera(dataset: str, df: pl.DataFrame, schema: pa.DataFrameSchema) -> CheckResult:
    try:
        schema.validate(df, lazy=True)
        return _result(dataset, "schema_validation", 0, df.height, Severity.CRITICAL, False)
    except pa.errors.SchemaErrors as exc:
        cases = exc.failure_cases
        n = int(cases.height) if hasattr(cases, "height") else len(cases)
        sample = cases.head(5).to_dicts() if hasattr(cases, "to_dicts") else []
        return _result(
            dataset,
            "schema_validation",
            n,
            df.height,
            Severity.CRITICAL,
            True,
            failure_sample=[{k: str(v) for k, v in row.items()} for row in sample],
        )


# 4. Types
def check_dtypes(dataset: str, df: pl.DataFrame, expected: dict[str, pl.DataType]) -> CheckResult:
    bad = {
        c: {"expected": str(t), "actual": str(df.schema.get(c))}
        for c, t in expected.items()
        if c in df.schema and df.schema[c] != t
    }
    return _result(
        dataset,
        "type_validation",
        len(bad),
        len(expected),
        Severity.CRITICAL,
        bool(bad),
        mismatches=bad,
    )


# 5. Ranges
def check_range(
    dataset: str,
    df: pl.DataFrame,
    column: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
    inclusive_min: bool = True,
    severity: Severity = Severity.CRITICAL,
) -> CheckResult:
    s = df[column].drop_nulls()
    mask = pl.Series([False] * s.len())
    if minimum is not None:
        mask = mask | ((s < minimum) if inclusive_min else (s <= minimum))
    if maximum is not None:
        mask = mask | (s > maximum)
    bad = int(mask.sum()) if s.len() else 0
    return _result(
        dataset,
        f"range[{column}]",
        bad,
        df.height,
        severity,
        bad > 0,
        minimum=minimum,
        maximum=maximum,
    )


# 6. Referential integrity
def check_referential_integrity(
    dataset: str,
    child: pl.DataFrame,
    parent: pl.DataFrame,
    key: str,
    warn: float,
    critical: float,
    child_key: str | None = None,
) -> CheckResult:
    ck = child_key or key
    parents = parent[key].drop_nulls().unique()
    orphans = int((~child[ck].is_in(parents.implode())).sum()) if child.height else 0
    return _by_fraction(
        dataset, f"referential_integrity[{ck}->{key}]", orphans, child.height, warn, critical
    )


# 7. Freshness
def check_freshness(
    dataset: str,
    latest: datetime | None,
    reference: datetime,
    warn_days: float,
    critical_days: float,
    name: str = "freshness",
) -> CheckResult:
    if latest is None:
        return _result(dataset, name, 1, 1, Severity.CRITICAL, True, reason="no data")
    age = (reference - latest).total_seconds() / 86400.0
    if age > critical_days:
        sev, failed = Severity.CRITICAL, True
    elif age > warn_days:
        sev, failed = Severity.WARNING, True
    else:
        sev, failed = Severity.INFO, False
    return _result(
        dataset,
        name,
        int(failed),
        1,
        sev,
        failed,
        age_days=round(age, 3),
        latest=str(latest),
        reference=str(reference),
    )


# 8. Volume anomalies
def check_volume(
    dataset: str,
    current: int,
    history: list[int],
    low: float,
    high: float,
    min_history: int,
    name: str = "volume_anomaly",
) -> CheckResult:
    if len(history) < min_history:
        return _result(
            dataset,
            name,
            0,
            current,
            Severity.WARNING,
            False,
            note=f"insufficient history ({len(history)}<{min_history})",
        )
    base = median(history)
    ratio = (current / base) if base else (float("inf") if current else 1.0)
    failed = ratio < low or ratio > high
    return _result(
        dataset,
        name,
        int(failed),
        current,
        Severity.WARNING,
        failed,
        current=current,
        median_history=base,
        ratio=round(ratio, 4),
        bounds=[low, high],
    )


# 9. Partition completeness
def check_partition_completeness(
    dataset: str, expected: list[date], present: list[date], name: str = "partition_completeness"
) -> CheckResult:
    missing = sorted(set(expected) - set(present))
    return _result(
        dataset,
        name,
        len(missing),
        len(expected),
        Severity.CRITICAL,
        bool(missing),
        missing=[d.isoformat() for d in missing[:20]],
    )


def check_coverage_chain(
    dataset: str,
    windows: list[tuple[datetime | None, datetime]],
    name: str = "partition_completeness[coverage_chain]",
) -> CheckResult:
    """Raw partitions must cover availability time contiguously (no gaps)."""
    gaps: list[dict[str, str]] = []
    for prev, cur in itertools.pairwise(windows):
        if cur[0] != prev[1]:
            gaps.append({"previous_end": str(prev[1]), "next_start": str(cur[0])})
    return _result(
        dataset, name, len(gaps), len(windows), Severity.CRITICAL, bool(gaps), gaps=gaps[:20]
    )


def check_nonempty(
    dataset: str, df: pl.DataFrame, severity: Severity = Severity.CRITICAL
) -> CheckResult:
    return _result(dataset, "non_empty", int(df.is_empty()), df.height, severity, df.is_empty())


def check_allowed_values(
    dataset: str,
    df: pl.DataFrame,
    column: str,
    allowed: list[str],
    severity: Severity = Severity.CRITICAL,
) -> CheckResult:
    bad = int((~df[column].drop_nulls().is_in(allowed)).sum()) if df.height else 0
    return _result(
        dataset, f"allowed_values[{column}]", bad, df.height, severity, bad > 0, allowed=allowed
    )


# 10. Feature consistency (internal logical relationships between features)
def check_feature_consistency(dataset: str, df: pl.DataFrame) -> CheckResult:
    rules = {
        "purchases_7d<=purchases_30d": pl.col("purchases_7d") > pl.col("purchases_30d"),
        "purchases_30d<=purchases_90d": pl.col("purchases_30d") > pl.col("purchases_90d"),
        "total_spend_30d<=total_spend_90d": pl.col("total_spend_30d")
        > pl.col("total_spend_90d") + 1e-6,
        "unique_products_30d<=lines": pl.col("unique_products_30d") < 0,
        "aov_null_iff_no_history": pl.col("avg_order_value").is_null()
        != pl.col("days_since_last_purchase").is_null(),
        "no_orders_30d_means_zero_spend": (pl.col("purchases_30d") == 0)
        & (pl.col("total_spend_30d") != 0),
        "orders_imply_products": (pl.col("purchases_30d") > 0)
        & (pl.col("unique_products_30d") == 0),
    }
    violations = {name: int(df.filter(expr).height) for name, expr in rules.items()}
    total_bad = sum(violations.values())
    return _result(
        dataset,
        "feature_consistency",
        total_bad,
        df.height,
        Severity.CRITICAL,
        total_bad > 0,
        violations={k: v for k, v in violations.items() if v},
    )


def check_rejected_fraction(
    dataset: str, rejected: int, total: int, warn: float, critical: float
) -> CheckResult:
    """Share of raw rows quarantined by the cleaning rules."""
    return _by_fraction(dataset, "rejected_fraction", rejected, total, warn, critical)
