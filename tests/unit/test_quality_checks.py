from __future__ import annotations

from datetime import date, datetime, timedelta

import polars as pl
import pytest

from featureforge.config.settings import QualityThresholds
from featureforge.ingestion.schemas import SOURCE_EVENT_COLUMNS, SOURCE_TX_COLUMNS
from featureforge.validation import checks as c
from featureforge.validation.results import (
    CheckResult,
    CriticalValidationError,
    Severity,
    ValidationReport,
    merge_reports,
    write_quality_report,
)
from featureforge.validation.suites import (
    raw_transactions_schema,
    validate_features,
    validate_raw,
)

TS = datetime(2025, 1, 1)


def test_result_has_required_fields():
    r = c.check_missing("ds", pl.DataFrame({"a": [1, None]}), "a", 0.1, 0.9)
    d = r.to_dict()
    for k in (
        "dataset",
        "check",
        "executed_at",
        "affected_records",
        "severity",
        "passed",
        "diagnostics",
    ):
        assert k in d
    assert d["affected_records"] == 1 and d["passed"] is False and d["severity"] == "warning"


@pytest.mark.parametrize(
    ("nulls", "passed", "sev"), [(0, True, "info"), (1, False, "warning"), (9, False, "critical")]
)
def test_missing_thresholds_separate_warning_from_critical(nulls, passed, sev):
    df = pl.DataFrame({"a": [None] * nulls + [1] * (10 - nulls)})
    r = c.check_missing("ds", df, "a", warn=0.05, critical=0.5)
    assert r.passed is passed
    assert r.severity.value == sev


def test_missing_column_is_critical():
    assert (
        c.check_missing("ds", pl.DataFrame({"a": [1]}), "zzz", 0.1, 0.2).severity
        is Severity.CRITICAL
    )


def test_duplicates():
    df = pl.DataFrame({"k": [1, 1, 2, 3], "v": [1, 1, 2, 3]})
    r = c.check_duplicates("ds", df, ["k", "v"], warn=0.1, critical=0.9)
    assert r.affected_records == 1 and r.severity is Severity.WARNING
    assert c.check_duplicates("ds", df.unique(), ["k"], 0.0, 0.0).passed


def test_range_and_dtype_checks():
    df = pl.DataFrame({"q": [1, 0, -2, 5]})
    assert c.check_range("ds", df, "q", minimum=0, inclusive_min=False).affected_records == 2
    assert c.check_range("ds", df, "q", minimum=-5, maximum=4).affected_records == 1
    ok = c.check_dtypes("ds", df, {"q": pl.Int64()})
    bad = c.check_dtypes("ds", df, {"q": pl.String()})
    assert ok.passed and not bad.passed and "q" in bad.diagnostics["mismatches"]


def test_referential_integrity():
    parent = pl.DataFrame({"customer_id": ["a", "b"]})
    child = pl.DataFrame({"customer_id": ["a", "z", "y", "b"]})
    r = c.check_referential_integrity("ds", child, parent, "customer_id", 0.1, 0.9)
    assert r.affected_records == 2 and r.severity is Severity.WARNING


def test_freshness_levels():
    ref = datetime(2025, 3, 10)
    assert c.check_freshness("d", ref - timedelta(days=1), ref, 3, 14).passed
    assert c.check_freshness("d", ref - timedelta(days=5), ref, 3, 14).severity is Severity.WARNING
    assert (
        c.check_freshness("d", ref - timedelta(days=30), ref, 3, 14).severity is Severity.CRITICAL
    )
    assert c.check_freshness("d", None, ref, 3, 14).severity is Severity.CRITICAL


def test_volume_anomaly():
    hist = [100, 110, 90, 105]
    assert c.check_volume("d", 100, hist, 0.2, 5, 3).passed
    assert not c.check_volume("d", 5, hist, 0.2, 5, 3).passed
    assert not c.check_volume("d", 900, hist, 0.2, 5, 3).passed
    insufficient = c.check_volume("d", 5, [100], 0.2, 5, 3)
    assert insufficient.passed and "insufficient" in insufficient.diagnostics["note"]


def test_partition_completeness_and_coverage_chain():
    exp = [date(2025, 1, d) for d in (1, 2, 3)]
    assert c.check_partition_completeness("d", exp, exp).passed
    miss = c.check_partition_completeness("d", exp, exp[:1])
    assert miss.severity is Severity.CRITICAL and miss.affected_records == 2
    a, b, cc = datetime(2025, 1, 1), datetime(2025, 1, 2), datetime(2025, 1, 3)
    assert c.check_coverage_chain("d", [(None, a), (a, b), (b, cc)]).passed
    assert not c.check_coverage_chain("d", [(None, a), (b, cc)]).passed  # gap


def test_pandera_schema_catches_wrong_type_null_and_missing_column():
    good = pl.DataFrame(schema={**SOURCE_TX_COLUMNS, "ingestion_date": pl.Date})
    good = good.vstack(
        pl.DataFrame(
            {
                k: [v]
                for k, v in {
                    "InvoiceNo": "1",
                    "StockCode": "10001",
                    "Description": "d",
                    "Quantity": 1,
                    "InvoiceDate": TS,
                    "UnitPrice": 1.0,
                    "CustomerID": "1",
                    "Country": "UK",
                    "record_id": "r",
                    "line_seq": 0,
                    "available_at": TS,
                    "is_synthetic": True,
                    "source_name": "s",
                    "ingestion_date": date(2025, 1, 1),
                }.items()
            },
            schema={**SOURCE_TX_COLUMNS, "ingestion_date": pl.Date},
        )
    )
    schema = raw_transactions_schema()
    assert c.check_schema_pandera("raw", good, schema).passed
    wrong_type = good.with_columns(pl.col("Quantity").cast(pl.Float64))
    assert not c.check_schema_pandera("raw", wrong_type, schema).passed
    null_key = good.with_columns(pl.lit(None, dtype=pl.String).alias("InvoiceNo"))
    assert not c.check_schema_pandera("raw", null_key, schema).passed
    no_col = good.drop("UnitPrice")
    r = c.check_schema_pandera("raw", no_col, schema)
    assert not r.passed and r.severity is Severity.CRITICAL


def test_feature_consistency_detects_violations():
    base = dict(
        purchases_7d=1,
        purchases_30d=2,
        purchases_90d=3,
        total_spend_30d=10.0,
        total_spend_90d=20.0,
        unique_products_30d=1,
        avg_order_value=5.0,
        days_since_last_purchase=1,
    )
    assert c.check_feature_consistency("f", pl.DataFrame([base])).passed
    bad = pl.DataFrame([{**base, "purchases_7d": 9}])
    r = c.check_feature_consistency("f", bad)
    assert not r.passed and "purchases_7d<=purchases_30d" in r.diagnostics["violations"]


def test_report_blocks_on_critical_only():
    rep = ValidationReport("stage")
    rep.results.append(CheckResult("d", "w", False, Severity.WARNING, 1, 10))
    rep.raise_if_critical()  # warnings do not block
    rep.results.append(CheckResult("d", "c", False, Severity.CRITICAL, 1, 10))
    with pytest.raises(CriticalValidationError) as ei:
        rep.raise_if_critical()
    assert "d.c" in str(ei.value) and rep.summary() == {
        "checks": 2,
        "passed": 0,
        "warnings": 1,
        "critical": 1,
    }


def test_report_artifacts_are_written(tmp_path):
    rep = ValidationReport("s")
    rep.results.append(CheckResult("d", "chk", True, Severity.INFO, 0, 5))
    payload = merge_reports([rep])
    path = write_quality_report(tmp_path, "r1", payload)
    assert path.exists() and (tmp_path / "r1.md").read_text().startswith("# Quality report")


def test_raw_suite_stops_early_on_schema_failure():
    tx = pl.DataFrame({"InvoiceNo": ["1"]})
    ev = pl.DataFrame(schema=SOURCE_EVENT_COLUMNS).with_columns(
        pl.lit(None, dtype=pl.Date).alias("ingestion_date")
    )
    rep = validate_raw(
        tx,
        ev,
        th=QualityThresholds(),
        tx_volume=(1, []),
        coverage={"transactions": [], "customer_events": []},
    )
    assert not rep.passed and len(rep.results) == 2


def test_feature_suite_flags_schema_problem_as_critical():
    rep = validate_features(
        pl.DataFrame({"customer_id": ["a"]}), cutoff=TS, previous_rows=[], th=QualityThresholds()
    )
    assert not rep.passed
