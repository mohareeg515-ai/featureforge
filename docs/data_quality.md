# Data quality framework

Code: `src/featureforge/validation/` (`checks.py` library, `suites.py` per-stage suites,
`results.py` result model). Thresholds: `QualityThresholds` in `config/settings.py`,
overridable via YAML or `FF_THRESHOLDS__<NAME>` environment variables.

## Result model

Every check returns a `CheckResult` with: `dataset`, `check`, `executed_at`,
`affected_records`, `total_records`, `severity` (`info` when passed, else `warning` or
`critical`), `passed`, and `diagnostics` (thresholds, fractions, samples).

* **warning** - recorded in reports and the run record; publication continues.
* **critical** - raises `CriticalValidationError`; the pipeline stops **before** publication.
  The failed run, its error category (`validation`) and the full diagnostic report
  (`reports/quality/snapshot_date=D/<run_id>.json|md`) are still persisted.

## Required checks and where they run

| # | Check | Function | Stage / dataset | Severity |
|---|---|---|---|---|
| 1 | Missing values | `check_missing` | raw `CustomerID`, events `customer_id`; normalized `customer_id`; features | threshold (warn/critical) |
| 2 | Duplicates | `check_duplicates` | raw rows, raw record versions, events, feature entity keys | threshold / critical |
| 3 | Schema | `check_schema_pandera` | raw transactions and events (Pandera, lazy) | critical |
| 4 | Types | `check_dtypes`, `schema_problems` | features vs registry | critical |
| 5 | Ranges | `check_range` | quantity>0, price>0, counts>=0, spend>=0, frequency>0 | critical |
| 6 | Referential integrity | `check_referential_integrity` | events -> transactions customers | threshold |
| 7 | Freshness | `check_freshness` | latest *available* transaction vs cutoff | warn after 3d / critical after 14d |
| 8 | Volume anomalies | `check_volume` | latest raw delta and feature entity count vs median of history | warning |
| 9 | Partition completeness | `check_coverage_chain`, `check_partition_completeness` | raw coverage windows must be contiguous | critical |
| 10 | Feature consistency | `check_feature_consistency` | `purchases_7d<=30d<=90d`, spend monotonic, AOV/recency null together, spend 0 iff no orders | critical |

Additional: `check_nonempty`, `check_allowed_values` (event types), quarantine-rate check
(`rejected_fraction`), `snapshot_ts == cutoff`, and a training/inference *value* comparison in
the test-suite (`value_mismatches`) rather than a name-only comparison.

The raw suite stops after a failed schema check, because later checks on a malformed frame only
produce noise.

## Defaults (`QualityThresholds`)

| Name | Warn | Critical |
|---|---:|---:|
| missing customer id (raw) | 35% | 90% |
| duplicate rows | 2% | 25% |
| rejected by cleaning | 40% | 90% |
| orphan events | 5% | 50% |
| freshness (days) | 3 | 14 |
| volume ratio vs median | outside [0.2, 5] | - |

The 35% missing-customer warning is deliberately above the ~25% seen in Online Retail; tune
per source.

## Diagnostic artifacts

* `rejected/<source>.parquet` - quarantined rows with `reject_reason`.
* `reports/quality/snapshot_date=D/<run_id>.{json,md}` - immutable per run;
  `latest.{json,md}` - convenience pointer.
* `metadata/runs.jsonl` - run state, durations, validation counts, error category.

A real report from the demo run is in [`examples/sample_quality_report.md`](../examples/sample_quality_report.md)
(41 checks on the synthetic demo data).
