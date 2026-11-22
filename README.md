# FeatureForge — Automated ML Feature Store & Data Pipeline Platform

A working, test-covered feature platform for an e-commerce use case: incremental ingestion,
cleaning, SQL feature engineering, data-quality gates, **point-in-time-correct** historical
snapshots in partitioned Parquet, idempotent Prefect orchestration with backfills, S3 state sync,
an operational dashboard, and a downstream ML demo. AWS infrastructure (Terraform, Docker, OIDC CI)
is **prepared but not deployed**.

> **Honest status.** Local pipeline, tests, benchmarks: executed (see
> [docs/IMPLEMENTATION_LOG.md](docs/IMPLEMENTATION_LOG.md)). Default data are **synthetic**
> (flagged `is_synthetic`); the real UCI Online Retail file could not be downloaded in the build
> environment, so the real-data reader is tested only on a small hand-written fixture in the same
> layout. Cloud: **Deployment prepared** (not deployed, not verified). Docker build and
> `terraform validate` were not run here (tools unavailable); CI runs them.

## 1. Business problem

Teams retraining and serving churn/propensity models suffer from (a) features recomputed differently
in training and serving, (b) leakage of future data into history, and (c) pipelines that cannot be
re-run safely. FeatureForge computes one set of customer features from one SQL definition,
snapshots them daily, and serves them for training (as-of joins) and inference (latest snapshot).

## 2. Architecture

See [docs/architecture.md](docs/architecture.md) (Mermaid diagrams, module boundaries, ADRs).

```
sources -> ingest (incremental, immutable raw) -> validate raw -> normalize (+quarantine)
 -> DuckDB feature SQL -> validate features -> staged atomic publish -> feature store (Parquet)
 -> get_training_features (as-of join)  /  get_latest_features (inference)  /  Athena or DuckDB
```

## 3. Technology stack

Python 3.12, Polars 2.1, DuckDB 2.0, PyArrow, Pydantic v2 (+settings), Pandera (raw schemas),
Prefect **3.9.4** (pinned), scikit-learn (demo), Streamlit (dashboard), boto3, pytest, Ruff, MyPy
(strict), Docker, GitHub Actions, Terraform (AWS: S3, ECR, ECS Fargate, EventBridge Scheduler,
CloudWatch, Glue, Athena, IAM, Budgets). Dependencies are locked in `uv.lock`.
Pandas is deliberately not used (not needed; Polars/DuckDB are enough).

## 4. Data sources

Default: seeded **synthetic** transactions (Online-Retail schema, with realistic defects) and
**synthetic** session/cart events. Optional: the real UCI Online Retail dataset
(`featureforge download-retail`, then `FF_SOURCE_KIND=retail_file`). Provenance, limitations and
the verification status are in [docs/data_dictionary.md](docs/data_dictionary.md). Raw data is never
committed.

## 5. Feature definitions

12 customer features (`purchases_7d/30d/90d`, `total_spend_30d/90d`, `avg_order_value`,
`days_since_last_purchase`, `unique_products_30d`, `purchase_frequency`,
`customer_lifetime_value_proxy`, `sessions_7d`, `cart_events_7d`), each with name, definition,
entity key, snapshot timestamp, dtype, null policy, source, calculation and tests:
[docs/feature_definitions.md](docs/feature_definitions.md) (generated from the registry; a test
prevents drift). Session/cart features are synthetic; the CLV feature is a proxy, not a model.

## 6. Repository structure

```
src/featureforge/{config,ingestion,processing,features,validation,storage,orchestration,monitoring,ml,utils}
src/featureforge/features/sql/customer_features.sql   # the single feature SQL (packaged with the wheel)
tests/{unit,integration,e2e}   infrastructure/terraform   docker/   scripts/   configs/   docs/
dashboards/app.py   examples/   .github/workflows/{ci,deploy}.yml   Dockerfile   docker-compose.yml
```

`sql/` lives inside the package (not at repo root) so it ships in the wheel and Docker image.

## 7-8. Local installation and configuration

```bash
git clone <repo> && cd featureforge
uv sync --extra dev          # Python 3.12; creates .venv from uv.lock
cp .env.example .env         # optional; every setting is an FF_* env var (see configs/dev.yaml)
```

Precedence: explicit overrides > `FF_*` env > YAML (`--config`) > defaults. Nested thresholds:
`FF_THRESHOLDS__DUPLICATE_WARN=0.1`. No credentials are ever read from files; AWS uses the IAM
role/profile.

## 9. Example pipeline execution

```bash
export FF_DATA_ROOT=./data
# one snapshot (Prefect-orchestrated); add --no-prefect for the plain executor
uv run featureforge run --date 2025-03-01
# historical backfill, weekly cadence (22 snapshots, ~5 s on 1 CPU in the demo)
uv run featureforge backfill --start 2025-04-01 --end 2025-08-26 --step-days 7 --no-prefect
uv run featureforge query "SELECT snapshot_date, count(*) FROM customer_features GROUP BY 1 ORDER BY 1"
uv run featureforge ml-demo          # downstream model from the feature store
uv run streamlit run dashboards/app.py
```

Real demo outputs: [examples/sample_quality_report.md](examples/sample_quality_report.md),
[examples/sample_ml_report.json](examples/sample_ml_report.json).

Python API:

```python
from featureforge.storage.feature_store import FeatureStore
store = FeatureStore("data/feature_store")
train = store.get_training_features(observations)           # point-in-time as-of join
live  = store.get_latest_features(["12001", "12002"])       # inference, same definitions
```

## 10. Data quality

41 checks per run across raw / normalized / feature stages (missing, duplicates, Pandera schema,
types, ranges, referential integrity, freshness, volume anomaly, partition completeness, feature
consistency). Warnings are reported; **critical failures block publication** and keep diagnostics.
[docs/data_quality.md](docs/data_quality.md).

## 11. Backfills, idempotency, corrections

[docs/backfills.md](docs/backfills.md). Re-running a batch is a no-op (`skipped_duplicate`);
failed backfills resume by re-running the same range; `restate` + `stale` + `backfill --stale-only`
rebuild only the partitions affected by source corrections.

## 12. Point-in-time correctness

[docs/point_in_time.md](docs/point_in_time.md): event time vs availability time, versioned
corrections, as-of retrieval, a worked leaky-join example and the tests that prove it.

## 13. AWS deployment

[docs/cloud_deployment.md](docs/cloud_deployment.md), [docs/cost_management.md](docs/cost_management.md).
EventBridge owns the single schedule; Prefect runs in-process in the Fargate task; OIDC (no keys);
manual approval; no NAT Gateway; budgets; destroy guide. **Nothing is deployed.**

## 14. Testing

```bash
uv run pytest                    # everything (≈70 s on 1 CPU)
uv run pytest tests/unit         # fast
uv run pytest tests/integration tests/e2e
uv run ruff check src tests dashboards scripts && uv run mypy
```

115 tests (88 unit, 23 integration incl. real Prefect flows and S3 via the **moto emulator**,
4 end-to-end through the CLI and dashboard). No paid or cloud services are needed.

## 15. Monitoring

JSON logs with `run_id`; `metadata/runs.jsonl` (durations, rows, validation counts, error
category, freshness); quality reports; Streamlit dashboard reading only real artifacts
(shows an empty state, never fabricated numbers).

## 16. Benchmarks (measured)

[docs/benchmarks.md](docs/benchmarks.md), reproducible with `scripts/benchmark.py`. On the
build machine (1 CPU, 2M-line synthetic table): Parquet is ~10x smaller than CSV (19.7 vs 206 MB),
the full feature query ran 1.59x faster from Parquet, a 2-column projection 5.2x faster, and a
60-partition store query for one partition ran ~10x faster than scanning all. The same query also
completed under a 256 MB DuckDB memory limit. Single-node only.

## 17. Known limitations

* Default data are synthetic; ML metrics on them demonstrate plumbing, **not** real-world skill.
* Real Online Retail path validated only on a tiny fixture; XLSX path needs the optional
  `real-data` extra and is untested.
* Single-node processing; no horizontal scaling is claimed or tested. Normalization rewrites the
  normalized tables each run and raw validation loads raw data into memory (fine at this scale).
* Corrections create versions only if customer/event time are unchanged; no tombstones.
* The local run lock is not distributed; S3 sync is not transactional across objects.
* Cloud: Terraform/Docker/workflows unexecuted; Athena DDL unverified on real Athena.
* Real-data `available_at` is an assumption (invoice time + 2 h).
* Not claimed production-ready: no SLOs, load tests, security review or on-call runbook.

## 18. Future improvements

Tombstone/delete semantics; incremental normalization; Glue/Athena integration test; distributed
lock (DynamoDB conditional write); online store (e.g. DynamoDB) for low-latency serving;
drift/skew monitoring; more entity types (products).

## Resume / GitHub description

*FeatureForge — a cloud-ready ML feature store built with Python, Polars, DuckDB and Prefect.
Ingests e-commerce transactions incrementally, validates data quality with blocking gates, and
publishes 12 point-in-time-correct customer features as partitioned Parquet snapshots with
idempotent, backfillable runs and a training/inference-consistent retrieval API. 115 automated
tests, strict typing, CI, and least-privilege AWS infrastructure as code (Terraform, ECS Fargate,
EventBridge, S3, Athena) prepared with cost guardrails.*

License: MIT.
