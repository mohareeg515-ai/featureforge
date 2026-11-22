# Implementation log and progress checklist

Everything below was actually executed in the build environment (Ubuntu 24.04, 1 CPU, 4 GB RAM,
Python 3.12.3, uv 0.11.32) unless marked otherwise. Nothing here is estimated.

## Environment findings

| Item | Finding |
|---|---|
| Docker, Terraform | not installed -> image build and `terraform validate/plan` **not run** |
| archive.ics.uci.edu | unreachable (outbound allow-list) -> real dataset **not downloaded/verified** |
| PyPI | reachable -> all Python dependencies installed and locked (`uv.lock`) |
| Resolved versions | polars 2.1.0, duckdb 2.0.0, pyarrow 26.0.1, pydantic 2.14.4, pandera 0.36.0, prefect 3.9.4, pytest 9.2.0, ruff 0.17.6, mypy 2.4.1 |

## Phase checklist

| Phase | Acceptance criteria | Status |
|---|---|---|
| 1 Requirements/architecture | architecture diagram, decisions, dataset decision | Done (`docs/architecture.md`); dataset: synthetic default, real optional (not verifiable here) |
| 2 Foundation | package, deps, config, logging, test framework | Done |
| 3 Ingestion | incremental, immutable raw, provenance, retries, invalid schema, duplicates tests | Done |
| 4 Processing/features | 12 features in one SQL, window-boundary + hand-computed + reference cross-check tests | Done |
| 5 Data quality | 10 required check types, severities, blocking critical failures, diagnostics | Done |
| 6 Feature store | partitioned Parquet, atomic publish, checksum, as-of retrieval, history, DuckDB queries | Done |
| 7 Orchestration | 9 Prefect tasks, retries/timeouts, backfill flow, idempotency, recovery | Done (real Prefect flows tested) |
| 8 Cloud infra | Dockerfiles, Terraform, workflows prepared | **Prepared, not validated** (no Docker/Terraform here) |
| 9 Monitoring + ML | run registry, dashboard, ML demo | Done (dashboard tested with Streamlit AppTest, not visually) |
| 10 Final validation | full test run, lint, types, security review, docs | Done for local scope; see limitations |

## Final verification results

* `pytest`: **115 passed** (88 unit, 23 integration, 4 e2e), ~40-70 s.
* `ruff check` and `ruff format --check`: clean. `mypy --strict` on `src/featureforge` (41 files): no issues.
* Demo run through the CLI: 22 weekly snapshots 2025-04-01..2025-08-26 in 5 s; `stale` returns `[]`;
  every run 41 checks, 0 critical. Sample outputs in `examples/`.
* ML demo (synthetic data): train/test positive rate 74.6% / 72.8% (heavily imbalanced toward positives
  because the observation universe is customers with a purchase in the last 90 days); logistic
  regression ROC-AUC 0.7317, average precision 0.8732 vs prior baseline 0.5 / 0.7276.
  Synthetic: demonstrates plumbing only.
* Benchmarks: `docs/benchmarks.md` (measured with `scripts/benchmark.py`).

## Defects found and fixed during the build

* DuckDB 2.0 removed `.pl()` on results -> Arrow conversion helper.
* `execute().fetchone()` left an unfinished result that broke the next call -> `sql().fetchall()`.
* A walrus expression inside `select` shadowed a module constant (ML demo).
* `--force` did not reach raw ingestion; added it plus an explicit `restate` command after
  realising corrections could not otherwise be applied.
* Data-dependent CLI commands crashed with raw tracebacks on an empty data root -> clear message.
* Prefect analytics wrote to SQLite at start-up and printed a "database is locked" error -> disabled by default.
* Several of my own tests initially asserted vacuous conditions (e.g. `... or True`); rewritten
  to assert real properties.

## Security review (local scope)

No credentials in code/IaC (asserted by tests); task role limited to one S3 prefix; CI has no secrets;
deploy workflow is manual, environment-gated, OIDC-only and pushes images only; bucket private,
encrypted, versioned. Not reviewed: container image CVEs, dependency audit, threat model.

## Cloud deployment status

**Deployment prepared.** Not deployed, no deployment attempted, nothing verified in AWS.

## Open items

Run `terraform validate`, `docker build`, and CI once in a networked environment; download the
real Online Retail file and run the pipeline on it; Athena DDL check.
