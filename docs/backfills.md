# Backfills, idempotency and corrections

## Running a backfill

```bash
# inclusive range, weekly cadence, plain Python executor
featureforge backfill --start 2025-04-01 --end 2025-08-26 --step-days 7 --no-prefect
# same thing as a parameterised Prefect flow
featureforge backfill --start 2025-04-01 --end 2025-08-26 --step-days 7
```

Dates run **ascending and sequentially**: raw ingestion is incremental (each partition is the
delta since the previous one), so order matters. `--continue-on-error` keeps going and reports
all failures at the end; default is stop at the first failure.

Measured example: 22 weekly snapshots over 2025-04-01..2025-08-26 on the 300-customer synthetic
set took 5 s end to end on 1 CPU (plain executor).

## Idempotency

* `batch_id = sha256(snapshot_date, transformation_version, fingerprint of eligible inputs)[:16]`.
* Re-running a logical batch whose published partition has the same `batch_id` *and* a valid
  checksum is a no-op: status `skipped_duplicate`, `published_at`/`publish_count` unchanged.
* `--force` rebuilds and replaces the partition (`publish_count` increments); it never creates a
  second partition. Publication writes a staging directory then renames it over the target.
* Raw ingestion for an already-ingested date is a no-op if the source content is identical, and
  **fails** (`ImmutableRawPartitionError`) if it changed.
* A per-snapshot lock (`metadata/locks`) rejects a second concurrent run of the same date.

## Recovering from a failed backfill

A failed date leaves earlier dates published and unchanged. Fix the cause and re-run the **same
range**: completed dates are `skipped_duplicate`, remaining dates run
(`tests/integration/test_pipeline.py::test_backfill_failure_recovery_resumes_without_republishing`).

## Corrections to historical source data

Two situations, handled differently:

1. **A correction arrives now** (source publishes a new version of an old record with
   `available_at` = now). Ingest it normally: it is a new version, only snapshots computed at
   cutoffs after `available_at` can see it. Earlier snapshots are *correctly* unchanged - they
   record what was known then.
2. **Restatement** (the source silently fixed old values and you want history rebuilt as if the
   data had always been right). Ordinary ingestion refuses it. Use:

```bash
featureforge restate --start 2025-03-08 --end 2025-03-08   # force re-ingest raw windows, rebuild normalized
featureforge stale                                         # list snapshots whose inputs changed
featureforge backfill --start 2025-03-01 --end 2025-03-22 --step-days 7 --stale-only
```

`stale` recomputes, for every published snapshot, the fingerprint of the records *eligible at
its cutoff*. Only snapshots whose eligible inputs changed (or whose transformation version or
file checksum changed) are listed, so a correction affecting data from 2025-03-05 invalidates
the snapshots from 2025-03-08 onward but not 2025-03-01. Verified by
`test_source_correction_invalidates_only_affected_snapshots`.

Limitations: the `record_id` includes customer and event time, so a correction that changes
those creates a new record rather than a new version; voiding a record entirely (tombstones)
is not implemented. When you bump `TRANSFORMATION_VERSION`, `stale` lists every snapshot.
