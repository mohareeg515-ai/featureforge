# Troubleshooting

| Symptom | Likely cause | What to do |
|---|---|---|
| `critical validation failures: [...]` | A critical check failed; nothing was published | Open `reports/quality/snapshot_date=D/<run_id>.md`, fix the source or thresholds, re-run the date |
| `ImmutableRawPartitionError` | The source changed history inside an already-ingested window | If it is a genuine restatement use `featureforge restate`; otherwise investigate the source |
| `RunInProgressError` | Another run holds `metadata/locks/snapshot_date=D.lock` | Wait; if a run crashed, the lock expires after 1 h (or delete the file) |
| `TransformationVersionError` | Snapshot built by an older feature version | `featureforge backfill --stale-only ...` |
| `StaleFeaturesError` / `NoSnapshotError` | Inference asked for fresher/earlier data than published | Publish a newer snapshot or relax `max_age` |
| `SourceSchemaError` | Retail file lacks expected columns | Compare with `docs/data_dictionary.md` |
| `No published snapshots found` | CLI data command run on an empty data root | Run `featureforge run` / `backfill` first; check `FF_DATA_ROOT` |
| Prefect hangs at start in a container | Prefect starts a temporary local API | Allow ~10 s start-up, or use `--no-prefect` |
| `FF_S3_BUCKET must be set` | `FF_STORAGE_BACKEND=s3` without a bucket | Set `FF_S3_BUCKET` |
| `.xlsx` read fails | Optional Excel reader missing | `uv sync --extra real-data` or convert to CSV |
| DuckDB out-of-memory | Dataset larger than `FF_DUCKDB_MEMORY_LIMIT` | Raise the limit or task memory; the temp dir is `staging/duckdb_tmp` |

Useful commands:

```bash
tail -n 20 data/metadata/runs.jsonl | python -m json.tool --json-lines   # run history
featureforge query "SELECT snapshot_date, count(*) FROM customer_features GROUP BY 1 ORDER BY 1"
```

Logs are JSON lines (`ts, level, logger, message, run_id, ...`); filter by `run_id`.
Error categories in run records: `validation`, `data_integrity`, `source_schema`, `concurrency`,
`missing_input`, `transient_io`, `configuration_or_data`, `unknown`.
