"""Reproducible benchmark: CSV vs Parquet workflows, memory-bounded execution, partition pruning.

Usage:  python scripts/benchmark.py --lines 2000000 --repeats 3 --out /tmp/ff_bench

Every number written to docs/benchmarks.md is measured by this script on the machine that
runs it; nothing is estimated. Each workload runs in its own subprocess so peak memory
(``ru_maxrss`` from ``wait4``) is per-workload.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb
import polars as pl

CUTOFF = datetime(2025, 10, 1)
TX_COLUMNS = (
    "{'record_id':'VARCHAR','invoice_no':'VARCHAR','stock_code':'VARCHAR','description':'VARCHAR',"
    "'quantity':'BIGINT','unit_price':'DOUBLE','invoice_ts':'TIMESTAMP','available_at':'TIMESTAMP',"
    "'customer_id':'VARCHAR','country':'VARCHAR','is_synthetic':'BOOLEAN','source_name':'VARCHAR',"
    "'ingestion_date':'DATE'}"
)
EV_COLUMNS = (
    "{'event_id':'VARCHAR','customer_id':'VARCHAR','event_type':'VARCHAR','event_ts':'TIMESTAMP',"
    "'available_at':'TIMESTAMP','is_synthetic':'BOOLEAN','source_name':'VARCHAR',"
    "'ingestion_date':'DATE'}"
)


def generate(out: Path, lines: int) -> dict[str, float]:
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    customers = max(1000, lines // 40)
    tx_select = f"""
        SELECT 'R' || i AS record_id, 'INV' || (i // 3) AS invoice_no,
               CAST(10000 + (hash(i) % 400) AS VARCHAR) AS stock_code, 'x' AS description,
               CAST(1 + (hash(i * 5) % 5) AS BIGINT) AS quantity,
               CAST(1 + (hash(i * 3) % 5000) / 100.0 AS DOUBLE) AS unit_price,
               TIMESTAMP '2025-01-01' + to_seconds(CAST(hash(i // 3 * 7) % (270 * 86400) AS BIGINT)) AS invoice_ts,
               TIMESTAMP '2025-01-01' + to_seconds(CAST(hash(i // 3 * 7) % (270 * 86400) AS BIGINT))
                   + INTERVAL 1 HOUR AS available_at,
               CAST(hash(i // 3) % {customers} AS VARCHAR) AS customer_id, 'UK' AS country,
               TRUE AS is_synthetic, 'gen' AS source_name, DATE '2025-01-01' AS ingestion_date
        FROM range({lines}) t(i)"""
    ev_select = f"""
        SELECT 'E' || i AS event_id, CAST(hash(i) % {customers} AS VARCHAR) AS customer_id,
               CASE WHEN i % 2 = 0 THEN 'session_start' ELSE 'add_to_cart' END AS event_type,
               TIMESTAMP '2025-01-01' + to_seconds(CAST(hash(i * 11) % (270 * 86400) AS BIGINT)) AS event_ts,
               TIMESTAMP '2025-01-01' + to_seconds(CAST(hash(i * 11) % (270 * 86400) AS BIGINT))
                   + INTERVAL 5 MINUTE AS available_at,
               TRUE AS is_synthetic, 'gen' AS source_name, DATE '2025-01-01' AS ingestion_date
        FROM range({lines // 2}) t(i)"""
    for name, sel in (("transactions", tx_select), ("customer_events", ev_select)):
        con.sql(f"COPY ({sel}) TO '{out}/{name}.parquet' (FORMAT parquet, COMPRESSION zstd)")
        con.sql(f"COPY ({sel}) TO '{out}/{name}.csv' (FORMAT csv, HEADER)")
    return {p.name: round(p.stat().st_size / 1e6, 1) for p in sorted(out.glob("*.*"))}


def _connect(kind: str, data: Path, memory_limit: str | None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    if memory_limit:
        con.execute(f"SET memory_limit='{memory_limit}'")
        con.execute(f"SET temp_directory='{data}/spill'")
    con.execute("SET threads=1")
    if kind == "csv":
        con.execute(
            f"CREATE VIEW transactions AS SELECT * FROM read_csv('{data}/transactions.csv', "
            f"header=true, columns={TX_COLUMNS})"
        )
        con.execute(
            f"CREATE VIEW customer_events AS SELECT * FROM read_csv('{data}/customer_events.csv', "
            f"header=true, columns={EV_COLUMNS})"
        )
    else:
        con.execute(
            f"CREATE VIEW transactions AS SELECT * FROM read_parquet('{data}/transactions.parquet')"
        )
        con.execute(
            f"CREATE VIEW customer_events AS SELECT * FROM read_parquet('{data}/customer_events.parquet')"
        )
    return con


def worker(name: str, data: Path) -> None:
    from featureforge.features.engine import _sql_text

    kind = "csv" if "csv" in name else "parquet"
    limit = "256MB" if "lowmem" in name else None
    con = _connect(kind, data, limit)
    if name.startswith("features"):
        sql, params = _sql_text(), {"cutoff": CUTOFF}
        t0 = time.perf_counter()
        rows = len(con.sql(sql, params=params).fetchall())
    elif name.startswith("projection"):
        sql = "SELECT customer_id, sum(quantity * unit_price) AS s FROM transactions GROUP BY 1"
        t0 = time.perf_counter()
        rows = len(con.sql(sql).fetchall())
    else:
        raise SystemExit(f"unknown worker {name}")
    print(json.dumps({"query_s": time.perf_counter() - t0, "rows": rows}))


def run_worker(name: str, data: Path) -> dict[str, float]:
    t0 = time.perf_counter()
    proc = subprocess.Popen(
        [sys.executable, __file__, "--worker", name, "--data", str(data)],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None
    out = proc.stdout.read()
    _, status, usage = os.wait4(proc.pid, 0)
    if status != 0:
        raise RuntimeError(f"worker {name} failed with status {status}")
    result = json.loads(out.strip().splitlines()[-1])
    result["total_s"] = time.perf_counter() - t0
    result["peak_rss_mb"] = usage.ru_maxrss / 1024
    return result


def partition_pruning(out: Path, partitions: int = 60, rows: int = 50_000) -> dict[str, float]:
    store = out / "store"
    con = duckdb.connect()
    for p in range(partitions):
        d = store / f"snapshot_date=2025-{1 + p // 28:02d}-{1 + p % 28:02d}"
        d.mkdir(parents=True, exist_ok=True)
        con.sql(f"""COPY (SELECT CAST(i AS VARCHAR) AS customer_id,
                          CAST(hash(i + {p}) % 20 AS BIGINT) AS purchases_30d,
                          CAST((hash(i * 3 + {p}) % 100000) / 100.0 AS DOUBLE) AS total_spend_30d
                   FROM range({rows}) t(i)) TO '{d}/part-0.parquet' (FORMAT parquet, COMPRESSION zstd)""")
    glob = f"{store}/snapshot_date=*/part-0.parquet"
    con.sql(f"CREATE VIEW cf AS SELECT * FROM read_parquet('{glob}', hive_partitioning=true)")

    def timed(sql: str) -> float:
        runs = []
        for _ in range(5):
            t0 = time.perf_counter()
            con.sql(sql).fetchall()
            runs.append(time.perf_counter() - t0)
        return statistics.median(runs)

    one = "SELECT sum(total_spend_30d) FROM cf WHERE snapshot_date = DATE '2025-01-15'"
    allp = "SELECT sum(total_spend_30d) FROM cf"
    plan = str(con.sql(f"EXPLAIN {one}").fetchall())
    return {
        "partitions": partitions,
        "rows_per_partition": rows,
        "one_partition_s": timed(one),
        "all_partitions_s": timed(allp),
        "plan_mentions_filter_pushdown": "snapshot_date" in plan,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lines", type=int, default=2_000_000)
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--out", type=Path, default=Path("/tmp/ff_bench"))
    ap.add_argument("--worker")
    ap.add_argument("--data", type=Path)
    ap.add_argument("--report", type=Path, default=Path("docs/benchmarks.md"))
    a = ap.parse_args()
    if a.worker:
        worker(a.worker, a.data)
        return

    sizes = generate(a.out, a.lines)
    names = [
        "features_csv",
        "features_parquet",
        "features_parquet_lowmem",
        "projection_csv",
        "projection_parquet",
    ]
    results: dict[str, dict[str, float]] = {}
    for n in names:
        runs = [run_worker(n, a.out) for _ in range(a.repeats)]
        results[n] = {
            "query_s": statistics.median(r["query_s"] for r in runs),
            "total_s": statistics.median(r["total_s"] for r in runs),
            "peak_rss_mb": max(r["peak_rss_mb"] for r in runs),
            "rows": runs[0]["rows"],
        }
        print(n, results[n], flush=True)
    pruning = partition_pruning(a.out)
    print("pruning", pruning)

    def x(a_: str, b_: str) -> str:
        return f"{results[a_]['query_s'] / results[b_]['query_s']:.2f}x"

    md = (
        f"""# Benchmarks (measured, not estimated)

Generated by `scripts/benchmark.py --lines {a.lines} --repeats {a.repeats}`.

**Environment**: {platform.platform()}, {os.cpu_count()} CPU(s), DuckDB {duckdb.__version__},
Polars {pl.__version__}, Python {platform.python_version()}. DuckDB `threads=1`. Data are
SYNTHETIC rows generated by the script (transactions: {a.lines:,} lines; events: {a.lines // 2:,}).
Medians of {a.repeats} runs, each in a fresh process; `peak RSS` is the maximum over runs.

## Data size on disk (MB)

| File | MB |
|---|---:|
"""
        + "\n".join(f"| {k} | {v} |" for k, v in sizes.items())
        + """

## Feature computation (the production SQL, full snapshot at one cutoff)

| Workflow | Query time (s) | Process total (s) | Peak RSS (MB) | Output rows |
|---|---:|---:|---:|---:|
"""
        + "\n".join(
            f"| {n} | {r['query_s']:.2f} | {r['total_s']:.2f} | {r['peak_rss_mb']:.0f} | {int(r['rows']):,} |"
            for n, r in results.items()
            if n.startswith("features")
        )
        + f"""

CSV / Parquet query-time ratio: **{x("features_csv", "features_parquet")}**.

## Column projection (2 of 13 columns: `GROUP BY customer_id, sum(quantity*unit_price)`)

| Workflow | Query time (s) | Peak RSS (MB) |
|---|---:|---:|
"""
        + "\n".join(
            f"| {n} | {r['query_s']:.2f} | {r['peak_rss_mb']:.0f} |"
            for n, r in results.items()
            if n.startswith("projection")
        )
        + f"""

CSV / Parquet ratio: **{x("projection_csv", "projection_parquet")}**.

## Memory-bounded execution

`features_parquet_lowmem` runs the same query with DuckDB `memory_limit=256MB` and a spill
directory. It completed successfully on this dataset (see peak RSS above). This shows
single-node, out-of-core capable processing; it is **not** a horizontal-scaling claim.

## Partition pruning (feature store query)

{pruning["partitions"]} snapshot partitions x {pruning["rows_per_partition"]:,} rows.

| Query | Median time (s) |
|---|---:|
| one partition (`WHERE snapshot_date = ...`) | {pruning["one_partition_s"]:.4f} |
| all partitions | {pruning["all_partitions_s"]:.4f} |

Ratio all/one: **{pruning["all_partitions_s"] / pruning["one_partition_s"]:.1f}x**.

## Caveats

* Single machine, 1 thread, synthetic data; ratios depend on hardware, data shape and DuckDB
  version. DuckDB's CSV reader is fast, so the gap is smaller than with naive CSV tooling.
* Query time excludes process start-up and import cost; "Process total" includes it.
"""
    )
    a.report.write_text(md)
    print(f"wrote {a.report}")


if __name__ == "__main__":
    main()
