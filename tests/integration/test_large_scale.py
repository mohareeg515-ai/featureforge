"""Memory-bounded processing on a generated larger dataset (DuckDB out-of-core + Polars lazy)."""

from __future__ import annotations

from datetime import datetime

import duckdb
import polars as pl
import pytest

from featureforge.features.definitions import feature_schema
from featureforge.features.engine import FeatureEngine

pytestmark = pytest.mark.integration
N_LINES = 400_000
CUTOFF = datetime(2025, 10, 1)


@pytest.fixture(scope="module")
def big(tmp_path_factory):
    d = tmp_path_factory.mktemp("big")
    con = duckdb.connect()
    con.sql(f"""
        COPY (
          SELECT 'R' || i AS record_id,
                 'INV' || (i // 3) AS invoice_no,
                 CAST(10000 + (hash(i) % 400) AS VARCHAR) AS stock_code,
                 'x' AS description,
                 CAST(1 + (hash(i * 5) % 5) AS BIGINT) AS quantity,
                 CAST(1 + (hash(i * 3) % 5000) / 100.0 AS DOUBLE) AS unit_price,
                 TIMESTAMP '2025-01-01' + to_seconds(CAST(hash(i // 3 * 7) % (270 * 86400) AS BIGINT))
                     AS invoice_ts,
                 TIMESTAMP '2025-01-01' + to_seconds(CAST(hash(i // 3 * 7) % (270 * 86400) AS BIGINT))
                     + INTERVAL 1 HOUR AS available_at,
                 CAST(hash(i // 3) % 20000 AS VARCHAR) AS customer_id,
                 'UK' AS country, TRUE AS is_synthetic, 'gen' AS source_name,
                 DATE '2025-01-01' AS ingestion_date
          FROM range({N_LINES}) t(i)
        ) TO '{d}/transactions.parquet' (FORMAT parquet, COMPRESSION zstd)
    """)
    con.sql(f"""
        COPY (
          SELECT 'E' || i AS event_id, CAST(hash(i) % 20000 AS VARCHAR) AS customer_id,
                 CASE WHEN i % 2 = 0 THEN 'session_start' ELSE 'add_to_cart' END AS event_type,
                 TIMESTAMP '2025-01-01' + to_seconds(CAST(hash(i * 11) % (270 * 86400) AS BIGINT))
                     AS event_ts,
                 TIMESTAMP '2025-01-01' + to_seconds(CAST(hash(i * 11) % (270 * 86400) AS BIGINT))
                     + INTERVAL 5 MINUTE AS available_at,
                 TRUE AS is_synthetic, 'gen' AS source_name, DATE '2025-01-01' AS ingestion_date
          FROM range({N_LINES // 2}) t(i)
        ) TO '{d}/customer_events.parquet' (FORMAT parquet, COMPRESSION zstd)
    """)
    return d


def test_features_under_a_small_memory_limit_match_a_polars_lazy_computation(big):
    with FeatureEngine(
        big / "transactions.parquet",
        big / "customer_events.parquet",
        memory_limit="256MB",
        threads=1,
        temp_directory=big / "spill",
    ) as eng:
        df = eng.compute(CUTOFF)
    assert df.height > 15_000 and dict(df.schema) == feature_schema()

    lazy = (  # independent lazy/streaming-capable computation
        pl.scan_parquet(big / "transactions.parquet")
        .filter((pl.col("invoice_ts") < CUTOFF) & (pl.col("available_at") <= CUTOFF))
        .group_by(["customer_id", "invoice_no"])
        .agg(pl.col("invoice_ts").min().alias("t"))
        .filter(pl.col("t") >= datetime(2025, 9, 1))  # CUTOFF - 30 days
        .select(pl.len())
        .collect()
        .item()
    )
    assert int(df["purchases_30d"].sum()) == lazy
    assert (df["purchases_7d"] <= df["purchases_30d"]).all()
    assert (df["purchases_30d"] <= df["purchases_90d"]).all()
