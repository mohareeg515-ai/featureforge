"""DuckDB-backed feature computation over Parquet (out-of-core capable)."""

from __future__ import annotations

import hashlib
from datetime import datetime
from importlib import resources
from pathlib import Path
from types import TracebackType
from typing import Any

import duckdb
import polars as pl

from featureforge.features.definitions import ENTITY_KEY, FEATURE_NAMES, SNAPSHOT_TS


def _sql_text() -> str:
    return (resources.files("featureforge.features") / "sql" / "customer_features.sql").read_text()


def _to_polars(
    con: duckdb.DuckDBPyConnection, sql: str, params: dict[str, Any] | None = None
) -> pl.DataFrame:
    """Run ``sql`` and return a polars frame via Arrow (stable across DuckDB versions)."""
    df = pl.from_arrow(con.sql(sql, params=params).to_arrow_table())
    assert isinstance(df, pl.DataFrame)
    return df


def _q(path: Path) -> str:
    return str(path).replace("'", "''")


class FeatureEngine:
    """Computes the full customer feature table for a cutoff timestamp.

    DuckDB scans the Parquet files lazily, applies column projection and can spill
    to ``temp_directory`` when ``memory_limit`` is exceeded.
    """

    def __init__(
        self,
        transactions: Path,
        events: Path,
        *,
        memory_limit: str = "1GB",
        threads: int = 1,
        temp_directory: Path | None = None,
    ) -> None:
        self._con = duckdb.connect()
        self._con.execute(f"SET memory_limit='{memory_limit}'")
        self._con.execute(f"SET threads={int(threads)}")
        if temp_directory is not None:
            temp_directory.mkdir(parents=True, exist_ok=True)
            self._con.execute(f"SET temp_directory='{_q(temp_directory)}'")
        self._con.execute(
            f"CREATE VIEW transactions AS SELECT * FROM read_parquet('{_q(transactions)}')"
        )
        self._con.execute(
            f"CREATE VIEW customer_events AS SELECT * FROM read_parquet('{_q(events)}')"
        )
        self._sql = _sql_text()

    def compute(self, cutoff: datetime) -> pl.DataFrame:
        result = _to_polars(self._con, self._sql, {"cutoff": cutoff})
        return result.select([ENTITY_KEY, SNAPSHOT_TS, *FEATURE_NAMES])

    def input_fingerprint(self, cutoff: datetime) -> str:
        """Fingerprint of exactly the records eligible at ``cutoff``.

        If a correction changes only data that becomes available *after* ``cutoff``, the
        fingerprint of that earlier snapshot is unchanged (so it is not invalidated).
        """
        row: Any = self._con.sql(
            """
            SELECT
              (SELECT count(*) || ':' || coalesce(sum(hash(
                   record_id || '|' || CAST(available_at AS VARCHAR) || '|' ||
                   CAST(quantity AS VARCHAR) || '|' || CAST(unit_price AS VARCHAR))), 0)
               FROM transactions
               WHERE invoice_ts < CAST($cutoff AS TIMESTAMP)
                 AND available_at <= CAST($cutoff AS TIMESTAMP)),
              (SELECT count(*) || ':' || coalesce(sum(hash(
                   event_id || '|' || CAST(available_at AS VARCHAR))), 0)
               FROM customer_events
               WHERE event_ts < CAST($cutoff AS TIMESTAMP)
                 AND available_at <= CAST($cutoff AS TIMESTAMP))
            """,
            params={"cutoff": cutoff},
        ).fetchall()[0]
        return hashlib.sha256(f"{row[0]}#{row[1]}".encode()).hexdigest()[:32]

    def close(self) -> None:
        self._con.close()

    def __enter__(self) -> FeatureEngine:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
