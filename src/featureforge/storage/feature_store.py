"""Historical feature store on partitioned Parquet.

Layout::

    feature_store/customer_features/snapshot_date=YYYY-MM-DD/part-0.parquet
                                                            /_metadata.json

* One partition per snapshot date (the dominant query pattern is "as of a date"), one
  compressed file per partition (avoids small-file explosion at daily cadence).
* Publication is staged then renamed, so readers never see a half-written partition.
* Retrieval for training uses an *as-of* (backward) join: an observation at time ``t``
  only ever sees a snapshot whose ``snapshot_ts <= t``.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from featureforge.features.consistency import apply_null_policy, schema_problems
from featureforge.features.definitions import (
    ENTITY_KEY,
    FEATURE_BY_NAME,
    FEATURE_NAMES,
    SNAPSHOT_TS,
    TRANSFORMATION_VERSION,
)
from featureforge.utils.fs import (
    atomic_replace_dir,
    atomic_write_json,
    new_staging_dir,
    recover_dir,
)
from featureforge.utils.hashing import sha256_file
from featureforge.utils.timeutils import snapshot_cutoff, utcnow

PARQUET = "part-0.parquet"
META = "_metadata.json"
TABLE = "customer_features"


class FeatureStoreError(RuntimeError):
    pass


class NoSnapshotError(FeatureStoreError):
    pass


class StaleFeaturesError(FeatureStoreError):
    pass


class TransformationVersionError(FeatureStoreError):
    pass


class SchemaMismatchError(FeatureStoreError):
    pass


class FeatureStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.table_dir = root / TABLE

    # ---------------------------------------------------------------- publication
    def partition_dir(self, d: date) -> Path:
        return self.table_dir / f"snapshot_date={d:%Y-%m-%d}"

    def publish(
        self,
        df: pl.DataFrame,
        snapshot_date: date,
        *,
        batch_id: str,
        input_fingerprint: str,
        run_id: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Atomically (re)publish the partition for ``snapshot_date``; returns its metadata."""
        problems = schema_problems(df)
        if problems:
            raise SchemaMismatchError("; ".join(problems))
        cutoff = snapshot_cutoff(snapshot_date)
        if (df.height and df[SNAPSHOT_TS].n_unique() != 1) or (
            df.height and df[SNAPSHOT_TS][0] != cutoff
        ):
            raise SchemaMismatchError(f"snapshot_ts must equal cutoff {cutoff}")
        if df[ENTITY_KEY].n_unique() != df.height:
            raise SchemaMismatchError("duplicate entity keys in snapshot")

        target = self.partition_dir(snapshot_date)
        recover_dir(target)
        previous = self.read_metadata(snapshot_date)
        staging = new_staging_dir(target)
        parquet = staging / PARQUET
        df.sort(ENTITY_KEY).write_parquet(parquet, compression="zstd", row_group_size=100_000)
        meta: dict[str, Any] = {
            "table": TABLE,
            "snapshot_date": snapshot_date.isoformat(),
            "snapshot_ts": cutoff.isoformat(),
            "batch_id": batch_id,
            "input_fingerprint": input_fingerprint,
            "transformation_version": TRANSFORMATION_VERSION,
            "run_id": run_id,
            "row_count": df.height,
            "columns": df.columns,
            "schema": {c: str(t) for c, t in df.schema.items()},
            "parquet_sha256": sha256_file(parquet),
            "published_at": utcnow().isoformat(),
            "publish_count": (previous or {}).get("publish_count", 0) + 1,
            **(extra or {}),
        }
        atomic_write_json(staging / META, meta)
        atomic_replace_dir(staging, target)
        return meta

    # ------------------------------------------------------------------ discovery
    def read_metadata(self, d: date) -> dict[str, Any] | None:
        p = self.partition_dir(d) / META
        if not p.exists():
            return None
        result: dict[str, Any] = json.loads(p.read_text())
        return result

    def list_snapshots(self) -> list[date]:
        if not self.table_dir.exists():
            return []
        return sorted(
            date.fromisoformat(p.name.split("=", 1)[1])
            for p in self.table_dir.glob("snapshot_date=*")
            if (p / META).exists() and (p / PARQUET).exists()
        )

    def verify(self, d: date) -> bool:
        meta = self.read_metadata(d)
        parquet = self.partition_dir(d) / PARQUET
        return (
            bool(meta)
            and parquet.exists()
            and sha256_file(parquet) == (meta or {})["parquet_sha256"]
        )

    # ------------------------------------------------------------------- reading
    def _read(self, dates: list[date], columns: list[str]) -> pl.DataFrame:
        files = [str(self.partition_dir(d) / PARQUET) for d in dates]
        if not files:
            return pl.DataFrame()
        return pl.scan_parquet(files).select(columns).collect()

    @staticmethod
    def _check_features(features: list[str] | None) -> list[str]:
        feats = list(features) if features else list(FEATURE_NAMES)
        unknown = [f for f in feats if f not in FEATURE_BY_NAME]
        if unknown:
            raise FeatureStoreError(f"unknown features: {unknown}")
        return feats

    def _check_version(self, d: date) -> None:
        meta = self.read_metadata(d)
        if meta and meta["transformation_version"] != TRANSFORMATION_VERSION:
            raise TransformationVersionError(
                f"snapshot {d} built with transformation {meta['transformation_version']}, "
                f"code is {TRANSFORMATION_VERSION}; rebuild the snapshot"
            )

    def get_latest_features(
        self,
        customer_ids: list[str] | None = None,
        features: list[str] | None = None,
        *,
        as_of: datetime | None = None,
        max_age: timedelta | None = None,
    ) -> pl.DataFrame:
        """Inference path: features from the newest snapshot available at ``as_of``."""
        feats = self._check_features(features)
        as_of = as_of or utcnow()
        eligible = [d for d in self.list_snapshots() if snapshot_cutoff(d) <= as_of]
        if not eligible:
            raise NoSnapshotError(f"no snapshot available at {as_of}")
        d = eligible[-1]
        self._check_version(d)
        ts = snapshot_cutoff(d)
        if max_age is not None and as_of - ts > max_age:
            raise StaleFeaturesError(f"latest snapshot {ts} is older than {max_age}")
        df = self._read([d], [ENTITY_KEY, SNAPSHOT_TS, *feats])
        if customer_ids is not None:
            wanted = pl.DataFrame({ENTITY_KEY: customer_ids}, schema={ENTITY_KEY: pl.String})
            df = wanted.join(df, on=ENTITY_KEY, how="left").with_columns(
                pl.col(SNAPSHOT_TS).fill_null(ts)
            )
            df = apply_null_policy(df, feats)
        return df

    def get_training_features(
        self,
        observations: pl.DataFrame,
        features: list[str] | None = None,
        *,
        max_staleness: timedelta = timedelta(days=1),
    ) -> pl.DataFrame:
        """Training path: point-in-time-correct as-of join.

        ``observations`` needs ``customer_id`` and ``observation_ts``. Each row gets the
        features of the latest snapshot with ``snapshot_ts <= observation_ts`` (and not
        older than ``max_staleness``). Entities absent from that snapshot follow the
        feature null policy; rows with no usable snapshot get NULL features and a NULL
        ``feature_snapshot_ts``.
        """
        feats = self._check_features(features)
        snaps = pl.DataFrame(
            {SNAPSHOT_TS: [snapshot_cutoff(d) for d in self.list_snapshots()]},
            schema={SNAPSHOT_TS: pl.Datetime("us")},
        ).sort(SNAPSHOT_TS)
        obs = (
            observations.select(
                pl.col(ENTITY_KEY).cast(pl.String),
                pl.col("observation_ts").cast(pl.Datetime("us")),
            )
            .with_row_index("_row")
            .sort("observation_ts")
        )
        matched = obs.join_asof(
            snaps.with_columns(pl.col(SNAPSHOT_TS).alias("feature_snapshot_ts")),
            left_on="observation_ts",
            right_on=SNAPSHOT_TS,
            strategy="backward",
            tolerance=max_staleness,
        )
        used = [
            d
            for d in self.list_snapshots()
            if snapshot_cutoff(d) in set(matched["feature_snapshot_ts"].drop_nulls().to_list())
        ]
        for d in used:
            self._check_version(d)
        data = (
            self._read(used, [ENTITY_KEY, SNAPSHOT_TS, *feats])
            if used
            else pl.DataFrame(
                schema={
                    ENTITY_KEY: pl.String,
                    SNAPSHOT_TS: pl.Datetime("us"),
                    **{f: FEATURE_BY_NAME[f].dtype for f in feats},
                }
            )
        )
        joined = matched.join(
            data.rename({SNAPSHOT_TS: "feature_snapshot_ts"}),
            on=[ENTITY_KEY, "feature_snapshot_ts"],
            how="left",
        )
        has_snapshot = pl.col("feature_snapshot_ts").is_not_null()
        zero = [f for f in feats if FEATURE_BY_NAME[f].null_policy == "zero"]
        joined = joined.with_columns(
            [
                pl.when(has_snapshot)
                .then(pl.col(f).fill_null(0))
                .otherwise(pl.col(f))
                .cast(FEATURE_BY_NAME[f].dtype)
                .alias(f)
                for f in zero
            ]
        )
        return joined.sort("_row").select(
            [ENTITY_KEY, "observation_ts", "feature_snapshot_ts", *feats]
        )

    def get_feature_history(
        self,
        customer_id: str,
        features: list[str] | None = None,
        *,
        start: date | None = None,
        end: date | None = None,
    ) -> pl.DataFrame:
        feats = self._check_features(features)
        dates = [
            d
            for d in self.list_snapshots()
            if (start is None or d >= start) and (end is None or d <= end)
        ]
        df = self._read(dates, [ENTITY_KEY, SNAPSHOT_TS, *feats])
        if df.is_empty():
            return df
        return df.filter(pl.col(ENTITY_KEY) == customer_id).sort(SNAPSHOT_TS)

    # --------------------------------------------------------------------- query
    def query(self, sql: str) -> pl.DataFrame:
        """Run DuckDB SQL over all partitions via the ``customer_features`` view.

        ``snapshot_date`` is a hive partition column, so filters on it prune partitions.
        """
        glob = str(self.table_dir / "snapshot_date=*" / PARQUET).replace("'", "''")
        con = duckdb.connect()
        try:
            con.execute(
                f"CREATE VIEW {TABLE} AS SELECT * FROM "
                f"read_parquet('{glob}', hive_partitioning=true)"
            )
            result = pl.from_arrow(con.sql(sql).to_arrow_table())
            assert isinstance(result, pl.DataFrame)
            return result
        finally:
            con.close()
