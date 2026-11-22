"""Schema / null-policy / value consistency checks between feature frames."""

from __future__ import annotations

import polars as pl

from featureforge.features.definitions import (
    ENTITY_KEY,
    FEATURE_BY_NAME,
    FEATURES,
    feature_schema,
)


def schema_problems(df: pl.DataFrame, features: list[str] | None = None) -> list[str]:
    """Return human-readable problems: missing columns, wrong dtypes, nulls in zero-policy."""
    expected = feature_schema()
    cols = [ENTITY_KEY, "snapshot_ts", *(features or [f.name for f in FEATURES])]
    problems: list[str] = []
    for c in cols:
        if c not in df.columns:
            problems.append(f"missing column: {c}")
        elif df.schema[c] != expected[c]:
            problems.append(f"dtype of {c}: expected {expected[c]}, got {df.schema[c]}")
    if not features:
        extra = set(df.columns) - set(expected)
        problems.extend(f"unexpected column: {c}" for c in sorted(extra))
    for c in cols:
        d = FEATURE_BY_NAME.get(c)
        if d and d.null_policy == "zero" and c in df.columns and df[c].null_count() > 0:
            problems.append(f"{c}: {df[c].null_count()} NULLs violate null policy 'zero'")
    return problems


def apply_null_policy(df: pl.DataFrame, features: list[str]) -> pl.DataFrame:
    """Fill 'zero'-policy features. Used for entities with no eligible activity."""
    exprs = [
        pl.col(f).fill_null(0).cast(FEATURE_BY_NAME[f].dtype)
        for f in features
        if FEATURE_BY_NAME[f].null_policy == "zero"
    ]
    return df.with_columns(exprs) if exprs else df


def value_mismatches(
    a: pl.DataFrame, b: pl.DataFrame, features: list[str], rtol: float = 1e-9, atol: float = 1e-9
) -> list[str]:
    """Compare two feature frames *by value* per entity (not just by column names)."""
    out: list[str] = []
    ja = a.select([ENTITY_KEY, *features]).sort(ENTITY_KEY)
    jb = b.select([ENTITY_KEY, *features]).sort(ENTITY_KEY)
    if ja[ENTITY_KEY].to_list() != jb[ENTITY_KEY].to_list():
        return ["entity sets differ"]
    for f in features:
        sa, sb = ja[f], jb[f]
        if sa.null_count() != sb.null_count() or (sa.is_null() != sb.is_null()).any():
            out.append(f"{f}: null pattern differs")
            continue
        x, y = sa.drop_nulls(), sb.drop_nulls()
        if x.dtype.is_float():
            bad = ((x - y).abs() > (atol + rtol * y.abs())).sum()
        else:
            bad = (x != y).sum()
        if bad:
            out.append(f"{f}: {bad} differing values")
    return out
