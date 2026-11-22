"""Filesystem helpers for safe, staged publication.

POSIX ``rename`` on the same filesystem is atomic. Directory replacement is done as
two renames (old -> trash, staging -> target), so a reader can briefly see *no* partition,
but never a half-written one. ``recover_dir`` repairs the state after a crash between
the two renames.
"""

from __future__ import annotations

import json
import os
import shutil
import uuid
from pathlib import Path
from typing import Any

import polars as pl


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def atomic_write_json(path: Path, payload: Any) -> None:
    atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True, default=str))


def write_parquet_atomic(df: pl.DataFrame, path: Path, compression: str = "zstd") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    df.write_parquet(tmp, compression=compression)  # type: ignore[arg-type]
    os.replace(tmp, path)


def new_staging_dir(target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = target.parent / f".staging-{target.name}-{uuid.uuid4().hex[:8]}"
    staging.mkdir(parents=True, exist_ok=False)
    return staging


def atomic_replace_dir(staging: Path, target: Path) -> None:
    """Publish ``staging`` as ``target``, replacing any previous version."""
    target.parent.mkdir(parents=True, exist_ok=True)
    trash: Path | None = None
    if target.exists():
        trash = target.parent / f".old-{target.name}-{uuid.uuid4().hex[:8]}"
        os.rename(target, trash)
    os.rename(staging, target)
    if trash is not None:
        shutil.rmtree(trash, ignore_errors=True)


def recover_dir(target: Path) -> None:
    """Repair a crash between the two renames and drop abandoned staging dirs."""
    parent = target.parent
    if not parent.exists():
        return
    olds = sorted(parent.glob(f".old-{target.name}-*"))
    if not target.exists() and olds:
        os.rename(olds[-1], target)
        olds = olds[:-1]
    for leftover in [*olds, *parent.glob(f".staging-{target.name}-*")]:
        shutil.rmtree(leftover, ignore_errors=True)
