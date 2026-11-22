"""Advisory per-snapshot lock: prevents two concurrent runs of the same logical batch.

This is the second line of defence against duplicate scheduling (the first is having
exactly one authoritative scheduler, see docs/cloud_deployment.md).
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from pathlib import Path

from featureforge.config.layout import Layout


class RunInProgressError(RuntimeError):
    pass


@contextmanager
def run_lock(layout: Layout, snapshot_date: date, ttl_seconds: float = 3600.0) -> Iterator[Path]:
    path: Path = layout.locks / f"snapshot_date={snapshot_date:%Y-%m-%d}.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        age = time.time() - path.stat().st_mtime
        if age < ttl_seconds:
            raise RunInProgressError(
                f"snapshot {snapshot_date} is already being processed (lock age {age:.0f}s)"
            ) from None
        path.unlink(missing_ok=True)  # stale lock from a crashed run
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    with os.fdopen(fd, "w") as fh:
        json.dump({"pid": os.getpid(), "created": time.time()}, fh)
    try:
        yield path
    finally:
        path.unlink(missing_ok=True)
