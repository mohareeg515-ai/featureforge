"""S3 state synchronisation for stateless containers (ECS Fargate).

Each Fargate task starts with an empty disk, but the pipeline needs earlier raw
partitions (incremental watermark) and snapshot metadata (idempotency). We therefore
``pull`` state at start and ``push`` results at the end.

Publication ordering: Parquet data objects are uploaded before ``_metadata.json`` /
``_meta.json`` so a reader that sees metadata also sees the data. S3 PUTs are atomic per
object and strongly consistent, but a *multi-object* partition is not transactional;
the partition has a single Parquet object name (``part-0.parquet``) which is overwritten
atomically, so Athena sees either the old or the new file, never a partial one.
Tested only against a local S3 emulator (moto); see docs/cloud_deployment.md.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from featureforge.config.settings import Settings
from featureforge.utils.logging import get_logger, log_event

_log = get_logger("s3")
PULL_DIRS = ("raw", "feature_store", "metadata")
PUSH_DIRS = ("raw", "feature_store", "metadata", "reports", "rejected")
_LATE = ("_metadata.json", "_meta.json")
_SKIP_PARTS = ("locks",)


def make_s3_client() -> Any:
    import boto3

    return boto3.client("s3")  # credentials/region from the IAM role or environment


class S3StateSync:
    def __init__(self, client: Any, bucket: str, prefix: str) -> None:
        self._c = client
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    def _key(self, rel: str) -> str:
        return f"{self._prefix}/{rel}" if self._prefix else rel

    def _remote(self, rel_dir: str) -> dict[str, int]:
        out: dict[str, int] = {}
        pager = self._c.get_paginator("list_objects_v2")
        for page in pager.paginate(Bucket=self._bucket, Prefix=self._key(rel_dir) + "/"):
            for obj in page.get("Contents", []):
                out[obj["Key"]] = int(obj["Size"])
        return out

    @staticmethod
    def _local_files(root: Path, rel_dir: str) -> Iterator[Path]:
        base = root / rel_dir
        if not base.exists():
            return
        for p in sorted(base.rglob("*")):
            rel_parts = p.relative_to(root).parts
            if not p.is_file() or any(part.startswith(".") for part in rel_parts):
                continue
            if any(part in _SKIP_PARTS for part in rel_parts):
                continue
            yield p

    def push(self, root: Path, dirs: tuple[str, ...] = PUSH_DIRS) -> int:
        uploaded = 0
        for d in dirs:
            remote = self._remote(d)
            files = sorted(self._local_files(root, d), key=lambda p: p.name in _LATE)
            for p in files:
                key = self._key(p.relative_to(root).as_posix())
                if remote.get(key) != p.stat().st_size or p.name in _LATE or d == "metadata":
                    self._c.upload_file(str(p), self._bucket, key)
                    uploaded += 1
        log_event(_log, "s3 push complete", uploaded=uploaded)
        return uploaded

    def pull(self, root: Path, dirs: tuple[str, ...] = PULL_DIRS) -> int:
        downloaded = 0
        for d in dirs:
            for key, size in self._remote(d).items():
                rel = key[len(self._prefix) + 1 :] if self._prefix else key
                dest = root / rel
                if any(part in _SKIP_PARTS for part in dest.relative_to(root).parts):
                    continue
                if dest.exists() and dest.stat().st_size == size:
                    continue
                dest.parent.mkdir(parents=True, exist_ok=True)
                self._c.download_file(self._bucket, key, str(dest))
                downloaded += 1
        log_event(_log, "s3 pull complete", downloaded=downloaded)
        return downloaded


@contextmanager
def state_sync(settings: Settings, client: Any | None = None) -> Iterator[None]:
    """Pull state before and push results after (also after failures, to keep diagnostics)."""
    if settings.storage_backend != "s3":
        yield
        return
    if not settings.s3_bucket:
        raise ValueError("FF_S3_BUCKET must be set when FF_STORAGE_BACKEND=s3")
    sync = S3StateSync(client or make_s3_client(), settings.s3_bucket, settings.s3_prefix)
    settings.data_root.mkdir(parents=True, exist_ok=True)
    sync.pull(settings.data_root)
    try:
        yield
    finally:
        sync.push(settings.data_root)
