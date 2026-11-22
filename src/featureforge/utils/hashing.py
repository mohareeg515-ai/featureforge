"""Deterministic hashing helpers."""

from __future__ import annotations

import hashlib
from pathlib import Path


def stable_hash(*parts: object, length: int = 16) -> str:
    """SHA-256 over the string forms of ``parts``; stable across processes."""
    digest = hashlib.sha256("\x1f".join(str(p) for p in parts).encode()).hexdigest()
    return digest[:length]


def sha256_file(path: Path, chunk_size: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()
