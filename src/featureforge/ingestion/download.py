"""Reproducible download of the real dataset with retries and a recorded checksum."""

from __future__ import annotations

import json
import shutil
import urllib.request
import zipfile
from pathlib import Path

from featureforge.utils.fs import atomic_write_json
from featureforge.utils.hashing import sha256_file
from featureforge.utils.logging import get_logger
from featureforge.utils.retry import TransientError, retry_call

ONLINE_RETAIL_ZIP = "https://archive.ics.uci.edu/static/public/352/online+retail.zip"
_log = get_logger("download")


def download_file(url: str, dest: Path, *, attempts: int = 3, timeout: float = 60.0) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)

    def _once() -> None:
        tmp = dest.with_suffix(dest.suffix + ".part")
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp, tmp.open("wb") as out:
                shutil.copyfileobj(resp, out)
        except OSError as exc:
            raise TransientError(str(exc)) from exc
        tmp.replace(dest)

    retry_call(_once, attempts=attempts)
    return dest


def download_online_retail(target_dir: Path, url: str = ONLINE_RETAIL_ZIP) -> Path:
    """Download + extract the dataset and write ``provenance.json`` next to it."""
    zip_path = download_file(url, target_dir / "online_retail.zip")
    with zipfile.ZipFile(zip_path) as zf:
        members = [n for n in zf.namelist() if n.lower().endswith((".xlsx", ".csv"))]
        if not members:
            raise ValueError("archive contains no .xlsx/.csv file")
        zf.extract(members[0], target_dir)
        data_file = target_dir / members[0]
    atomic_write_json(
        target_dir / "provenance.json",
        {
            "source_url": url,
            "dataset_page": "https://archive.ics.uci.edu/dataset/352/online+retail",
            "file": data_file.name,
            "sha256": sha256_file(data_file),
        },
    )
    _log.info("downloaded %s", json.dumps({"file": data_file.name}))
    return data_file
