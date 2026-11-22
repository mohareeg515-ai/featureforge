from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from featureforge.config.settings import Settings, load_settings


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for k in list(__import__("os").environ):
        if k.startswith("FF_"):
            monkeypatch.delenv(k, raising=False)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """Small, fast, deterministic synthetic configuration."""
    return load_settings(
        data_root=tmp_path / "data",
        synthetic_customers=60,
        synthetic_start=date(2025, 1, 1),
        synthetic_end=date(2025, 5, 1),
        seed=7,
    )
