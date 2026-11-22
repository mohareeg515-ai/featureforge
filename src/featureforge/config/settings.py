"""Typed, environment-driven configuration.

Precedence (highest first): explicit overrides > ``FF_*`` environment variables >
YAML file > defaults. Secrets are never stored here; AWS credentials come from the
IAM role / profile resolved by boto3.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class QualityThresholds(BaseModel):
    """Configurable validation thresholds (warning vs critical)."""

    model_config = ConfigDict(extra="forbid")

    missing_customer_warn: float = 0.35
    missing_customer_critical: float = 0.90
    duplicate_warn: float = 0.02
    duplicate_critical: float = 0.25
    rejected_warn: float = 0.40
    rejected_critical: float = 0.90
    orphan_event_warn: float = 0.05
    orphan_event_critical: float = 0.50
    freshness_warn_days: float = 3.0
    freshness_critical_days: float = 14.0
    volume_ratio_low: float = 0.2
    volume_ratio_high: float = 5.0
    min_history_for_volume: int = 3


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FF_", env_nested_delimiter="__", extra="forbid")

    env: Literal["dev", "prod", "test"] = "dev"
    data_root: Path = Path("data")
    source_kind: Literal["synthetic", "retail_file"] = "synthetic"
    retail_file: Path | None = None
    seed: int = 42
    synthetic_customers: int = Field(default=300, ge=1)
    synthetic_start: date = date(2025, 1, 1)
    synthetic_end: date = date(2025, 9, 1)  # exclusive
    inject_defects: bool = True
    transaction_lag_hours: int = Field(default=2, ge=0)
    events_lag_minutes: int = Field(default=5, ge=0)
    product_code_regex: str = r"^[0-9]{5}[A-Z]{0,2}$"
    storage_backend: Literal["local", "s3"] = "local"
    s3_bucket: str | None = None
    s3_prefix: str = "featureforge"
    log_level: str = "INFO"
    duckdb_memory_limit: str = "1GB"
    duckdb_threads: int = Field(default=1, ge=1)
    max_snapshot_staleness_days: int = Field(default=1, ge=0)
    thresholds: QualityThresholds = Field(default_factory=QualityThresholds)


def load_settings(config_file: Path | None = None, **overrides: Any) -> Settings:
    """Load settings with precedence overrides > env > YAML > defaults."""
    file_values: dict[str, Any] = {}
    if config_file is not None:
        loaded = yaml.safe_load(Path(config_file).read_text(encoding="utf-8")) or {}
        if not isinstance(loaded, dict):
            raise ValueError(f"{config_file} must contain a YAML mapping")
        file_values = loaded
    env_values = Settings().model_dump(exclude_unset=True)
    merged = {**file_values, **env_values, **overrides}
    return Settings(**merged)
