from __future__ import annotations

import json
import logging
import os
import time
from datetime import date

import pytest
from pydantic import ValidationError

from featureforge.config.layout import Layout
from featureforge.config.settings import load_settings
from featureforge.orchestration.locks import RunInProgressError, run_lock
from featureforge.utils.fs import atomic_replace_dir, new_staging_dir, recover_dir
from featureforge.utils.hashing import stable_hash
from featureforge.utils.logging import JsonFormatter, bind_run_id
from featureforge.utils.retry import TransientError, retry_call
from featureforge.utils.timeutils import date_range, snapshot_cutoff


def test_defaults_and_validation(tmp_path):
    s = load_settings()
    assert s.source_kind == "synthetic" and s.storage_backend == "local" and s.seed == 42
    with pytest.raises(ValidationError):
        load_settings(storage_backend="gcs")
    with pytest.raises(ValidationError):
        load_settings(unknown_field=1)


def test_precedence_overrides_env_yaml_defaults(tmp_path, monkeypatch):
    cfg = tmp_path / "c.yaml"
    cfg.write_text("seed: 1\nsynthetic_customers: 11\nthresholds:\n  duplicate_warn: 0.5\n")
    assert load_settings(cfg).seed == 1
    assert load_settings(cfg).thresholds.duplicate_warn == 0.5
    monkeypatch.setenv("FF_SEED", "2")
    assert load_settings(cfg).seed == 2 and load_settings(cfg).synthetic_customers == 11
    assert load_settings(cfg, seed=3).seed == 3


def test_yaml_must_be_mapping(tmp_path):
    cfg = tmp_path / "c.yaml"
    cfg.write_text("- a\n- b\n")
    with pytest.raises(ValueError):
        load_settings(cfg)


def test_repo_configs_load():
    from pathlib import Path

    for f in Path("configs").glob("*.yaml"):
        assert load_settings(f).env in {"dev", "prod", "test"}


def test_stable_hash_is_deterministic_and_sensitive():
    assert stable_hash("a", 1) == stable_hash("a", 1) != stable_hash("a", 2)


def test_atomic_replace_and_recover(tmp_path):
    target = tmp_path / "p=1"
    for payload in ("old", "new"):
        st = new_staging_dir(target)
        (st / "f.txt").write_text(payload)
        atomic_replace_dir(st, target)
    assert (target / "f.txt").read_text() == "new"
    assert [p.name for p in tmp_path.iterdir()] == ["p=1"]  # no leftovers
    # crash between the two renames: target gone, old dir present
    os.rename(target, tmp_path / ".old-p=1-deadbeef")
    (tmp_path / ".staging-p=1-abc").mkdir()
    recover_dir(target)
    assert (target / "f.txt").read_text() == "new"
    assert [p.name for p in tmp_path.iterdir()] == ["p=1"]


def test_retry_succeeds_after_transient_and_gives_up():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise TransientError("blip")
        return "ok"

    sleeps: list[float] = []
    assert retry_call(flaky, attempts=3, base_delay=1, sleep=sleeps.append) == "ok"
    assert sleeps == [1, 2]

    def always():
        raise TransientError("down")

    with pytest.raises(TransientError):
        retry_call(always, attempts=2, sleep=lambda _: None)
    with pytest.raises(ValueError):  # non-transient errors are not retried
        retry_call(lambda: (_ for _ in ()).throw(ValueError("bad")), sleep=lambda _: None)


def test_run_lock_blocks_concurrent_runs_and_recovers_stale(tmp_path):
    layout = Layout(tmp_path)
    d = date(2025, 3, 1)
    with run_lock(layout, d):
        with pytest.raises(RunInProgressError), run_lock(layout, d):
            pass
        with run_lock(layout, date(2025, 3, 2)):  # other dates are independent
            pass
    with run_lock(layout, d):  # released afterwards
        pass
    lock = layout.locks / "snapshot_date=2025-03-01.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.write_text("{}")
    old = time.time() - 7200
    os.utime(lock, (old, old))
    with run_lock(layout, d, ttl_seconds=3600):  # stale lock is replaced
        pass


def test_json_log_format_and_run_id():
    rec = logging.LogRecord("featureforge.x", logging.INFO, "f", 1, "hello %s", ("w",), None)
    rec.ctx = {"rows": 5}
    with bind_run_id("run-1"):
        payload = json.loads(JsonFormatter().format(rec))
    assert payload["message"] == "hello w" and payload["run_id"] == "run-1"
    assert payload["rows"] == 5 and payload["level"] == "INFO"


def test_time_helpers():
    assert snapshot_cutoff(date(2025, 1, 2)).isoformat() == "2025-01-02T00:00:00"
    assert date_range(date(2025, 1, 1), date(2025, 1, 10), 3) == [
        date(2025, 1, 1),
        date(2025, 1, 4),
        date(2025, 1, 7),
        date(2025, 1, 10),
    ]
    with pytest.raises(ValueError):
        date_range(date(2025, 1, 2), date(2025, 1, 1))
