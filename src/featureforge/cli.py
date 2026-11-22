"""Command line interface: ``featureforge <command>``."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import Any

from featureforge.config.settings import Settings, load_settings
from featureforge.utils.logging import configure_logging


def _d(value: str) -> date:
    return date.fromisoformat(value)


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="featureforge", description=__doc__)
    p.add_argument("--config", type=Path, default=None, help="YAML config file")
    sub = p.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="build + publish one snapshot")
    run.add_argument("--date", type=_d, default=None, help="snapshot date (default: today UTC)")
    run.add_argument("--force", action="store_true")
    run.add_argument("--no-prefect", action="store_true", help="run without Prefect")

    bf = sub.add_parser("backfill", help="historical backfill over a date range (inclusive)")
    bf.add_argument("--start", type=_d, required=True)
    bf.add_argument("--end", type=_d, required=True)
    bf.add_argument("--step-days", type=int, default=1)
    bf.add_argument("--force", action="store_true")
    bf.add_argument("--stale-only", action="store_true")
    bf.add_argument("--continue-on-error", action="store_true")
    bf.add_argument("--no-prefect", action="store_true")

    sub.add_parser("stale", help="list snapshots invalidated by source corrections")

    rs = sub.add_parser("restate", help="force re-ingest raw windows after a source correction")
    rs.add_argument("--start", type=_d, required=True)
    rs.add_argument("--end", type=_d, required=True)

    q = sub.add_parser("query", help="DuckDB SQL over all snapshots (view: customer_features)")
    q.add_argument("sql")

    ml = sub.add_parser("ml-demo", help="train the downstream baseline from the feature store")
    ml.add_argument("--horizon-days", type=int, default=30)

    sv = sub.add_parser("serve", help="local Prefect-owned schedule (dev only)")
    sv.add_argument("--cron", default="0 2 * * *")

    dl = sub.add_parser("download-retail", help="download the UCI Online Retail dataset")
    dl.add_argument("--target", type=Path, default=Path("data/external"))
    return p


def _run(settings: Settings, args: argparse.Namespace) -> int:
    from featureforge.storage.s3 import state_sync
    from featureforge.utils.timeutils import utcnow

    day = args.date or utcnow().date()
    with state_sync(settings):
        if args.no_prefect:
            from featureforge.orchestration.pipeline import run_snapshot

            out: dict[str, Any] = run_snapshot(settings, day, force=args.force).to_dict()
        else:
            from featureforge.orchestration.flows import featureforge_flow

            out = featureforge_flow(
                snapshot_date=day,
                force=args.force,
                config_file=str(args.config) if args.config else None,
            )
    print(
        json.dumps(
            {k: out[k] for k in ("run_id", "status", "batch_id", "rows", "validation")}, indent=2
        )
    )
    return 0


def _backfill(settings: Settings, args: argparse.Namespace) -> int:
    from featureforge.orchestration.flows import (
        BackfillError,
        backfill_flow,
        run_backfill,
    )
    from featureforge.orchestration.maintenance import find_stale_snapshots
    from featureforge.orchestration.pipeline import run_snapshot
    from featureforge.storage.s3 import state_sync

    with state_sync(settings):
        try:
            if args.no_prefect:
                only = None
                if args.stale_only:
                    only = [
                        s["snapshot_date"]
                        for s in find_stale_snapshots(settings)
                        if args.start <= s["snapshot_date"] <= args.end
                    ]
                summary = run_backfill(
                    args.start,
                    args.end,
                    lambda d: run_snapshot(settings, d, force=args.force).to_dict(),
                    step_days=args.step_days,
                    stop_on_failure=not args.continue_on_error,
                    only_dates=only,
                )
            else:
                summary = backfill_flow(
                    args.start,
                    args.end,
                    step_days=args.step_days,
                    force=args.force,
                    stop_on_failure=not args.continue_on_error,
                    only_stale=args.stale_only,
                    config_file=str(args.config) if args.config else None,
                )
        except BackfillError as exc:
            print(json.dumps(exc.summary, indent=2))
            return 1
    print(json.dumps(summary, indent=2))
    return 0


def _has_data(settings: Settings) -> bool:
    from featureforge.config.layout import Layout
    from featureforge.storage.feature_store import FeatureStore

    layout = Layout(settings.data_root)
    return (
        bool(FeatureStore(layout.feature_store).list_snapshots())
        and (layout.normalized / "transactions.parquet").exists()
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "download-retail":
        from featureforge.ingestion.download import download_online_retail

        print(download_online_retail(args.target))
        return 0
    settings = load_settings(args.config)
    configure_logging(settings.log_level)
    if args.command == "run":
        return _run(settings, args)
    if args.command == "backfill":
        return _backfill(settings, args)
    if args.command in {"stale", "query", "ml-demo"} and not _has_data(settings):
        print(
            "No published snapshots found under "
            f"{settings.data_root}. Run `featureforge run` or `backfill` first.",
            file=sys.stderr,
        )
        return 1
    if args.command == "stale":
        from featureforge.orchestration.maintenance import find_stale_snapshots

        stale = find_stale_snapshots(settings)
        print(
            json.dumps(
                [{**s, "snapshot_date": s["snapshot_date"].isoformat()} for s in stale], indent=2
            )
        )
        return 0
    if args.command == "restate":
        from featureforge.orchestration.maintenance import restate_raw
        from featureforge.storage.s3 import state_sync

        with state_sync(settings):
            dates = restate_raw(settings, args.start, args.end)
        print(json.dumps([d.isoformat() for d in dates]))
        return 0
    if args.command == "query":
        from featureforge.config.layout import Layout
        from featureforge.storage.feature_store import FeatureStore

        print(FeatureStore(Layout(settings.data_root).feature_store).query(args.sql))
        return 0
    if args.command == "ml-demo":
        from featureforge.ml.demo import InsufficientDataError, run_ml_demo

        try:
            print(json.dumps(run_ml_demo(settings, horizon_days=args.horizon_days), indent=2))
        except InsufficientDataError as exc:
            print(f"ML demo needs more history: {exc}", file=sys.stderr)
            return 1
        return 0
    if args.command == "serve":
        from featureforge.orchestration.flows import serve_scheduled

        serve_scheduled(args.cron, str(args.config) if args.config else None)
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
