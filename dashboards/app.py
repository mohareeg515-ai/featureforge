"""Operational dashboard (Streamlit). Reads ONLY real run artifacts from FF_DATA_ROOT.

Run:  FF_DATA_ROOT=./data streamlit run dashboards/app.py
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from featureforge.config.layout import Layout
from featureforge.monitoring.runs import RunRegistry
from featureforge.storage.feature_store import FeatureStore
from featureforge.utils.timeutils import utcnow

st.set_page_config(page_title="FeatureForge operations", layout="wide")
st.title("FeatureForge: pipeline operations")

root = Path(os.environ.get("FF_DATA_ROOT", "data"))
layout = Layout(root)
st.caption(f"Data root: `{root}`. Everything shown is read from run artifacts on disk.")

runs = RunRegistry(layout.runs_file).read_all()
if not runs:
    st.info("No pipeline runs recorded yet. Run `featureforge run --date YYYY-MM-DD` first.")
    st.stop()

df = pd.DataFrame(runs)
df["errors"] = df["error"].apply(lambda e: e["category"] if isinstance(e, dict) else "")
df["warnings"] = df["validation"].apply(lambda v: v["warnings"])
df["critical"] = df["validation"].apply(lambda v: v["critical"])
df["feature_rows"] = df["rows"].apply(lambda r: r.get("feature_rows"))
df["raw_tx_delta"] = df["rows"].apply(lambda r: r.get("raw_transactions_delta"))

store = FeatureStore(layout.feature_store)
snaps = store.list_snapshots()
latest_file = layout.metadata / "latest.json"
latest = json.loads(latest_file.read_text()) if latest_file.exists() else {}

c1, c2, c3, c4 = st.columns(4)
c1.metric("Runs recorded", len(df))
c2.metric("Failed runs", int((df["status"] == "failed").sum()))
c3.metric("Latest feature snapshot", snaps[-1].isoformat() if snaps else "none")
fresh = latest.get("freshness_hours")
c4.metric("Data freshness at last run (h)", "n/a" if fresh is None else f"{fresh:.1f}")
if snaps:
    age_days = (utcnow() - datetime.combine(snaps[-1], datetime.min.time())).days
    st.caption(f"Latest snapshot is {age_days} day(s) old relative to now (UTC).")

st.subheader("Recent runs")
cols = [
    "run_id",
    "snapshot_date",
    "status",
    "orchestrator",
    "duration_s",
    "feature_rows",
    "raw_tx_delta",
    "warnings",
    "critical",
    "errors",
    "started_at",
]
st.dataframe(df[cols].tail(50).iloc[::-1], use_container_width=True, hide_index=True)

left, right = st.columns(2)
with left:
    st.subheader("Processing duration (s)")
    st.bar_chart(df.set_index("run_id")["duration_s"].tail(30))
with right:
    st.subheader("Status counts")
    st.bar_chart(df["status"].value_counts())

st.subheader("Quality checks of the latest report")
report_dirs = sorted((layout.reports / "quality").glob("snapshot_date=*/latest.json"))
if report_dirs:
    report = json.loads(report_dirs[-1].read_text())
    st.caption(f"{report_dirs[-1].parent.name}, run {report.get('run_id')}: {report['summary']}")
    checks = pd.DataFrame(report["results"])[
        ["dataset", "check", "passed", "severity", "affected_records", "total_records"]
    ]
    st.dataframe(checks, use_container_width=True, hide_index=True)
else:
    st.info("No quality report found.")
