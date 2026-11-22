from __future__ import annotations

import zipfile
from datetime import date, datetime

import polars as pl
import pytest

from featureforge.config.layout import Layout
from featureforge.ingestion.download import download_online_retail
from featureforge.ingestion.ingest import (
    ImmutableRawPartitionError,
    ingest_source,
    partition_dates,
    previous_watermark,
    read_meta,
    scan_raw,
)
from featureforge.ingestion.schemas import SOURCE_EVENT_COLUMNS, SOURCE_TX_COLUMNS
from featureforge.ingestion.sources import (
    RetailFileTransactionSource,
    SourceSchemaError,
    build_sources,
)
from featureforge.ingestion.synthetic import generate_transactions
from featureforge.utils.hashing import sha256_file

RETAIL_CSV = """InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
536365,85123A,WHITE HANGING HEART,6,12/1/2010 8:26,2.55,17850.0,United Kingdom
536365,71053,WHITE METAL LANTERN,6,12/1/2010 8:26,3.39,17850.0,United Kingdom
536366,22633,HAND WARMER,6,12/1/2010 8:28,1.85,,United Kingdom
C536379,D,Discount,-1,12/1/2010 9:41,27.50,14527,United Kingdom
"""


def test_synthetic_is_deterministic_and_flagged(settings):
    kw = dict(seed=3, n_customers=30, start=date(2025, 1, 1), end=date(2025, 3, 1))
    a, b = generate_transactions(**kw), generate_transactions(**kw)
    assert a.equals(b)
    assert not a.equals(generate_transactions(**{**kw, "seed": 4}))
    assert a["is_synthetic"].all() and a["source_name"].str.starts_with("synthetic").all()
    assert dict(a.schema) == SOURCE_TX_COLUMNS


def test_synthetic_defects_exist_and_can_be_disabled():
    kw = dict(seed=5, n_customers=80, start=date(2025, 1, 1), end=date(2025, 4, 1))
    dirty, clean = generate_transactions(**kw), generate_transactions(**kw, inject_defects=False)
    assert dirty["CustomerID"].null_count() > 0
    assert dirty["InvoiceNo"].str.starts_with("C").any()
    assert (dirty["UnitPrice"] == 0).any() or (dirty["StockCode"] == "POST").any()
    assert (
        clean["CustomerID"].null_count() == 0 and not clean["InvoiceNo"].str.starts_with("C").any()
    )


def test_events_are_synthetic_and_schema_conformant(settings):
    _tx, ev = build_sources(settings)
    df = ev.read()
    assert dict(df.schema) == SOURCE_EVENT_COLUMNS and df["is_synthetic"].all()
    assert set(df["event_type"].unique()) == {"session_start", "add_to_cart"}
    assert (df["available_at"] >= df["event_ts"]).all()
    assert df["event_id"].n_unique() == df.height
    assert ev.info()["note"].startswith("SYNTHETIC")


def test_retail_reader_standardizes_real_layout(tmp_path):
    f = tmp_path / "online_retail.csv"
    f.write_text(RETAIL_CSV)
    df = RetailFileTransactionSource(f, lag_hours=2).read()
    assert df.height == 4 and not df["is_synthetic"].any()
    assert df["source_name"].unique().to_list() == ["uci_online_retail"]
    assert df["InvoiceDate"][0] == datetime(2010, 12, 1, 8, 26)
    assert df["available_at"][0] == datetime(2010, 12, 1, 10, 26)
    assert df["CustomerID"][0] == "17850.0"  # raw layer preserves the original value
    assert df["CustomerID"].null_count() == 1
    assert df["record_id"].n_unique() == 4


def test_retail_reader_rejects_bad_schema(tmp_path):
    f = tmp_path / "bad.csv"
    f.write_text("InvoiceNo,StockCode\n1,2\n")
    with pytest.raises(SourceSchemaError):
        RetailFileTransactionSource(f).read()
    with pytest.raises(FileNotFoundError):
        RetailFileTransactionSource(tmp_path / "nope.csv").read()


def test_download_extracts_and_records_provenance(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    csv = src / "Online Retail.csv"
    csv.write_text(RETAIL_CSV)
    archive = src / "x.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.write(csv, "Online Retail.csv")
    out = download_online_retail(tmp_path / "dl", url=archive.as_uri())
    prov = __import__("json").loads((tmp_path / "dl" / "provenance.json").read_text())
    assert out.exists() and prov["sha256"] == sha256_file(out)
    assert prov["dataset_page"].startswith("https://archive.ics.uci.edu/")


def test_incremental_ingestion_slices_by_availability(settings):
    layout = Layout(settings.data_root)
    tx, _ = build_sources(settings)
    d1, d2 = date(2025, 3, 1), date(2025, 3, 8)
    r1, r2 = ingest_source(layout, tx, d1), ingest_source(layout, tx, d2)
    assert r1.available_after is None and r2.available_after == datetime(2025, 3, 1)
    full = tx.read()
    assert r1.rows + r2.rows == full.filter(pl.col("available_at") <= datetime(2025, 3, 8)).height
    assert previous_watermark(layout, "transactions", d2) == datetime(2025, 3, 1)
    assert partition_dates(layout, "transactions") == [d1, d2]
    meta = read_meta(layout, "transactions", d2)
    assert meta["is_synthetic"] is True and meta["source_info"]["note"].startswith("SYNTHETIC")
    assert len(meta["parquet_sha256"]) == 64
    union = scan_raw(layout, "transactions").collect()
    assert union.height == r1.rows + r2.rows  # partitions are disjoint deltas
    assert set(union["ingestion_date"].unique().to_list()) == {d1, d2}


def test_reingest_same_date_is_noop_and_changed_history_is_rejected(settings, monkeypatch):
    layout = Layout(settings.data_root)
    tx, _ = build_sources(settings)
    d = date(2025, 3, 1)
    first = ingest_source(layout, tx, d)
    assert not first.skipped_existing
    again = ingest_source(layout, tx, d)
    assert again.skipped_existing and again.rows == first.rows

    original = tx.read()
    tampered = original.with_columns(
        pl.when(pl.int_range(pl.len()) == 0)
        .then(99)
        .otherwise(pl.col("Quantity"))
        .alias("Quantity")
    )
    monkeypatch.setattr(tx, "read", lambda: tampered)
    with pytest.raises(ImmutableRawPartitionError):
        ingest_source(layout, tx, d)
    assert ingest_source(layout, tx, d, force=True).skipped_existing is False


def test_empty_delta_still_records_coverage(settings):
    layout = Layout(settings.data_root)
    tx, _ = build_sources(settings)
    ingest_source(layout, tx, date(2025, 8, 1))  # source ends 2025-05-01 -> all data loaded
    r = ingest_source(layout, tx, date(2025, 8, 2))
    assert r.rows == 0
    assert read_meta(layout, "transactions", date(2025, 8, 2))["available_after"] is not None
