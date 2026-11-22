from __future__ import annotations

from datetime import date, datetime

import polars as pl
import pytest

from featureforge.ingestion.schemas import SOURCE_TX_COLUMNS
from featureforge.processing.cleaning import clean_customer_id
from featureforge.processing.normalize import normalize_events, normalize_transactions

REGEX = r"^[0-9]{5}[A-Z]{0,2}$"
TS = datetime(2025, 1, 1, 10)


def raw(rows):
    base = dict(
        InvoiceNo="1",
        StockCode="85123A",
        Description=" d ",
        Quantity=1,
        InvoiceDate=TS,
        UnitPrice=2.0,
        CustomerID="100",
        Country="UK",
        record_id="r",
        line_seq=0,
        available_at=TS,
        is_synthetic=True,
        source_name="t",
        ingestion_date=date(2025, 1, 2),
    )
    data = [{**base, **r, "record_id": r.get("record_id", f"r{i}")} for i, r in enumerate(rows)]
    return pl.DataFrame(data, schema={**SOURCE_TX_COLUMNS, "ingestion_date": pl.Date}).lazy()


def reasons(lf):
    clean, rej = normalize_transactions(lf, REGEX)
    return clean.collect(), rej.collect()


def test_customer_id_standardization():
    s = pl.Series(["17850.0", " 123 ", "", "nan", None, "99.50"])
    out = pl.DataFrame({"c": s}).select(clean_customer_id(pl.col("c")).alias("c"))["c"].to_list()
    assert out == ["17850", "123", None, None, None, "99.50"]


@pytest.mark.parametrize(
    ("override", "reason"),
    [
        ({"CustomerID": None}, "missing_customer_id"),
        ({"InvoiceNo": "C123", "Quantity": -1}, "cancellation"),
        ({"Quantity": 0}, "non_positive_quantity"),
        ({"UnitPrice": 0.0}, "non_positive_price"),
        ({"StockCode": "POST"}, "non_product_stock_code"),
        ({"InvoiceDate": None}, "missing_timestamp"),
        ({"Quantity": None}, "missing_quantity_or_price"),
    ],
)
def test_each_rule_quarantines_with_reason(override, reason):
    clean, rej = reasons(raw([{}, override]))
    assert clean.height == 1 and rej["reject_reason"].to_list() == [reason]


def test_standardization_and_exact_duplicates():
    rows = [
        {"StockCode": " 85123a ", "CustomerID": "7.0", "record_id": "a"},
        {"StockCode": "85123A", "CustomerID": "7", "record_id": "b"},
    ]  # identical after clean
    clean, rej = reasons(raw(rows))
    assert clean["stock_code"].to_list() == ["85123A"] and clean["customer_id"][0] == "7"
    assert clean["description"][0] == "d"
    assert rej["reject_reason"].to_list() == ["exact_duplicate"]


def test_record_versions_with_different_availability_are_kept():
    later = datetime(2025, 1, 5)
    clean, rej = reasons(
        raw(
            [
                {"record_id": "R", "Quantity": 1},
                {"record_id": "R", "Quantity": 5, "available_at": later},
            ]
        )
    )
    assert clean.height == 2 and rej.height == 0  # both versions retained for as-of reads


def test_events_normalization():
    ev = pl.DataFrame(
        {
            "event_id": ["1", "2", "3", "4", "4"],
            "customer_id": ["a", None, "c", "d", "d"],
            "event_type": [
                "session_start",
                "session_start",
                "weird",
                " ADD_TO_CART ",
                "add_to_cart",
            ],
            "event_ts": [TS] * 5,
            "available_at": [TS] * 5,
            "is_synthetic": [True] * 5,
            "source_name": ["s"] * 5,
            "ingestion_date": [date(2025, 1, 2)] * 5,
        }
    ).lazy()
    clean, rej = normalize_events(ev)
    assert sorted(clean.collect()["event_id"].to_list()) == ["1", "4"]
    assert sorted(rej.collect()["reject_reason"].to_list()) == [
        "exact_duplicate",
        "missing_customer_id",
        "unknown_event_type",
    ]
