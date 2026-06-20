"""Tests for backend/api/refunds.py — POST /record, GET /list, and deeper /summary branches.

The /summary smoke test (counting orders) lives in test_analytics_endpoints.py and is
not duplicated here; these tests drive the uncovered paths: recording refunds/chargebacks,
listing them, and the by-source attribution + rate math in the summary.
"""

from __future__ import annotations

import sqlite3

import pytest

from tests.helpers import insert_rows, order, touchpoint


def _refund_rows(db_path: str, order_id: str | None = None) -> list[dict]:
    """Read refund_log directly so we can assert DB side-effects."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        if order_id is None:
            cur = conn.execute("SELECT * FROM refund_log ORDER BY ts")
        else:
            cur = conn.execute("SELECT * FROM refund_log WHERE order_id = ?", [order_id])
        return [dict(r) for r in cur.fetchall()]
    except sqlite3.OperationalError:
        # Table is only created on the first endpoint call that reaches
        # _ensure_refunds_table; absent table == no rows written.
        return []
    finally:
        conn.close()


def _order_row(db_path: str, order_id: str) -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute("SELECT * FROM orders WHERE order_id = ?", [order_id])
        row = cur.fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# POST /api/refunds/record
# --------------------------------------------------------------------------- #


def test_record_refund_happy_path_writes_log_and_updates_order(client, api_db):
    insert_rows(
        api_db,
        "orders",
        [order("o1", "2026-01-10T12:00:00Z", "cust1", gross=100, net=100, refunds=0)],
    )

    r = client.post(
        "/api/refunds/record",
        json={"order_id": "o1", "amount": 25.5, "type": "refund", "reason": "changed mind", "source": "shopify"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["order_id"] == "o1"
    assert body["amount"] == 25.5
    assert isinstance(body["refund_id"], str) and len(body["refund_id"]) == 32

    # refund_log side-effect
    rows = _refund_rows(api_db, "o1")
    assert len(rows) == 1
    logged = rows[0]
    assert logged["id"] == body["refund_id"]
    assert logged["order_id"] == "o1"
    assert logged["type"] == "refund"
    assert logged["reason"] == "changed mind"
    assert logged["source"] == "shopify"
    assert float(logged["amount"]) == 25.5
    # customer_key copied from the original order
    assert logged["customer_key"] == "cust1"

    # orders table updated: refunds +25.5, net -25.5
    o = _order_row(api_db, "o1")
    assert float(o["refunds"]) == 25.5
    assert float(o["net"]) == 100 - 25.5


def test_record_chargeback_updates_chargebacks_column(client, api_db):
    insert_rows(
        api_db,
        "orders",
        [order("o2", "2026-01-11T12:00:00Z", "cust2", gross=200, net=200, chargebacks=0)],
    )

    r = client.post(
        "/api/refunds/record",
        json={"order_id": "o2", "amount": 40, "type": "CHARGEBACK"},  # type normalized to lowercase
    )
    assert r.status_code == 200
    assert r.json()["ok"] is True

    rows = _refund_rows(api_db, "o2")
    assert len(rows) == 1
    assert rows[0]["type"] == "chargeback"
    # defaults applied for omitted fields
    assert rows[0]["reason"] == ""
    assert rows[0]["source"] == "manual"

    o = _order_row(api_db, "o2")
    assert float(o["chargebacks"]) == 40
    assert float(o["net"]) == 200 - 40
    # refunds column untouched for a chargeback
    assert float(o["refunds"]) == 0


def test_record_refund_missing_order_id_returns_error(client, api_db):
    r = client.post("/api/refunds/record", json={"amount": 10, "type": "refund"})
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is False
    assert "order_id" in body["error"]

    # nothing was written
    assert _refund_rows(api_db) == []


def test_record_refund_unknown_order_has_empty_customer_key(client, api_db):
    # No matching order row -> customer_key falls back to "" but the log is still written.
    r = client.post("/api/refunds/record", json={"order_id": "ghost", "amount": 5})
    assert r.status_code == 200
    assert r.json()["ok"] is True

    rows = _refund_rows(api_db, "ghost")
    assert len(rows) == 1
    assert rows[0]["customer_key"] == ""
    assert rows[0]["type"] == "refund"  # default type


# --------------------------------------------------------------------------- #
# GET /api/refunds/list
# --------------------------------------------------------------------------- #


def test_list_empty(client, api_db):
    r = client.get("/api/refunds/list")
    assert r.status_code == 200
    assert r.json() == {"rows": [], "count": 0}


def test_list_populated_and_order_id_filter(client, api_db):
    insert_rows(
        api_db,
        "orders",
        [
            order("oa", "2026-01-10T00:00:00Z", "ca", gross=100, net=100),
            order("ob", "2026-01-10T00:00:00Z", "cb", gross=100, net=100),
        ],
    )
    assert client.post("/api/refunds/record", json={"order_id": "oa", "amount": 10}).status_code == 200
    assert client.post("/api/refunds/record", json={"order_id": "ob", "amount": 20}).status_code == 200

    r = client.get("/api/refunds/list")
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 2
    assert len(body["rows"]) == 2
    assert {row["order_id"] for row in body["rows"]} == {"oa", "ob"}

    # order_id filter narrows to one
    r2 = client.get("/api/refunds/list", params={"order_id": "oa"})
    b2 = r2.json()
    assert b2["count"] == 1
    assert b2["rows"][0]["order_id"] == "oa"
    assert float(b2["rows"][0]["amount"]) == 10


def test_list_respects_limit(client, api_db):
    insert_rows(
        api_db,
        "orders",
        [order(f"o{i}", "2026-01-10T00:00:00Z", f"c{i}", gross=100, net=100) for i in range(3)],
    )
    for i in range(3):
        client.post("/api/refunds/record", json={"order_id": f"o{i}", "amount": i + 1})

    r = client.get("/api/refunds/list", params={"limit": 2})
    body = r.json()
    assert body["count"] == 2
    assert len(body["rows"]) == 2


# --------------------------------------------------------------------------- #
# GET /api/refunds/summary — deeper branches
# --------------------------------------------------------------------------- #


def test_summary_totals_rates_and_by_source(client, api_db):
    # Two customers, each attributed to a different platform via touchpoints.
    insert_rows(
        api_db,
        "touchpoints",
        [
            touchpoint("2026-01-09T09:00:00Z", "c1", platform="meta"),
            touchpoint("2026-01-09T09:00:00Z", "c2", platform="google"),
        ],
    )
    # gross totals 1000; we will refund 100 and charge back 50.
    insert_rows(
        api_db,
        "orders",
        [
            order("o1", "2026-01-10T12:00:00Z", "c1", gross=600, net=600),
            order("o2", "2026-01-10T12:00:00Z", "c2", gross=400, net=400),
        ],
    )

    # Record a refund on o1 (meta) and a chargeback on o2 (google).
    assert client.post("/api/refunds/record", json={"order_id": "o1", "amount": 100, "type": "refund"}).status_code == 200
    assert client.post("/api/refunds/record", json={"order_id": "o2", "amount": 50, "type": "chargeback"}).status_code == 200

    r = client.get("/api/refunds/summary")
    assert r.status_code == 200
    body = r.json()

    # totals grouped by type
    assert body["totals"]["refund"]["count"] == 1
    assert body["totals"]["refund"]["amount"] == 100.0
    assert body["totals"]["chargeback"]["count"] == 1
    assert body["totals"]["chargeback"]["amount"] == 50.0

    # order-level rollups (refunds/chargebacks were applied to the orders table by /record)
    assert body["total_orders"] == 2
    assert body["total_revenue"] == 1000.0
    # refund_rate = 100 / 1000 * 100 = 10.0 ; chargeback_rate = 50 / 1000 * 100 = 5.0
    assert body["refund_rate"] == 10.0
    assert body["chargeback_rate"] == 5.0
    # net_after_refunds = 1000 - 100 - 50
    assert body["net_after_refunds"] == 850.0

    # by_source attribution joins refund_log -> orders -> touchpoints(platform)
    by_source = {row["source"]: row for row in body["by_source"]}
    assert set(by_source) == {"meta", "google"}
    assert by_source["meta"]["refund_count"] == 1
    assert float(by_source["meta"]["refund_amount"]) == 100.0
    assert by_source["google"]["refund_count"] == 1
    assert float(by_source["google"]["refund_amount"]) == 50.0


def test_summary_date_filter_excludes_out_of_range(client, api_db):
    insert_rows(
        api_db,
        "touchpoints",
        [touchpoint("2026-01-09T09:00:00Z", "c1", platform="meta")],
    )
    insert_rows(api_db, "orders", [order("o1", "2026-01-10T12:00:00Z", "c1", gross=500, net=500)])
    assert client.post("/api/refunds/record", json={"order_id": "o1", "amount": 50, "type": "refund"}).status_code == 200

    # The recorded refund's ts is "now" (2026), so a 2020 window must exclude it from
    # the refund-derived aggregates (totals / by_source), exercising the WHERE branch.
    r = client.get(
        "/api/refunds/summary",
        params={"start_date": "2020-01-01", "end_date": "2020-12-31"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["totals"] == {}
    assert body["by_source"] == []
    # order-level stats ignore the date filter, so totals still reflect the seeded order
    assert body["total_orders"] == 1
    assert body["total_revenue"] == 500.0


def test_summary_empty_db_returns_zeroes(client, api_db):
    r = client.get("/api/refunds/summary")
    assert r.status_code == 200
    body = r.json()
    assert body["totals"] == {}
    assert body["by_source"] == []
    assert body["total_orders"] == 0
    assert body["refund_rate"] == 0
    assert body["chargeback_rate"] == 0
