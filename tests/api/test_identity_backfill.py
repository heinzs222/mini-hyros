"""Filling in names for rows that were ingested before identities were kept.

The identity behind a hashed customer_key is only recorded at ingestion, and no
ordinary sync repairs history: Stripe skips ranges its coverage ledger already
covers, and the lead writers skip contacts outside the reporting window. Without
a backfill every pre-existing CRM row stays a hash forever.
"""

from __future__ import annotations

import sqlite3

import httpx
import respx

from tests.helpers import insert_rows, order


def _identities(db_path: str) -> dict[str, dict]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute("SELECT * FROM customer_identities").fetchall()
        except sqlite3.OperationalError:
            return {}
    return {str(r["customer_key"]): dict(r) for r in rows}


def _clear_sources(monkeypatch):
    for var in ("STRIPE_API_SECRET_KEY", "STRIPE_SECRET_KEY", "GHL_API_TOKEN",
                "GHL_ACCESS_TOKEN", "GHL_LOCATION_ID"):
        monkeypatch.delenv(var, raising=False)


def test_backfill_matches_stripe_charges_to_existing_orders(client, api_db, monkeypatch):
    _clear_sources(monkeypatch)
    monkeypatch.setenv("STRIPE_API_SECRET_KEY", "sk_test_1")

    # An order already in the warehouse, stored under a key the CRM renders raw.
    insert_rows(api_db, "orders", [order("stripe|pi_1", "2026-07-30T12:00:00Z", "ck-existing", gross=250)])

    charge = {
        "id": "ch_1",
        "paid": True,
        "amount": 25000,
        "amount_refunded": 0,
        "created": 1785412800,
        "currency": "cad",
        "payment_intent": "pi_1",
        "billing_details": {"email": "Buyer@Example.com", "name": "Real Buyer", "phone": "+15145551212"},
    }
    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith="https://api.stripe.com/v1/charges").mock(
            return_value=httpx.Response(200, json={"data": [charge], "has_more": False})
        )
        body = client.post("/api/crm/backfill-identities", params={"days": 30}).json()

    assert body["written"] == 1
    assert body["sources"]["stripe"]["fetched"] == 1
    # Keyed to what the order already carries, not a freshly computed hash, so
    # the CRM row actually resolves.
    record = _identities(api_db)["ck-existing"]
    assert record["name"] == "Real Buyer"
    assert record["email"] == "buyer@example.com"


def test_backfill_reports_sources_that_are_not_configured(client, api_db, monkeypatch):
    _clear_sources(monkeypatch)
    body = client.post("/api/crm/backfill-identities", params={"days": 30}).json()

    assert body["written"] == 0
    assert body["sources"]["stripe"]["skipped"] is True
    assert body["sources"]["ghl"]["skipped"] is True


def test_one_failing_source_does_not_sink_the_other(client, api_db, monkeypatch):
    _clear_sources(monkeypatch)
    monkeypatch.setenv("STRIPE_API_SECRET_KEY", "sk_test_1")

    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith="https://api.stripe.com/v1/charges").mock(
            return_value=httpx.Response(500, json={"error": {"message": "boom"}})
        )
        body = client.post("/api/crm/backfill-identities", params={"days": 30}).json()

    assert "error" in body["sources"]["stripe"]
    assert body["sources"]["ghl"]["skipped"] is True


def test_coverage_reports_how_many_customers_are_still_hashes(client, api_db):
    insert_rows(api_db, "conversions", [
        {"conversion_id": "c1", "ts": "2026-07-30T10:00:00Z", "type": "lead",
         "value": "0", "order_id": "", "customer_key": "known"},
        {"conversion_id": "c2", "ts": "2026-07-30T11:00:00Z", "type": "lead",
         "value": "0", "order_id": "", "customer_key": "unknown"},
    ])
    insert_rows(api_db, "customer_identities", [{
        "customer_key": "known", "email": "a@b.com", "name": "A B",
        "phone": "", "source": "stripe", "updated_at": "2026-07-30T10:00:00Z",
    }])

    body = client.get("/api/crm/identity-coverage").json()
    assert body["customers_total"] == 2
    assert body["customers_with_identity"] == 1
    assert body["missing"] == 1
