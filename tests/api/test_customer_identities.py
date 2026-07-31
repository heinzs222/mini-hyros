"""Names and emails behind the hashed customer_key.

``customer_key`` is a hash, so every ingestion path used to discard the email
and name after computing it and the CRM could only render the hash. These
cover the contact book that keeps them: written on ingestion (Stripe sync, GHL
sync, webhooks) and read back by the journey/lead endpoints.
"""

from __future__ import annotations

import sqlite3

import httpx
import respx

from tests.helpers import insert_rows, order


def _identities(db_path: str) -> dict[str, dict[str, str]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute("SELECT * FROM customer_identities").fetchall()
        except sqlite3.OperationalError:
            return {}
    return {str(r["customer_key"]): dict(r) for r in rows}


def _conv(cid: str, ts: str, ctype: str, ck: str, value: float = 0.0, order_id: str = "") -> dict:
    return {
        "conversion_id": cid, "ts": ts, "type": ctype,
        "value": str(value), "order_id": order_id, "customer_key": ck,
    }


# ── Writers ────────────────────────────────────────────────────────────────────

def test_stripe_sync_records_name_and_email(client, api_db, monkeypatch):
    monkeypatch.setenv("STRIPE_API_SECRET_KEY", "sk_test_123")

    charge = {
        "id": "ch_1",
        "paid": True,
        "amount": 49900,
        "amount_refunded": 0,
        "created": 1767000000,
        "currency": "cad",
        "payment_intent": "pi_1",
        "billing_details": {
            "email": "Marie.Tremblay@example.com",
            "name": "Marie Tremblay",
            "phone": "+15145550123",
        },
    }
    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith="https://api.stripe.com/v1/charges").mock(
            return_value=httpx.Response(200, json={"data": [charge], "has_more": False})
        )
        resp = client.post("/api/stripe/sync", params={"start_date": "2025-12-29", "end_date": "2025-12-29"})

    assert resp.status_code == 200
    identities = _identities(api_db)
    assert len(identities) == 1
    record = next(iter(identities.values()))
    # Email is normalized to lowercase so it matches the hashing input.
    assert record["email"] == "marie.tremblay@example.com"
    assert record["name"] == "Marie Tremblay"
    assert record["phone"] == "+15145550123"
    assert record["source"] == "stripe"


def test_ghl_webhook_records_contact_identity(client, api_db):
    resp = client.post(
        "/api/webhooks/ghl",
        json={
            "type": "ContactCreate",
            "email": "Jean@example.com",
            "firstName": "Jean",
            "lastName": "Dupuis",
            "phone": "+15145550000",
            "contact_id": "c-1",
        },
    )
    assert resp.status_code == 200
    customer_key = resp.json()["customer_key"]

    record = _identities(api_db)[customer_key]
    assert record["email"] == "jean@example.com"
    assert record["name"] == "Jean Dupuis"


def test_identity_writes_never_unlearn_a_known_field(api_db):
    from attributionops.db import connect
    from attributionops.schema import ensure_customer_identities, upsert_customer_identity

    with connect(api_db) as conn:
        ensure_customer_identities(conn)
        # Stripe knows the email; a later GHL contact adds the name.
        upsert_customer_identity(conn, "ck1", email="a@b.com", source="stripe")
        upsert_customer_identity(conn, "ck1", name="Alex Roy", source="ghl")
        # A source with neither must not blank what we already had.
        assert upsert_customer_identity(conn, "ck1", email="", name="") is False
        conn.commit()

    record = _identities(api_db)["ck1"]
    assert record["email"] == "a@b.com"
    assert record["name"] == "Alex Roy"
    assert record["source"] == "ghl"


def test_identity_ignores_rows_without_a_customer_key(api_db):
    from attributionops.db import connect
    from attributionops.schema import ensure_customer_identities, upsert_customer_identity

    with connect(api_db) as conn:
        ensure_customer_identities(conn)
        assert upsert_customer_identity(conn, "", email="ghost@example.com") is False
        conn.commit()

    assert _identities(api_db) == {}


# ── Readers ────────────────────────────────────────────────────────────────────

def _seed_lead(api_db, customer_key: str) -> None:
    insert_rows(api_db, "conversions", [_conv("k1", "2026-01-10T09:00:00Z", "lead", customer_key)])
    insert_rows(api_db, "orders", [order("o1", "2026-01-11T09:00:00Z", customer_key, gross=250)])
    insert_rows(api_db, "conversions", [
        _conv("k2", "2026-01-11T09:00:00Z", "purchase", customer_key, value=250, order_id="o1"),
    ])


def test_lead_rows_show_the_person_not_the_hash(client, api_db):
    customer_key = "2f5a51ba52ee4d3b9c8a71f0d2e3b4c5"
    _seed_lead(api_db, customer_key)
    insert_rows(api_db, "customer_identities", [{
        "customer_key": customer_key,
        "email": "marie.tremblay@example.com",
        "name": "Marie Tremblay",
        "phone": "",
        "source": "stripe",
        "updated_at": "2026-01-11T09:00:00Z",
    }])

    rows = client.get("/api/journey/leads").json()["rows"]
    assert rows
    for row in rows:
        assert row["name"] == "Marie Tremblay"
        assert row["email"] == "marie.tremblay@example.com"
        assert row["display_name"] == "Marie Tremblay"
        assert row["customer_key_short"] == "Marie Tremblay"
        # The hash is still available for lookups/links.
        assert row["customer_key"] == customer_key


def test_lead_rows_fall_back_to_email_then_hash(client, api_db):
    with_email = "aaaa1111bbbb2222cccc3333dddd4444"
    without = "9999888877776666555544443333222"
    _seed_lead(api_db, with_email)
    insert_rows(api_db, "conversions", [_conv("k3", "2026-01-12T09:00:00Z", "lead", without)])
    insert_rows(api_db, "customer_identities", [{
        "customer_key": with_email,
        "email": "no.name@example.com",
        "name": "",
        "phone": "",
        "source": "stripe",
        "updated_at": "2026-01-11T09:00:00Z",
    }])

    rows = {row["customer_key"]: row for row in client.get("/api/journey/leads").json()["rows"]}
    assert rows[with_email]["display_name"] == "no.name@example.com"
    # An unknown contact still renders exactly as it did before.
    assert rows[without]["display_name"] == without
    assert rows[without]["customer_key_short"].endswith("...")


def test_customer_journey_returns_the_contact_details(client, api_db):
    customer_key = "abcdef0123456789abcdef0123456789"
    _seed_lead(api_db, customer_key)
    insert_rows(api_db, "customer_identities", [{
        "customer_key": customer_key,
        "email": "buyer@example.com",
        "name": "Buyer One",
        "phone": "+15145559999",
        "source": "ghl",
        "updated_at": "2026-01-11T09:00:00Z",
    }])

    body = client.get("/api/journey/customer", params={"customer_key": customer_key}).json()
    assert body["name"] == "Buyer One"
    assert body["email"] == "buyer@example.com"
    assert body["phone"] == "+15145559999"
    assert body["display_name"] == "Buyer One"


def test_lead_rows_survive_a_warehouse_without_the_contact_book(client, api_db):
    """Older databases predate customer_identities; the CRM must still load."""
    customer_key = "1111222233334444555566667777888"
    _seed_lead(api_db, customer_key)
    with sqlite3.connect(api_db) as conn:
        conn.execute("DROP TABLE IF EXISTS customer_identities")
        conn.commit()

    body = client.get("/api/journey/leads").json()
    assert body["count"] >= 1
    assert body["rows"][0]["display_name"] == customer_key
