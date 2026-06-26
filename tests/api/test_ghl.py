"""Tests for the GoHighLevel webhook integration (``api.ghl``).

The router is mounted under ``/api/webhooks`` in ``main.py`` (see
``app.include_router(ghl_router, prefix="/api/webhooks")``), so the master
webhook lives at ``POST /api/webhooks/ghl`` and the debug view at
``GET /api/webhooks/ghl-debug``. These routes are PUBLIC (no auth).

GHL never performs outbound HTTP in this module, so no respx mocking is
required here. We assert on response JSON *and* the DB side-effects written to
the ``orders`` / ``conversions`` / ``touchpoints`` / ``sessions`` /
``webhook_log`` tables.
"""

from __future__ import annotations

import sqlite3

import pytest


def _rows(db_path: str, sql: str, params=()):
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


def _count(db_path: str, table: str, where: str = "", params=()) -> int:
    sql = f"SELECT COUNT(*) AS n FROM {table}"
    if where:
        sql += f" WHERE {where}"
    return _rows(db_path, sql, params)[0]["n"]


# ── Contact events ─────────────────────────────────────────────────────────────

def test_contact_create_only_stitches_matching_sessions(client, api_db):
    # Seed two anonymous browser paths. A bare contact webhook should not claim
    # either one, because that would attach unrelated visitors to the same lead.
    with sqlite3.connect(api_db) as conn:
        conn.execute(
            "INSERT INTO sessions (session_id, visitor_id, ts, customer_key) VALUES (?,?,?,?)",
            ("sessA", "visA", "2026-06-01T00:00:00Z", ""),
        )
        conn.execute(
            "INSERT INTO sessions (session_id, visitor_id, ts, customer_key) VALUES (?,?,?,?)",
            ("sessB", "visB", "2026-06-01T00:05:00Z", ""),
        )
        conn.execute(
            "INSERT INTO touchpoints (ts, channel, platform, customer_key, session_id, visitor_id) VALUES (?,?,?,?,?,?)",
            ("2026-06-01T00:00:00Z", "paid", "meta", "", "sessA", "visA"),
        )
        conn.execute(
            "INSERT INTO touchpoints (ts, channel, platform, customer_key, session_id, visitor_id) VALUES (?,?,?,?,?,?)",
            ("2026-06-01T00:05:00Z", "paid", "google", "", "sessB", "visB"),
        )
        conn.commit()

    bare = client.post(
        "/api/webhooks/ghl",
        json={"type": "ContactCreate", "email": "Buyer@Example.com", "firstName": "Jane", "lastName": "Doe"},
    )
    assert bare.status_code == 200
    customer_key = bare.json()["customer_key"]
    assert customer_key
    assert _count(api_db, "touchpoints", "customer_key = ?", (customer_key,)) == 0
    assert _count(api_db, "sessions", "customer_key = ?", (customer_key,)) == 0

    resp = client.post(
        "/api/webhooks/ghl",
        json={
            "type": "ContactUpdate",
            "email": "Buyer@Example.com",
            "hyros_session_id": "sessA",
            "hyros_visitor_id": "visA",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["event_type"] == "ContactUpdate"
    assert body["action"] == "contact_identified"
    assert body["customer_key"] == customer_key

    # Only the matching visitor/session path should now carry the customer_key.
    assert _count(api_db, "touchpoints", "customer_key = ?", (customer_key,)) == 1
    assert _count(api_db, "sessions", "customer_key = ?", (customer_key,)) == 1
    assert _count(api_db, "sessions", "session_id = ? AND customer_key = ''", ("sessB",)) == 1


def test_email_is_normalized_lowercase_in_customer_key(client, api_db):
    upper = client.post("/api/webhooks/ghl", json={"type": "ContactCreate", "email": "PERSON@X.COM"})
    lower = client.post("/api/webhooks/ghl", json={"type": "ContactCreate", "email": "person@x.com"})
    assert upper.json()["customer_key"] == lower.json()["customer_key"]


# ── Form submission (lead) ──────────────────────────────────────────────────────

def test_form_submission_creates_lead_session_touchpoint(client, api_db):
    resp = client.post(
        "/api/webhooks/ghl",
        json={
            "type": "FormSubmission",
            "email": "lead@example.com",
            "form_name": "Newsletter Signup",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["action"] == "lead_tracked"
    assert body["form_name"] == "Newsletter Signup"
    ck = body["customer_key"]

    # A Lead conversion, a session and a touchpoint should be written.
    leads = _rows(api_db, "SELECT * FROM conversions WHERE customer_key = ? AND type = 'Lead'", (ck,))
    assert len(leads) == 1
    assert _count(api_db, "sessions", "customer_key = ? AND landing_page = ?", (ck, "Newsletter Signup")) == 1
    assert _count(api_db, "touchpoints", "customer_key = ?", (ck,)) == 1
    # The form path logs to webhook_log.
    assert _count(api_db, "webhook_log", "source = 'ghl'") >= 1


# ── Appointment / booking ───────────────────────────────────────────────────────

def test_appointment_booked_creates_booking_conversion(client, api_db):
    resp = client.post(
        "/api/webhooks/ghl",
        json={
            "type": "AppointmentBooked",
            "email": "booker@example.com",
            "calendarName": "Strategy Call",
            "startTime": "2026-07-01T15:00:00Z",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["action"] == "booking_tracked"
    assert body["calendar"] == "Strategy Call"
    ck = body["customer_key"]

    bookings = _rows(api_db, "SELECT * FROM conversions WHERE customer_key = ? AND type = 'Booking'", (ck,))
    assert len(bookings) == 1
    assert _count(api_db, "sessions", "customer_key = ? AND landing_page = ?", (ck, "Strategy Call")) == 1


# ── Opportunity events ──────────────────────────────────────────────────────────

def test_opportunity_open_stage_records_opportunity_conversion(client, api_db):
    resp = client.post(
        "/api/webhooks/ghl",
        json={
            "type": "OpportunityCreate",
            "email": "prospect@example.com",
            "opportunity": {"id": "opp-1", "stage": "New Lead", "pipeline": "Sales", "value": 500},
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["action"] == "opportunity_tracked"
    assert body["stage"] == "New Lead"
    assert body["value"] == 500.0
    ck = body["customer_key"]

    convs = _rows(api_db, "SELECT * FROM conversions WHERE customer_key = ? AND type = 'Opportunity'", (ck,))
    assert len(convs) == 1
    # An open opportunity is NOT a purchase, so no order is written.
    assert _count(api_db, "orders", "customer_key = ?", (ck,)) == 0


def test_opportunity_won_stage_creates_order_and_purchase(client, api_db):
    resp = client.post(
        "/api/webhooks/ghl",
        json={
            "type": "OpportunityStageUpdate",
            "email": "winner@example.com",
            "utm_source": "facebook",
            "opportunity": {"id": "opp-won", "stage": "Closed Won", "value": 1000},
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["action"] == "opportunity_tracked"
    assert body["stage"] == "Closed Won"
    assert body["value"] == 1000.0
    ck = body["customer_key"]

    orders = _rows(api_db, "SELECT * FROM orders WHERE customer_key = ?", (ck,))
    assert len(orders) == 1
    assert float(orders[0]["gross"]) == 1000.0
    # Fees = value * 0.029 + 0.30.
    assert float(orders[0]["fees"]) == pytest.approx(1000 * 0.029 + 0.30, abs=0.01)

    purchases = _rows(api_db, "SELECT * FROM conversions WHERE customer_key = ? AND type = 'Purchase'", (ck,))
    assert len(purchases) == 1
    # utm_source present → a touchpoint is also recorded, attributed to meta.
    tps = _rows(api_db, "SELECT * FROM touchpoints WHERE customer_key = ?", (ck,))
    assert len(tps) == 1
    assert tps[0]["platform"] == "meta"


# ── Payment events ──────────────────────────────────────────────────────────────

def test_payment_received_creates_order_and_purchase(client, api_db):
    resp = client.post(
        "/api/webhooks/ghl",
        json={
            "type": "PaymentReceived",
            "email": "payer@example.com",
            "amount": 250.5,
            "payment_id": "pay-123",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["action"] == "payment_tracked"
    assert body["value"] == 250.5
    ck = body["customer_key"]
    order_id = body["order_id"]

    orders = _rows(api_db, "SELECT * FROM orders WHERE order_id = ?", (order_id,))
    assert len(orders) == 1
    assert float(orders[0]["gross"]) == 250.5
    assert orders[0]["customer_key"] == ck

    purchases = _rows(api_db, "SELECT * FROM conversions WHERE customer_key = ? AND type = 'Purchase'", (ck,))
    assert len(purchases) == 1


def test_payment_zero_value_is_skipped(client, api_db):
    resp = client.post(
        "/api/webhooks/ghl",
        json={"type": "InvoicePaid", "email": "free@example.com", "amount": 0},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["skipped"] is True
    assert body["reason"] == "zero_value"
    # Nothing persisted.
    assert _count(api_db, "orders") == 0
    assert _count(api_db, "conversions") == 0


def test_payment_order_id_is_deterministic(client, api_db):
    # The order_id is derived deterministically from the payment_id, so the
    # same payment maps to the same order_id on every delivery. (The base
    # schema has no UNIQUE constraint on orders.order_id, so the INSERT OR
    # IGNORE does not collapse the rows at the DB level — we only assert the
    # ID derivation is stable here.)
    payload = {"type": "PaymentReceived", "email": "dup@example.com", "amount": 99, "payment_id": "same-id"}
    first = client.post("/api/webhooks/ghl", json=payload)
    second = client.post("/api/webhooks/ghl", json=payload)
    assert first.json()["order_id"] == second.json()["order_id"]
    assert _count(api_db, "orders", "order_id = ?", (first.json()["order_id"],)) >= 1


# ── Inference (no explicit type) ────────────────────────────────────────────────

def test_inferred_payment_from_amount_field(client, api_db):
    # No "type"/"event" field at all — _infer_event_type sees amount > 0.
    resp = client.post(
        "/api/webhooks/ghl",
        json={"email": "inferred@example.com", "amount": 42, "payment_id": "inf-1"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["event_type"] == "PaymentReceived"
    assert body["action"] == "payment_tracked"
    assert _count(api_db, "orders") == 1


def test_inferred_form_from_email_only(client, api_db):
    # An email-only payload with no recognizable indicators → FormSubmission.
    resp = client.post("/api/webhooks/ghl", json={"email": "bare@example.com"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["event_type"] == "FormSubmission"
    assert body["action"] == "lead_tracked"


# ── Unknown / no-email fallbacks ────────────────────────────────────────────────

def test_unknown_event_without_email_is_only_logged(client, api_db):
    resp = client.post("/api/webhooks/ghl", json={"type": "NoteCreate", "note": "hello"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["action"] == "logged"
    assert "no email found" in body["note"]
    assert _count(api_db, "orders") == 0
    assert _count(api_db, "conversions") == 0
    # Still logged for debugging.
    assert _count(api_db, "webhook_log", "source = 'ghl'") >= 1


def test_unknown_event_with_email_falls_back_to_lead(client, api_db):
    resp = client.post("/api/webhooks/ghl", json={"type": "TaskCreate", "email": "task@example.com"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["action"] == "lead_tracked_fallback"
    ck = body["customer_key"]
    assert _count(api_db, "conversions", "customer_key = ? AND type = 'Lead'", (ck,)) == 1
    assert _count(api_db, "touchpoints", "customer_key = ?", (ck,)) == 1
    assert _count(api_db, "sessions", "customer_key = ?", (ck,)) == 1


def test_empty_body_is_handled(client, api_db):
    resp = client.post("/api/webhooks/ghl", content=b"")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["action"] == "logged"


# ── Source / platform resolution via custom params ──────────────────────────────

def test_tiktok_click_id_resolves_to_tiktok_platform(client, api_db):
    resp = client.post(
        "/api/webhooks/ghl",
        json={
            "type": "PaymentReceived",
            "email": "tt@example.com",
            "amount": 80,
            "ttc_id": "TT123",
            "utm_source": "tiktok",
        },
    )
    assert resp.status_code == 200
    ck = resp.json()["customer_key"]
    tps = _rows(api_db, "SELECT * FROM touchpoints WHERE customer_key = ?", (ck,))
    assert len(tps) == 1
    assert tps[0]["platform"] == "tiktok"
    assert tps[0]["channel"] == "paid_social"


# ── Debug endpoint ──────────────────────────────────────────────────────────────

def test_payment_preserves_source_and_identity_fields(client, api_db):
    resp = client.post(
        "/api/webhooks/ghl",
        json={
            "type": "PaymentReceived",
            "email": "source@example.com",
            "amount": 120,
            "payment_id": "pay-source-1",
            "hyros_session_id": "sess-pay",
            "hyros_visitor_id": "vis-pay",
            "utm_source": "tiktok",
            "ttclid": "tt-click-1",
            "ttc_id": "tt-campaign-1",
            "campaign_id": "camp-1",
            "ad_id": "ad-1",
            "creative_id": "creative-1",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    ck = body["customer_key"]

    orders = _rows(api_db, "SELECT * FROM orders WHERE order_id = ?", (body["order_id"],))
    assert len(orders) == 1
    assert orders[0]["customer_key"] == ck
    assert orders[0]["session_id"] == "sess-pay"
    assert orders[0]["visitor_id"] == "vis-pay"
    assert orders[0]["platform"] == "tiktok"
    assert orders[0]["channel"] == "paid_social"
    assert orders[0]["campaign_id"] == "camp-1"
    assert orders[0]["ad_id"] == "ad-1"
    assert orders[0]["creative_id"] == "creative-1"
    assert orders[0]["ttclid"] == "tt-click-1"

    purchases = _rows(api_db, "SELECT * FROM conversions WHERE order_id = ?", (body["order_id"],))
    assert len(purchases) == 1
    assert purchases[0]["session_id"] == "sess-pay"
    assert purchases[0]["visitor_id"] == "vis-pay"

    tps = _rows(api_db, "SELECT * FROM touchpoints WHERE customer_key = ?", (ck,))
    assert len(tps) == 1
    assert tps[0]["session_id"] == "sess-pay"
    assert tps[0]["visitor_id"] == "vis-pay"
    assert tps[0]["platform"] == "tiktok"


def test_ghl_debug_returns_logged_webhooks(client, api_db):
    # Trigger a path that logs (form submission).
    client.post("/api/webhooks/ghl", json={"type": "FormSubmission", "email": "d@example.com"})
    resp = client.get("/api/webhooks/ghl-debug")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] >= 1
    assert all(r["source"] == "ghl" for r in body["rows"])


def test_ghl_debug_empty_on_fresh_db(client, api_db):
    resp = client.get("/api/webhooks/ghl-debug")
    assert resp.status_code == 200
    body = resp.json()
    assert body["rows"] == []
    assert body["count"] == 0
