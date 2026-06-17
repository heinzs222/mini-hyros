"""Tests for backend/api/webhooks.py — inbound webhook ingestion.

Covers the five POST routes mounted under /api/webhooks:
  /shopify, /stripe, /track, /identify, /conversion.

Each test exercises the HTTP contract (status + JSON) AND the SQLite
side-effects (orders / conversions / sessions / touchpoints), plus the
HMAC / signature enforcement branches where applicable.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import sqlite3

import httpx
import respx

# webhooks.py is importable because conftest puts backend/ on sys.path.
from api import webhooks


# ── small DB helpers ───────────────────────────────────────────────────────────

def _rows(db_path: str, sql: str, params=()) -> list[dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


# ── /shopify ───────────────────────────────────────────────────────────────────

def test_shopify_no_secret_inserts_order(client, api_db, monkeypatch):
    monkeypatch.delenv("SHOPIFY_WEBHOOK_SECRET", raising=False)
    payload = {
        "id": 9001,
        "email": "buyer@example.com",
        "total_price": "100.00",
        "created_at": "2026-06-01T10:00:00Z",
    }
    resp = client.post("/api/webhooks/shopify", json=payload)
    assert resp.status_code == 200
    body = resp.json()
    assert body == {"ok": True, "order_id": "9001"}

    orders = _rows(api_db, "SELECT * FROM orders WHERE order_id = ?", ("9001",))
    assert len(orders) == 1
    o = orders[0]
    assert float(o["gross"]) == 100.0
    assert float(o["net"]) == 100.0
    # fee estimate = gross*0.029 + 0.30 = 3.20
    assert float(o["fees"]) == 3.20
    assert o["customer_key"] == _sha256("buyer@example.com")

    convs = _rows(api_db, "SELECT * FROM conversions WHERE order_id = ?", ("9001",))
    assert len(convs) == 1
    assert convs[0]["type"] == "Purchase"
    assert float(convs[0]["value"]) == 100.0


def test_shopify_refunds_reduce_net(client, api_db, monkeypatch):
    monkeypatch.delenv("SHOPIFY_WEBHOOK_SECRET", raising=False)
    payload = {
        "id": 9002,
        "email": "r@example.com",
        "total_price": "200.00",
        "refunds": [{"transactions": [{"amount": "50.00"}]}],
    }
    resp = client.post("/api/webhooks/shopify", json=payload)
    assert resp.status_code == 200
    o = _rows(api_db, "SELECT * FROM orders WHERE order_id = ?", ("9002",))[0]
    assert float(o["gross"]) == 200.0
    assert float(o["refunds"]) == 50.0
    assert float(o["net"]) == 150.0


def test_shopify_customer_nested_email(client, api_db, monkeypatch):
    monkeypatch.delenv("SHOPIFY_WEBHOOK_SECRET", raising=False)
    payload = {
        "id": 9003,
        "customer": {"email": "nested@example.com"},
        "total_price": "10.00",
    }
    resp = client.post("/api/webhooks/shopify", json=payload)
    assert resp.status_code == 200
    o = _rows(api_db, "SELECT * FROM orders WHERE order_id = ?", ("9003",))[0]
    assert o["customer_key"] == _sha256("nested@example.com")


def _shopify_hmac(secret: str, body: bytes) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def test_shopify_valid_hmac_accepted(client, api_db, monkeypatch):
    secret = "shh-secret"
    monkeypatch.setenv("SHOPIFY_WEBHOOK_SECRET", secret)
    payload = {"id": 9100, "email": "h@example.com", "total_price": "42.00"}
    body = json.dumps(payload).encode()
    sig = _shopify_hmac(secret, body)
    resp = client.post(
        "/api/webhooks/shopify",
        content=body,
        headers={"X-Shopify-Hmac-Sha256": sig, "Content-Type": "application/json"},
    )
    assert resp.status_code == 200
    assert resp.json()["order_id"] == "9100"
    assert len(_rows(api_db, "SELECT * FROM orders WHERE order_id = ?", ("9100",))) == 1


def test_shopify_invalid_hmac_rejected(client, api_db, monkeypatch):
    monkeypatch.setenv("SHOPIFY_WEBHOOK_SECRET", "shh-secret")
    payload = {"id": 9101, "email": "h@example.com", "total_price": "42.00"}
    body = json.dumps(payload).encode()
    resp = client.post(
        "/api/webhooks/shopify",
        content=body,
        headers={"X-Shopify-Hmac-Sha256": "deadbeef", "Content-Type": "application/json"},
    )
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid HMAC"
    # No row written.
    assert _rows(api_db, "SELECT * FROM orders WHERE order_id = ?", ("9101",)) == []


# ── /stripe ────────────────────────────────────────────────────────────────────

def test_stripe_checkout_completed_inserts_order(client, api_db, monkeypatch):
    monkeypatch.delenv("STRIPE_WEBHOOK_SECRET", raising=False)
    payload = {
        "type": "checkout.session.completed",
        "data": {
            "object": {
                "id": "cs_test_1",
                "customer_email": "s@example.com",
                "amount_total": 12345,  # cents -> 123.45
                "subscription": "sub_123",
            }
        },
    }
    resp = client.post("/api/webhooks/stripe", json=payload)
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "order_id": "cs_test_1"}

    o = _rows(api_db, "SELECT * FROM orders WHERE order_id = ?", ("cs_test_1",))[0]
    assert float(o["gross"]) == 123.45
    assert float(o["net"]) == 123.45
    assert o["customer_key"] == _sha256("s@example.com")
    assert o["subscription_id"] == "sub_123"

    convs = _rows(api_db, "SELECT * FROM conversions WHERE order_id = ?", ("cs_test_1",))
    assert len(convs) == 1


def test_stripe_charge_succeeded_uses_amount_and_receipt_email(client, api_db, monkeypatch):
    monkeypatch.delenv("STRIPE_WEBHOOK_SECRET", raising=False)
    payload = {
        "type": "charge.succeeded",
        "data": {
            "object": {
                "id": "ch_1",
                "receipt_email": "c@example.com",
                "amount": 5000,  # cents -> 50.00
            }
        },
    }
    resp = client.post("/api/webhooks/stripe", json=payload)
    assert resp.status_code == 200
    o = _rows(api_db, "SELECT * FROM orders WHERE order_id = ?", ("ch_1",))[0]
    assert float(o["gross"]) == 50.0
    assert o["customer_key"] == _sha256("c@example.com")


def test_stripe_payment_intent_uses_payment_intent_id(client, api_db, monkeypatch):
    monkeypatch.delenv("STRIPE_WEBHOOK_SECRET", raising=False)
    payload = {
        "type": "payment_intent.succeeded",
        "data": {"object": {"payment_intent": "pi_99", "amount": 999}},
    }
    resp = client.post("/api/webhooks/stripe", json=payload)
    assert resp.status_code == 200
    assert resp.json()["order_id"] == "pi_99"
    assert len(_rows(api_db, "SELECT * FROM orders WHERE order_id = ?", ("pi_99",))) == 1


def test_stripe_unsupported_event_skipped(client, api_db, monkeypatch):
    monkeypatch.delenv("STRIPE_WEBHOOK_SECRET", raising=False)
    payload = {"type": "customer.created", "data": {"object": {"id": "cus_1"}}}
    resp = client.post("/api/webhooks/stripe", json=payload)
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "skipped": True, "event_type": "customer.created"}
    assert _rows(api_db, "SELECT * FROM orders") == []


def test_stripe_secret_set_missing_signature_rejected(client, api_db, monkeypatch):
    monkeypatch.setenv("STRIPE_WEBHOOK_SECRET", "whsec_x")
    payload = {"type": "charge.succeeded", "data": {"object": {"id": "ch_x", "amount": 100}}}
    resp = client.post("/api/webhooks/stripe", json=payload)
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Missing Stripe-Signature"
    assert _rows(api_db, "SELECT * FROM orders") == []


def test_stripe_secret_set_with_signature_accepted(client, api_db, monkeypatch):
    monkeypatch.setenv("STRIPE_WEBHOOK_SECRET", "whsec_x")
    payload = {"type": "charge.succeeded", "data": {"object": {"id": "ch_ok", "amount": 100}}}
    resp = client.post(
        "/api/webhooks/stripe",
        json=payload,
        headers={"Stripe-Signature": "t=1,v1=anything"},
    )
    assert resp.status_code == 200
    assert resp.json()["order_id"] == "ch_ok"
    assert len(_rows(api_db, "SELECT * FROM orders WHERE order_id = ?", ("ch_ok",))) == 1


# ── /track ─────────────────────────────────────────────────────────────────────

def test_track_pageview_writes_session_and_touchpoint(client, api_db):
    payload = {
        "session_id": "sess-track-1",
        "visitor_id": "vis-1",
        "utm_source": "facebook",
        "landing_page": "/lp",
        "device": "mobile",
    }
    resp = client.post("/api/webhooks/track", json=payload)
    assert resp.status_code == 200
    assert resp.json() == {"ok": True, "session_id": "sess-track-1"}

    sess = _rows(api_db, "SELECT * FROM sessions WHERE session_id = ?", ("sess-track-1",))
    assert len(sess) == 1
    assert sess[0]["visitor_id"] == "vis-1"
    assert sess[0]["event_name"] == "pageview"
    assert sess[0]["landing_page"] == "/lp"
    assert sess[0]["device"] == "mobile"

    tp = _rows(api_db, "SELECT * FROM touchpoints WHERE session_id = ?", ("sess-track-1",))
    assert len(tp) == 1
    # utm_source facebook -> meta / paid_social
    assert tp[0]["platform"] == "meta"
    assert tp[0]["channel"] == "paid_social"


def test_track_email_in_custom_data_becomes_hashed_customer_key(client, api_db):
    payload = {
        "session_id": "sess-track-2",
        "custom_data": {"email": "Track@Example.com"},
        "utm_source": "google",
    }
    resp = client.post("/api/webhooks/track", json=payload)
    assert resp.status_code == 200
    sess = _rows(api_db, "SELECT * FROM sessions WHERE session_id = ?", ("sess-track-2",))[0]
    # email lower-cased then hashed
    assert sess["customer_key"] == _sha256("track@example.com")
    # google utm -> paid_search
    tp = _rows(api_db, "SELECT * FROM touchpoints WHERE session_id = ?", ("sess-track-2",))[0]
    assert tp["platform"] == "google"
    assert tp["channel"] == "paid_search"


def test_track_tiktok_h_ad_id_maps_to_campaign(client, api_db):
    payload = {
        "session_id": "sess-track-3",
        "detected_platform": "tiktok",
        "h_ad_id": "hh-123",
    }
    resp = client.post("/api/webhooks/track", json=payload)
    assert resp.status_code == 200
    tp = _rows(api_db, "SELECT * FROM touchpoints WHERE session_id = ?", ("sess-track-3",))[0]
    assert tp["platform"] == "tiktok"
    assert tp["channel"] == "paid_social"
    # for tiktok, h_ad_id -> campaign_id
    assert tp["campaign_id"] == "hh-123"


def test_track_email_medium_channel(client, api_db):
    payload = {"session_id": "sess-track-email", "utm_medium": "email"}
    resp = client.post("/api/webhooks/track", json=payload)
    assert resp.status_code == 200
    tp = _rows(api_db, "SELECT * FROM touchpoints WHERE session_id = ?", ("sess-track-email",))[0]
    assert tp["channel"] == "email"
    assert tp["platform"] == ""


def test_track_meta_campaign_resolved_from_adset(client, api_db):
    # Seed an ad_names mapping adset -> campaign.
    conn = sqlite3.connect(api_db)
    conn.execute(
        "INSERT INTO ad_names (platform, entity_type, entity_id, name, parent_id, source, updated_at)"
        " VALUES ('meta','adset','adset-77','My Adset','camp-from-adset','manual','2026-01-01T00:00:00Z')"
    )
    conn.commit()
    conn.close()

    payload = {
        "session_id": "sess-track-meta",
        "detected_platform": "meta",
        "adset_id": "adset-77",
    }
    resp = client.post("/api/webhooks/track", json=payload)
    assert resp.status_code == 200
    tp = _rows(api_db, "SELECT * FROM touchpoints WHERE session_id = ?", ("sess-track-meta",))[0]
    assert tp["adset_id"] == "adset-77"
    assert tp["campaign_id"] == "camp-from-adset"


def test_track_form_submit_event(client, api_db):
    payload = {
        "session_id": "sess-form",
        "event": "FormSubmit",
        "custom_data": {"form_name": "contact", "email": "f@x.com"},
        "landing_page": "/contact",
    }
    resp = client.post("/api/webhooks/track", json=payload)
    assert resp.status_code == 200
    sess = _rows(api_db, "SELECT * FROM sessions WHERE session_id = ?", ("sess-form",))[0]
    assert sess["event_name"] == "FormSubmit"


def test_track_booking_confirmed_event(client, api_db):
    payload = {
        "session_id": "sess-booking",
        "event": "BookingConfirmed",
        "custom_data": {"calendar": "demo-call"},
    }
    resp = client.post("/api/webhooks/track", json=payload)
    assert resp.status_code == 200
    sess = _rows(api_db, "SELECT * FROM sessions WHERE session_id = ?", ("sess-booking",))[0]
    assert sess["event_name"] == "BookingConfirmed"


def test_track_custom_event_broadcasts_session(client, api_db):
    # Event that is neither pageview / FormSubmit / BookingConfirmed exercises
    # the generic "new_session" broadcast branch with the event attached.
    payload = {"session_id": "sess-custom-evt", "event": "ScrollDepth", "utm_source": "google"}
    resp = client.post("/api/webhooks/track", json=payload)
    assert resp.status_code == 200
    sess = _rows(api_db, "SELECT * FROM sessions WHERE session_id = ?", ("sess-custom-evt",))[0]
    assert sess["event_name"] == "ScrollDepth"


def test_track_meta_h_ad_id_maps_to_ad_id(client, api_db):
    payload = {"session_id": "sess-meta-ad", "detected_platform": "meta", "h_ad_id": "ad-h-1"}
    resp = client.post("/api/webhooks/track", json=payload)
    assert resp.status_code == 200
    tp = _rows(api_db, "SELECT * FROM touchpoints WHERE session_id = ?", ("sess-meta-ad",))[0]
    assert tp["platform"] == "meta"
    assert tp["ad_id"] == "ad-h-1"


def test_track_organic_h_ad_id_maps_to_ad_id(client, api_db):
    # No platform detected -> organic; h_ad_id falls through to ad_id (the else branch).
    payload = {"session_id": "sess-org-ad", "h_ad_id": "ad-h-2"}
    resp = client.post("/api/webhooks/track", json=payload)
    assert resp.status_code == 200
    tp = _rows(api_db, "SELECT * FROM touchpoints WHERE session_id = ?", ("sess-org-ad",))[0]
    assert tp["platform"] == ""
    assert tp["channel"] == "organic"
    assert tp["ad_id"] == "ad-h-2"


# ── /identify ──────────────────────────────────────────────────────────────────

def test_identify_backfills_customer_key(client, api_db):
    # First create a session/touchpoint with no customer_key via /track.
    client.post("/api/webhooks/track", json={"session_id": "sess-id-1", "visitor_id": "vis-id-1"})
    pre = _rows(api_db, "SELECT customer_key FROM sessions WHERE session_id = ?", ("sess-id-1",))[0]
    assert pre["customer_key"] == ""

    resp = client.post(
        "/api/webhooks/identify",
        json={"email": "Identify@Example.com", "visitor_id": "vis-id-1"},
    )
    assert resp.status_code == 200
    ck = _sha256("identify@example.com")
    assert resp.json() == {"ok": True, "customer_key": ck}

    sess = _rows(api_db, "SELECT customer_key FROM sessions WHERE session_id = ?", ("sess-id-1",))[0]
    assert sess["customer_key"] == ck
    tp = _rows(api_db, "SELECT customer_key FROM touchpoints WHERE session_id = ?", ("sess-id-1",))[0]
    assert tp["customer_key"] == ck


def test_identify_requires_email(client, api_db):
    resp = client.post("/api/webhooks/identify", json={"visitor_id": "v"})
    assert resp.status_code == 200
    assert resp.json() == {"ok": False, "error": "email is required"}


def test_identify_backfills_by_session_id(client, api_db):
    client.post("/api/webhooks/track", json={"session_id": "sess-id-2"})
    resp = client.post(
        "/api/webhooks/identify",
        json={"email": "by-session@example.com", "session_id": "sess-id-2"},
    )
    assert resp.status_code == 200
    ck = _sha256("by-session@example.com")
    sess = _rows(api_db, "SELECT customer_key FROM sessions WHERE session_id = ?", ("sess-id-2",))[0]
    assert sess["customer_key"] == ck


# ── /conversion ────────────────────────────────────────────────────────────────

def test_conversion_purchase_creates_order_and_conversion(client, api_db, monkeypatch):
    monkeypatch.delenv("STAPE_ENDPOINT", raising=False)
    payload = {
        "order_id": "conv-ord-1",
        "value": "75.50",
        "type": "Purchase",
        "customer_key": "Conv@Example.com",
        "session_id": "sess-conv-1",
        "utm_source": "tiktok",
    }
    resp = client.post("/api/webhooks/conversion", json=payload)
    assert resp.status_code == 200
    ck = _sha256("conv@example.com")
    assert resp.json() == {"ok": True, "order_id": "conv-ord-1", "customer_key": ck}

    o = _rows(api_db, "SELECT * FROM orders WHERE order_id = ?", ("conv-ord-1",))[0]
    assert float(o["gross"]) == 75.50
    assert o["customer_key"] == ck

    conv = _rows(api_db, "SELECT * FROM conversions WHERE order_id = ?", ("conv-ord-1",))[0]
    assert conv["type"] == "Purchase"
    assert float(conv["value"]) == 75.50

    tp = _rows(api_db, "SELECT * FROM touchpoints WHERE session_id = ?", ("sess-conv-1",))[0]
    assert tp["platform"] == "tiktok"
    assert tp["channel"] == "paid_social"


def test_conversion_lead_no_order_row(client, api_db, monkeypatch):
    monkeypatch.delenv("STAPE_ENDPOINT", raising=False)
    payload = {
        "order_id": "conv-lead-1",
        "value": "0",
        "type": "Lead",
        "session_id": "sess-lead-1",
    }
    resp = client.post("/api/webhooks/conversion", json=payload)
    assert resp.status_code == 200
    # Lead is not a purchase -> no order row, but a conversion row exists.
    assert _rows(api_db, "SELECT * FROM orders WHERE order_id = ?", ("conv-lead-1",)) == []
    conv = _rows(api_db, "SELECT * FROM conversions WHERE order_id = ?", ("conv-lead-1",))[0]
    assert conv["type"] == "Lead"


def test_conversion_generates_order_id_when_missing(client, api_db, monkeypatch):
    monkeypatch.delenv("STAPE_ENDPOINT", raising=False)
    payload = {"value": "10", "type": "Purchase", "visitor_id": "vis-gen", "session_id": "s-gen"}
    resp = client.post("/api/webhooks/conversion", json=payload)
    assert resp.status_code == 200
    order_id = resp.json()["order_id"]
    assert order_id  # non-empty generated id
    assert len(_rows(api_db, "SELECT * FROM orders WHERE order_id = ?", (order_id,))) == 1


def test_conversion_forwards_purchase_to_stape(client, api_db, monkeypatch):
    monkeypatch.setenv("STAPE_ENDPOINT", "https://stape.example.com/")
    payload = {
        "order_id": "conv-stape-1",
        "value": "20",
        "type": "Purchase",
        "visitor_id": "v-stape",
        "session_id": "s-stape",
        "utm_source": "google",
    }
    with respx.mock(assert_all_mocked=False) as router:
        route = router.get(url__startswith="https://stape.example.com").mock(
            return_value=httpx.Response(200, json={})
        )
        resp = client.post("/api/webhooks/conversion", json=payload)
        assert resp.status_code == 200
        assert resp.json()["order_id"] == "conv-stape-1"
        # The purchase should have been forwarded to Stape.
        assert route.called

    assert len(_rows(api_db, "SELECT * FROM orders WHERE order_id = ?", ("conv-stape-1",))) == 1


def test_conversion_stape_failure_does_not_break_endpoint(client, api_db, monkeypatch):
    monkeypatch.setenv("STAPE_ENDPOINT", "https://stape.example.com/")
    payload = {
        "order_id": "conv-stape-err",
        "value": "5",
        "type": "Purchase",
        "session_id": "s-stape-err",
    }
    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith="https://stape.example.com").mock(
            side_effect=httpx.ConnectError("boom")
        )
        resp = client.post("/api/webhooks/conversion", json=payload)
        # Stape errors are swallowed; endpoint still succeeds and writes the order.
        assert resp.status_code == 200
    assert len(_rows(api_db, "SELECT * FROM orders WHERE order_id = ?", ("conv-stape-err",))) == 1


def test_conversion_lookup_customer_key_from_prior_session(client, api_db, monkeypatch):
    monkeypatch.delenv("STAPE_ENDPOINT", raising=False)
    # Prior identified session for this visitor.
    client.post("/api/webhooks/track", json={"session_id": "s-prior", "visitor_id": "v-prior"})
    client.post("/api/webhooks/identify", json={"email": "prior@example.com", "visitor_id": "v-prior"})
    ck = _sha256("prior@example.com")

    # Conversion with no customer_key but same visitor -> should resolve.
    payload = {
        "order_id": "conv-lookup-1",
        "value": "30",
        "type": "Purchase",
        "visitor_id": "v-prior",
        "session_id": "s-prior",
    }
    resp = client.post("/api/webhooks/conversion", json=payload)
    assert resp.status_code == 200
    assert resp.json()["customer_key"] == ck
    o = _rows(api_db, "SELECT * FROM orders WHERE order_id = ?", ("conv-lookup-1",))[0]
    assert o["customer_key"] == ck


def test_conversion_meta_via_fbc_id_and_adset_resolution(client, api_db, monkeypatch):
    monkeypatch.delenv("STAPE_ENDPOINT", raising=False)
    # Seed adset->campaign mapping so meta campaign is resolved from adset.
    conn = sqlite3.connect(api_db)
    conn.execute(
        "INSERT INTO ad_names (platform, entity_type, entity_id, name, parent_id, source, updated_at)"
        " VALUES ('meta','adset','cv-adset','A','cv-campaign','manual','2026-01-01T00:00:00Z')"
    )
    conn.commit()
    conn.close()

    payload = {
        "order_id": "conv-meta-fbc",
        "value": "12",
        "type": "Purchase",
        "session_id": "s-meta-fbc",
        "fbc_id": "cv-adset",  # used as adset_id and forces meta platform
    }
    resp = client.post("/api/webhooks/conversion", json=payload)
    assert resp.status_code == 200
    tp = _rows(api_db, "SELECT * FROM touchpoints WHERE session_id = ?", ("s-meta-fbc",))[0]
    assert tp["platform"] == "meta"
    assert tp["channel"] == "paid_social"
    assert tp["adset_id"] == "cv-adset"
    assert tp["campaign_id"] == "cv-campaign"


def test_conversion_email_medium_channel(client, api_db, monkeypatch):
    monkeypatch.delenv("STAPE_ENDPOINT", raising=False)
    payload = {
        "order_id": "conv-email",
        "value": "0",
        "type": "Lead",
        "session_id": "s-email",
        "utm_medium": "email",
    }
    resp = client.post("/api/webhooks/conversion", json=payload)
    assert resp.status_code == 200
    tp = _rows(api_db, "SELECT * FROM touchpoints WHERE session_id = ?", ("s-email",))[0]
    assert tp["channel"] == "email"
    assert tp["platform"] == ""


def test_conversion_tiktok_h_ad_id_maps_to_campaign(client, api_db, monkeypatch):
    monkeypatch.delenv("STAPE_ENDPOINT", raising=False)
    payload = {
        "order_id": "conv-tt-h",
        "value": "0",
        "type": "Lead",
        "session_id": "s-tt-h",
        "ttc_id": "ttx",
        "h_ad_id": "tt-hh",
    }
    resp = client.post("/api/webhooks/conversion", json=payload)
    assert resp.status_code == 200
    tp = _rows(api_db, "SELECT * FROM touchpoints WHERE session_id = ?", ("s-tt-h",))[0]
    assert tp["platform"] == "tiktok"
    assert tp["campaign_id"] == "tt-hh"


def test_conversion_google_h_ad_id_maps_to_ad_id(client, api_db, monkeypatch):
    monkeypatch.delenv("STAPE_ENDPOINT", raising=False)
    payload = {
        "order_id": "conv-g-h",
        "value": "0",
        "type": "Lead",
        "session_id": "s-g-h",
        "gc_id": "gx",
        "h_ad_id": "g-hh",
    }
    resp = client.post("/api/webhooks/conversion", json=payload)
    assert resp.status_code == 200
    tp = _rows(api_db, "SELECT * FROM touchpoints WHERE session_id = ?", ("s-g-h",))[0]
    assert tp["platform"] == "google"
    assert tp["channel"] == "paid_search"
    assert tp["ad_id"] == "g-hh"


def test_conversion_lookup_customer_key_by_session_only(client, api_db, monkeypatch):
    monkeypatch.delenv("STAPE_ENDPOINT", raising=False)
    # Seed an identified session directly (customer_key set) with no visitor_id,
    # so the conversion resolves the key via the session_id branch only.
    ck = _sha256("session-only@example.com")
    conn = sqlite3.connect(api_db)
    conn.execute(
        "INSERT INTO sessions (session_id, visitor_id, ts, customer_key)"
        " VALUES ('s-sess-only', '', '2026-06-01T00:00:00Z', ?)",
        (ck,),
    )
    conn.commit()
    conn.close()

    payload = {
        "order_id": "conv-sess-only",
        "value": "5",
        "type": "Purchase",
        "session_id": "s-sess-only",
    }
    resp = client.post("/api/webhooks/conversion", json=payload)
    assert resp.status_code == 200
    assert resp.json()["customer_key"] == ck


def test_conversion_booking_type(client, api_db, monkeypatch):
    monkeypatch.delenv("STAPE_ENDPOINT", raising=False)
    payload = {
        "order_id": "conv-booking",
        "value": "0",
        "type": "AppointmentBooked",
        "session_id": "s-booking",
    }
    resp = client.post("/api/webhooks/conversion", json=payload)
    assert resp.status_code == 200
    conv = _rows(api_db, "SELECT * FROM conversions WHERE order_id = ?", ("conv-booking",))[0]
    assert conv["type"] == "AppointmentBooked"
    # Not a purchase -> no order row.
    assert _rows(api_db, "SELECT * FROM orders WHERE order_id = ?", ("conv-booking",)) == []


def test_module_db_helper_reads_env(api_db):
    # _db() should reflect the per-test ATTRIBUTIONOPS_DB_PATH set by the api_db fixture.
    assert webhooks._db() == api_db
