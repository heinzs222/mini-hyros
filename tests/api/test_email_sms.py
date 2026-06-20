"""Tests for the Email/SMS sequence attribution API (prefix /api/email-sms).

Covers:
  POST /api/email-sms/track       — record an email/SMS touchpoint
  GET  /api/email-sms/attribution — attribution by sequence (+ revenue enrichment)
  GET  /api/email-sms/summary     — per-channel performance summary

No outbound HTTP is made by this module, so respx is not required. All side
effects land in the temp SQLite warehouse provided by the ``api_db`` fixture.
"""

from __future__ import annotations

import sqlite3

import api.email_sms as email_sms
from tests.helpers import insert_rows, order


def _events(db_path: str):
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(r) for r in conn.execute("SELECT * FROM email_sms_events")]


def _count(db_path: str, table: str) -> int:
    with sqlite3.connect(db_path) as conn:
        try:
            return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        except sqlite3.OperationalError:
            # Table not created yet (e.g. request rejected before _ensure_*).
            return 0


# ── POST /track ────────────────────────────────────────────────────────────────


def test_track_click_inserts_event_touchpoint_and_session(client, api_db):
    r = client.post(
        "/api/email-sms/track",
        json={
            "email": "Buyer@Example.com",
            "channel": "email",
            "event_type": "clicked",
            "sequence_name": "Welcome Sequence",
            "step_number": 3,
            "subject_line": "Your offer expires",
            "link_url": "https://shop.example.com/promo",
            "source": "klaviyo",
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert isinstance(body["event_id"], str) and body["event_id"]

    # An email-driven event hashes the (lowercased) email into customer_key.
    expected_ck = email_sms._sha256("buyer@example.com")
    events = _events(api_db)
    assert len(events) == 1
    ev = events[0]
    assert ev["customer_key"] == expected_ck
    assert ev["channel"] == "email"
    assert ev["event_type"] == "clicked"
    assert ev["sequence_name"] == "Welcome Sequence"
    assert ev["step_number"] == "3"
    assert ev["source"] == "klaviyo"

    # A click also fans out into touchpoints + sessions for attribution.
    assert _count(api_db, "touchpoints") == 1
    assert _count(api_db, "sessions") == 1
    with sqlite3.connect(api_db) as conn:
        conn.row_factory = sqlite3.Row
        tp = dict(conn.execute("SELECT * FROM touchpoints").fetchone())
        sess = dict(conn.execute("SELECT * FROM sessions").fetchone())
    assert tp["customer_key"] == expected_ck
    assert tp["campaign_id"] == "Welcome Sequence"
    assert tp["channel"] == "email"
    assert sess["customer_key"] == expected_ck
    assert sess["utm_source"] == "klaviyo"
    assert sess["landing_page"] == "https://shop.example.com/promo"


def test_track_non_click_does_not_create_touchpoint(client, api_db):
    r = client.post(
        "/api/email-sms/track",
        json={
            "email": "open@example.com",
            "channel": "email",
            "event_type": "opened",
            "sequence_name": "Promo",
        },
    )
    assert r.status_code == 200
    assert r.json()["ok"] is True
    assert _count(api_db, "email_sms_events") == 1
    # "opened" is not a click, so no touchpoint/session side effects.
    assert _count(api_db, "touchpoints") == 0
    assert _count(api_db, "sessions") == 0


def test_track_with_customer_key_only(client, api_db):
    r = client.post(
        "/api/email-sms/track",
        json={
            "customer_key": "ck_direct",
            "channel": "sms",
            "event_type": "sent",
            "sequence_name": "Cart Recovery",
        },
    )
    assert r.status_code == 200
    assert r.json()["ok"] is True
    events = _events(api_db)
    assert len(events) == 1
    assert events[0]["customer_key"] == "ck_direct"
    assert events[0]["channel"] == "sms"


def test_track_requires_email_or_customer_key(client, api_db):
    r = client.post(
        "/api/email-sms/track",
        json={"channel": "sms", "event_type": "sent", "sequence_name": "Promo"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is False
    assert body["error"] == "email or customer_key required"
    # Nothing persisted.
    assert _count(api_db, "email_sms_events") == 0


def test_track_invalid_email_falls_back_to_customer_key(client, api_db):
    # An email without "@" is not treated as an email; customer_key fills in.
    r = client.post(
        "/api/email-sms/track",
        json={
            "email": "not-an-email",
            "customer_key": "fallback_ck",
            "event_type": "clicked",
            "sequence_name": "Promo",
        },
    )
    assert r.status_code == 200
    assert r.json()["ok"] is True
    events = _events(api_db)
    assert len(events) == 1
    assert events[0]["customer_key"] == "fallback_ck"


def test_track_defaults_channel_and_event_type(client, api_db):
    r = client.post(
        "/api/email-sms/track",
        json={"customer_key": "ck_defaults", "sequence_name": "Defaults"},
    )
    assert r.status_code == 200
    assert r.json()["ok"] is True
    ev = _events(api_db)[0]
    # Defaults: channel -> "email", event_type -> "clicked".
    assert ev["channel"] == "email"
    assert ev["event_type"] == "clicked"
    # A defaulted "clicked" still fans out into touchpoints/sessions.
    assert _count(api_db, "touchpoints") == 1
    assert _count(api_db, "sessions") == 1


# ── GET /attribution ───────────────────────────────────────────────────────────


def test_attribution_empty_returns_no_rows(client, api_db):
    r = client.get("/api/email-sms/attribution")
    assert r.status_code == 200
    assert r.json() == {"rows": []}


def test_attribution_aggregates_and_enriches_with_revenue(client, api_db):
    email = "vip@example.com"
    ck = email_sms._sha256(email)

    # Send -> open -> click for one sequence/contact.
    for et in ("sent", "opened", "clicked"):
        client.post(
            "/api/email-sms/track",
            json={
                "email": email,
                "channel": "email",
                "event_type": et,
                "sequence_name": "Welcome",
            },
        )

    # That contact later purchased — revenue must be attributed to the sequence.
    insert_rows(api_db, "orders", [order("o1", "2026-02-01T00:00:00Z", ck, gross="150.00")])

    r = client.get("/api/email-sms/attribution")
    assert r.status_code == 200
    rows = r.json()["rows"]
    assert len(rows) == 1
    row = rows[0]
    assert row["sequence_name"] == "Welcome"
    assert row["channel"] == "email"
    assert row["sent"] == 1
    assert row["opened"] == 1
    assert row["clicked"] == 1
    assert row["unique_contacts"] == 1
    assert row["attributed_orders"] == 1
    assert row["attributed_revenue"] == 150.0
    # Rates are percentages relative to "sent".
    assert row["open_rate"] == 100.0
    assert row["click_rate"] == 100.0


def test_attribution_channel_filter(client, api_db):
    client.post(
        "/api/email-sms/track",
        json={"customer_key": "ck_e", "channel": "email", "event_type": "clicked", "sequence_name": "EmailSeq"},
    )
    client.post(
        "/api/email-sms/track",
        json={"customer_key": "ck_s", "channel": "sms", "event_type": "clicked", "sequence_name": "SmsSeq"},
    )

    r = client.get("/api/email-sms/attribution", params={"channel": "sms"})
    assert r.status_code == 200
    rows = r.json()["rows"]
    assert len(rows) == 1
    assert rows[0]["channel"] == "sms"
    assert rows[0]["sequence_name"] == "SmsSeq"


def test_attribution_no_revenue_when_no_orders(client, api_db):
    client.post(
        "/api/email-sms/track",
        json={"email": "noorder@example.com", "event_type": "clicked", "sequence_name": "Orphan"},
    )
    rows = client.get("/api/email-sms/attribution").json()["rows"]
    assert len(rows) == 1
    assert rows[0]["attributed_orders"] == 0
    assert rows[0]["attributed_revenue"] == 0


def test_attribution_sequence_without_clicks_has_zero_revenue(client, api_db):
    # A sequence with only sent/opened events (no clickers) -> empty clicker set,
    # exercising the no-clickers branch of the revenue enrichment.
    client.post(
        "/api/email-sms/track",
        json={"email": "lurker@example.com", "event_type": "sent", "sequence_name": "NoClicks"},
    )
    client.post(
        "/api/email-sms/track",
        json={"email": "lurker@example.com", "event_type": "opened", "sequence_name": "NoClicks"},
    )
    rows = client.get("/api/email-sms/attribution").json()["rows"]
    assert len(rows) == 1
    assert rows[0]["sequence_name"] == "NoClicks"
    assert rows[0]["clicked"] == 0
    assert rows[0]["attributed_orders"] == 0
    assert rows[0]["attributed_revenue"] == 0


# ── GET /summary ───────────────────────────────────────────────────────────────


def test_summary_empty_returns_no_channels(client, api_db):
    r = client.get("/api/email-sms/summary")
    assert r.status_code == 200
    assert r.json() == {"channels": []}


def test_summary_groups_by_channel(client, api_db):
    # Two email events (sent + clicked) for one contact, one SMS click for another.
    client.post(
        "/api/email-sms/track",
        json={"customer_key": "ck1", "channel": "email", "event_type": "sent", "sequence_name": "S1"},
    )
    client.post(
        "/api/email-sms/track",
        json={"customer_key": "ck1", "channel": "email", "event_type": "clicked", "sequence_name": "S1"},
    )
    client.post(
        "/api/email-sms/track",
        json={"customer_key": "ck2", "channel": "sms", "event_type": "clicked", "sequence_name": "S2"},
    )

    r = client.get("/api/email-sms/summary")
    assert r.status_code == 200
    channels = {c["channel"]: c for c in r.json()["channels"]}
    assert set(channels) == {"email", "sms"}

    em = channels["email"]
    assert em["total_events"] == 2
    assert em["unique_contacts"] == 1
    assert em["sequences"] == 1
    assert em["sent"] == 1
    assert em["clicked"] == 1

    sms = channels["sms"]
    assert sms["total_events"] == 1
    assert sms["unique_contacts"] == 1
    assert sms["clicked"] == 1
