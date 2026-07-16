"""Tests for the GoHighLevel API sync endpoints (/api/ghl).

Covers credential storage, connect/status/sync endpoints (with the GHL
LeadConnector API mocked via respx), and the field-mapping/writer helpers that
turn GHL contacts + opportunities into warehouse conversions/orders. Nothing
hits the real network.
"""

from __future__ import annotations

import sqlite3

import respx
from httpx import Response

from attributionops.db import sql_rows

import api.ghl_sync as gs

GHL = "https://services.leadconnectorhq.com"


# ── helpers ────────────────────────────────────────────────────────────────────

def _contact(cid, email, *, date_added="2026-06-15T12:00:00.000Z", utm_source="facebook",
             utm_medium="paid_social", fbclid="", gclid="", phone=""):
    return {
        "id": cid,
        "email": email,
        "phone": phone,
        "firstName": "Jane",
        "lastName": "Doe",
        "dateAdded": date_added,
        "source": utm_source,
        "attributionSource": {
            "utmSource": utm_source,
            "utmMedium": utm_medium,
            "campaign": "spring_promo",
            "url": "https://funnel.example/optin",
            "fbclid": fbclid,
            "gclid": gclid,
        },
    }


def _opportunity(oid, email, *, value=199.0, status="won", created="2026-06-16T09:00:00.000Z"):
    return {
        "id": oid,
        "name": "Deal",
        "monetaryValue": value,
        "status": status,
        "createdAt": created,
        "contact": {"email": email},
    }


def _submission(sid, contact_id, email, *, created="2026-06-15T12:30:00.000Z"):
    return {
        "id": sid,
        "contactId": contact_id,
        "email": email,
        "name": "Demo form",
        "createdAt": created,
        "others": {
            "eventData": {
                "source": "facebook",
                "medium": "form",
                "page": {"url": "https://funnel.example/optin"},
                "fbclid": "fb-click",
            }
        },
    }


# ── credential storage ──────────────────────────────────────────────────────────

def test_save_and_get_credentials_roundtrip(api_db):
    gs.save_ghl_credentials(api_db, "pit-abc", "loc_1")
    token, location = gs.get_ghl_credentials(api_db)
    assert token == "pit-abc"
    assert location == "loc_1"


def test_get_credentials_env_fallback(api_db, monkeypatch):
    monkeypatch.setenv("GHL_API_TOKEN", "env-token")
    monkeypatch.setenv("GHL_LOCATION_ID", "env-loc")
    token, location = gs.get_ghl_credentials(api_db)
    assert token == "env-token"
    assert location == "env-loc"


# ── mapping helpers ─────────────────────────────────────────────────────────────

def test_within_range(monkeypatch):
    assert gs._within("2026-06-15T00:00:00Z", "2026-06-01", "2026-06-30")
    assert not gs._within("2026-05-31T00:00:00Z", "2026-06-01", "2026-06-30")
    assert not gs._within("2026-07-01T00:00:00Z", "2026-06-01", "2026-06-30")
    # Reporting uses Hyros's fixed UTC-06 day boundary, not the UTC date.
    monkeypatch.setenv("REPORT_TIMEZONE", "Etc/GMT+6")
    assert gs._within("2026-07-12T04:30:00Z", "2026-07-11", "2026-07-11")
    assert not gs._within("2026-07-12T06:30:00Z", "2026-07-11", "2026-07-11")


def test_src_info_and_attribution_flags():
    src = gs._src_info_from_attribution(_contact("c1", "a@x.com", fbclid="fb123"))
    assert src["utm_source"] == "facebook"
    assert src["fbclid"] == "fb123"
    assert gs._has_attribution(src) is True
    assert gs._has_attribution({"utm_source": "", "gclid": "", "fbclid": "", "ttclid": ""}) is False


def test_src_infos_read_real_ghl_attribution_history():
    contact = {
        "id": "c-history",
        "email": "history@example.com",
        "source": "Website",
        "attributions": [
            {
                "isFirst": True,
                "utmSource": "google",
                "utmGclid": "google-click-1",
                "pageUrl": "https://funnel.example/landing",
            },
            {
                "isLast": True,
                "utmSource": "facebook",
                "utmFbclid": "meta-click-1",
                "fbc": "fb.1.123.meta-click-1",
                "pageUrl": "https://funnel.example/checkout",
            },
        ],
    }

    infos = gs._src_infos_from_attribution(contact)

    assert len(infos) == 2
    assert infos[0]["gclid"] == "google-click-1"
    assert infos[0]["url"] == "https://funnel.example/landing"
    assert infos[1]["fbclid"] == "meta-click-1"
    assert infos[1]["fbc_id"] == "fb.1.123.meta-click-1"
    assert gs._src_info_from_attribution(contact) == infos[1]


def test_contact_ts_normalises_iso():
    ts = gs._contact_ts({"dateAdded": "2026-06-15T12:00:00.000Z"}, "fallback")
    assert ts == "2026-06-15T12:00:00Z"
    assert gs._contact_ts({}, "fallback") == "fallback"


# ── writers (no network) ────────────────────────────────────────────────────────

def test_write_contacts_creates_leads_and_touchpoints(api_db):
    contacts = [
        _contact("c1", "lead1@example.com", fbclid="fb1"),
        _contact("c2", "lead2@example.com", utm_source="", utm_medium=""),  # no attribution
        _contact("c3", "", ),  # no email -> skipped
        _contact("c4", "old@example.com", date_added="2026-01-01T00:00:00Z"),  # out of range
    ]
    stats = gs._write_contacts(api_db, contacts, "2026-06-01", "2026-06-30")
    assert stats["leads"] == 2
    assert stats["skipped"] == 2

    leads = sql_rows(api_db, "SELECT * FROM conversions WHERE LOWER(type) = 'lead'")
    assert len(leads) == 2
    # customer_key is sha256(email) so it stitches to Stripe/pixel records.
    assert {r["customer_key"] for r in leads} == {gs._sha256("lead1@example.com"), gs._sha256("lead2@example.com")}

    # Only the attributed contact gets a touchpoint.
    touches = sql_rows(api_db, "SELECT * FROM touchpoints")
    assert len(touches) == 1
    assert touches[0]["customer_key"] == gs._sha256("lead1@example.com")


def test_write_contacts_replaces_imported_attribution_history(api_db):
    contact = {
        "id": "c-history",
        "email": "history@example.com",
        "dateAdded": "2026-06-15T12:00:00Z",
        "source": "Website",
        "attributions": [
            {"isFirst": True, "utmSource": "google", "utmGclid": "g-1"},
            {"isLast": True, "utmSource": "facebook", "utmFbclid": "f-1"},
        ],
    }

    gs._write_contacts(api_db, [contact], "2026-06-01", "2026-06-30")
    gs._write_contacts(api_db, [contact], "2026-06-01", "2026-06-30")

    touches = sql_rows(api_db, "SELECT * FROM touchpoints ORDER BY ts")
    assert len(touches) == 2
    assert touches[0]["platform"] == "google"
    assert touches[0]["gclid"] == "g-1"
    assert touches[1]["platform"] == "meta"
    assert touches[1]["fbclid"] == "f-1"


def test_write_contacts_lead_key_is_per_local_day(api_db, monkeypatch):
    # Reporting uses Hyros's fixed UTC-06 day boundary; 12:00Z is 06:00 local.
    monkeypatch.setenv("REPORT_TIMEZONE", "Etc/GMT+6")
    first = _contact("c1", "re@example.com", date_added="2026-06-15T12:00:00Z")
    gs._write_contacts(api_db, [first], "2026-06-01", "2026-06-30")
    # Re-running the same sync must not duplicate the same-day lead.
    gs._write_contacts(api_db, [first], "2026-06-01", "2026-06-30")
    # A later-day re-engagement is a NEW lead event for the same contact.
    later = _contact("c1", "re@example.com", date_added="2026-06-20T12:00:00Z")
    gs._write_contacts(api_db, [later], "2026-06-01", "2026-06-30")

    rows = sql_rows(
        api_db,
        "SELECT conversion_id FROM conversions WHERE LOWER(type)='lead' ORDER BY ts",
    )
    assert [r["conversion_id"] for r in rows] == [
        gs._sha256("ghl_lead|c1|2026-06-15"),
        gs._sha256("ghl_lead|c1|2026-06-20"),
    ]


def test_write_contacts_migrates_legacy_lead_key_in_place(api_db, monkeypatch):
    monkeypatch.setenv("REPORT_TIMEZONE", "Etc/GMT+6")
    legacy_id = gs._sha256("ghl_lead|c1")
    with sqlite3.connect(api_db) as conn:
        conn.execute(
            "INSERT INTO conversions (conversion_id, ts, type, value, order_id, customer_key)"
            " VALUES (?,?,?,?,?,?)",
            (legacy_id, "2026-06-15T12:00:00Z", "Lead", "0", legacy_id,
             gs._sha256("re@example.com")),
        )
        conn.commit()

    contact = _contact("c1", "re@example.com", date_added="2026-06-15T12:00:00Z")
    stats = gs._write_contacts(api_db, [contact], "2026-06-01", "2026-06-30")
    assert stats["leads"] == 1

    rows = sql_rows(
        api_db,
        "SELECT conversion_id, order_id FROM conversions WHERE LOWER(type)='lead'",
    )
    # The legacy row is rekeyed (from its OWN ts), not duplicated.
    assert rows == [{
        "conversion_id": gs._sha256("ghl_lead|c1|2026-06-15"),
        "order_id": gs._sha256("ghl_lead|c1|2026-06-15"),
    }]


def test_migrate_legacy_lead_key_deletes_duplicate_when_dated_exists(api_db, monkeypatch):
    monkeypatch.setenv("REPORT_TIMEZONE", "Etc/GMT+6")
    legacy_id = gs._sha256("ghl_lead|c1")
    dated_id = gs._sha256("ghl_lead|c1|2026-06-15")
    with sqlite3.connect(api_db) as conn:
        for conv_id in (legacy_id, dated_id):
            conn.execute(
                "INSERT INTO conversions (conversion_id, ts, type, value, order_id, customer_key)"
                " VALUES (?,?,?,?,?,?)",
                (conv_id, "2026-06-15T12:00:00Z", "Lead", "0", conv_id,
                 gs._sha256("re@example.com")),
            )
        conn.commit()

    gs._migrate_legacy_lead_conversion(api_db, "c1")

    rows = sql_rows(api_db, "SELECT conversion_id FROM conversions WHERE LOWER(type)='lead'")
    assert [r["conversion_id"] for r in rows] == [dated_id]


def test_write_contacts_phone_only_uses_normalized_phone_key(api_db):
    contacts = [
        _contact("c-phone", "", phone="+1 (514) 555-0134"),
        _contact("c-none", ""),  # neither email nor phone -> skipped
    ]
    stats = gs._write_contacts(api_db, contacts, "2026-06-01", "2026-06-30")
    assert stats["leads"] == 1
    assert stats["skipped"] == 1

    rows = sql_rows(api_db, "SELECT customer_key FROM conversions WHERE LOWER(type)='lead'")
    assert [r["customer_key"] for r in rows] == [gs._sha256("phone:5145550134")]
    # Formatting/country-code variants of the same phone hash identically.
    assert gs.normalized_phone_key("514-555-0134") == gs._sha256("phone:5145550134")
    assert gs.normalized_phone_key("15145550134") == gs._sha256("phone:5145550134")
    assert gs.normalized_phone_key("") == ""


def test_write_opportunities_won_creates_order(api_db):
    opps = [
        _opportunity("o1", "buyer@example.com", value=199.0, status="won"),
        _opportunity("o2", "browser@example.com", value=0.0, status="open"),
    ]
    stats = gs._write_opportunities(api_db, opps, "2026-06-01", "2026-06-30")
    assert stats["orders"] == 1
    assert stats["open_opportunities"] == 1

    orders = sql_rows(api_db, "SELECT * FROM orders")
    assert len(orders) == 1
    assert float(orders[0]["gross"]) == 199.0
    assert orders[0]["customer_key"] == gs._sha256("buyer@example.com")


def test_write_form_submissions_preserves_repeat_optins(api_db):
    contact = _contact("c1", "lead@example.com")
    submissions = [
        _submission("s1", "c1", "lead@example.com"),
        _submission("s2", "c1", "lead@example.com", created="2026-06-16T12:30:00.000Z"),
    ]

    stats = gs._write_form_submissions(
        api_db, submissions, "2026-06-01", "2026-06-30", {"c1": contact}
    )

    assert stats == {"optins": 2, "skipped": 0}
    rows = sql_rows(api_db, "SELECT type, customer_key FROM conversions ORDER BY ts")
    assert [row["type"] for row in rows] == ["OptIn", "OptIn"]
    assert {row["customer_key"] for row in rows} == {gs._sha256("lead@example.com")}


# ── endpoints ───────────────────────────────────────────────────────────────────

def test_connect_validates_and_saves(client, api_db):
    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith=f"{GHL}/locations/").mock(return_value=Response(200, json={"location": {"id": "loc_1"}}))
        r = client.post("/api/ghl/connect", json={"api_token": "pit-xyz", "location_id": "loc_1"})
    assert r.status_code == 200
    body = r.json()
    assert body["connected"] is True
    token, location = gs.get_ghl_credentials(api_db)
    assert token == "pit-xyz" and location == "loc_1"


def test_connect_rejects_invalid_token(client, api_db):
    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith=f"{GHL}/locations/").mock(return_value=Response(401, json={"message": "unauthorized"}))
        r = client.post("/api/ghl/connect", json={"api_token": "bad", "location_id": "loc_1"})
    body = r.json()
    assert body["connected"] is False
    assert "invalid" in body["error"].lower() or "missing scope" in body["error"].lower()


def test_connect_requires_both_fields(client, api_db):
    r = client.post("/api/ghl/connect", json={"api_token": "pit-xyz"})
    body = r.json()
    assert body["connected"] is False
    assert "required" in body["error"].lower()


def test_status_not_configured(client, api_db, monkeypatch):
    monkeypatch.delenv("GHL_API_TOKEN", raising=False)
    monkeypatch.delenv("GHL_LOCATION_ID", raising=False)
    r = client.get("/api/ghl/status")
    body = r.json()
    assert body["connected"] is False
    assert body["configured"] is False


def test_status_configured(client, api_db):
    gs.save_ghl_credentials(api_db, "pit-xyz", "loc_1")
    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith=f"{GHL}/locations/").mock(return_value=Response(200, json={"location": {"id": "loc_1"}}))
        r = client.get("/api/ghl/status")
    body = r.json()
    assert body["configured"] is True
    assert body["connected"] is True
    assert body["location_id"] == "loc_1"


def test_sync_writes_leads_and_orders(client, api_db):
    gs.save_ghl_credentials(api_db, "pit-xyz", "loc_1")

    contacts_page = {"contacts": [
        _contact("c1", "lead1@example.com", fbclid="fb1"),
        _contact("c2", "lead2@example.com"),
    ], "meta": {}}
    opps_page = {"opportunities": [
        _opportunity("o1", "buyer@example.com", value=250.0, status="won"),
    ]}
    forms_page = {
        "submissions": [_submission("s1", "c1", "lead1@example.com")],
        "meta": {"currentPage": 1, "nextPage": None},
    }

    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith=f"{GHL}/contacts/").mock(return_value=Response(200, json=contacts_page))
        router.get(url__startswith=f"{GHL}/forms/submissions").mock(return_value=Response(200, json=forms_page))
        router.get(url__startswith=f"{GHL}/opportunities/search").mock(return_value=Response(200, json=opps_page))
        r = client.post("/api/ghl/sync", params={"start_date": "2026-06-01", "end_date": "2026-06-30"})

    body = r.json()
    assert body["leads"] == 2
    assert body["optins"] == 1
    assert body["orders"] == 1
    assert body["errors"] == []

    leads = sql_rows(api_db, "SELECT COUNT(*) AS c FROM conversions WHERE LOWER(type)='lead'")
    assert int(leads[0]["c"]) == 2
    orders = sql_rows(api_db, "SELECT COUNT(*) AS c FROM orders")
    assert int(orders[0]["c"]) == 1


def test_sync_scans_recent_pages_until_historical_window(client, api_db):
    gs.save_ghl_credentials(api_db, "pit-xyz", "loc_1")
    first_url = f"{GHL}/contacts/"
    second_url = f"{GHL}/contacts/?startAfter=page2"
    third_url = f"{GHL}/contacts/?startAfter=page3"

    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith=first_url).mock(
            side_effect=[
                Response(200, json={
                    "contacts": [_contact("new", "new@example.com", date_added="2026-07-10T00:00:00Z")],
                    "meta": {"nextPageUrl": second_url},
                }),
                Response(200, json={
                    "contacts": [_contact("match", "match@example.com", date_added="2026-06-20T00:00:00Z")],
                    "meta": {"nextPageUrl": third_url},
                }),
                Response(200, json={
                    "contacts": [_contact("old", "old@example.com", date_added="2026-05-01T00:00:00Z")],
                    "meta": {"nextPageUrl": f"{GHL}/contacts/?startAfter=page4"},
                }),
            ]
        )
        router.get(url__startswith=f"{GHL}/opportunities/search").mock(
            return_value=Response(200, json={"opportunities": []})
        )
        router.get(url__startswith=f"{GHL}/forms/submissions").mock(
            return_value=Response(200, json={"submissions": [], "meta": {"nextPage": None}})
        )
        response = client.post(
            "/api/ghl/sync",
            params={"start_date": "2026-06-19", "end_date": "2026-06-25", "limit": 100},
        )

    body = response.json()
    assert body["errors"] == []
    assert body["leads"] == 1
    leads = sql_rows(api_db, "SELECT customer_key FROM conversions WHERE LOWER(type)='lead'")
    assert [row["customer_key"] for row in leads] == [gs._sha256("match@example.com")]


def test_form_submission_fetch_follows_all_pages(client, api_db):
    gs.save_ghl_credentials(api_db, "pit-xyz", "loc_1")
    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith=f"{GHL}/contacts/").mock(
            return_value=Response(200, json={"contacts": [], "meta": {}})
        )
        router.get(url__startswith=f"{GHL}/forms/submissions").mock(
            side_effect=[
                Response(200, json={
                    "submissions": [_submission("s1", "c1", "one@example.com")],
                    "meta": {"currentPage": 1, "nextPage": 2},
                }),
                Response(200, json={
                    "submissions": [_submission("s2", "c2", "two@example.com")],
                    "meta": {"currentPage": 2, "nextPage": None},
                }),
            ]
        )
        router.get(url__startswith=f"{GHL}/opportunities/search").mock(
            return_value=Response(200, json={"opportunities": []})
        )
        response = client.post(
            "/api/ghl/sync",
            params={"start_date": "2026-06-01", "end_date": "2026-06-30", "limit": 100},
        )

    body = response.json()
    assert body["errors"] == []
    assert body["optins"] == 2
    assert body["synced"] == 2


def test_form_submission_fetch_filters_before_applying_match_limit(client, api_db):
    gs.save_ghl_credentials(api_db, "pit-xyz", "loc_1")
    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith=f"{GHL}/contacts/").mock(
            return_value=Response(200, json={"contacts": [], "meta": {}})
        )
        forms_route = router.get(url__startswith=f"{GHL}/forms/submissions").mock(
            side_effect=[
                Response(200, json={
                    "submissions": [
                        _submission("new-1", "c-new-1", "new1@example.com", created="2026-07-10T12:00:00Z"),
                        _submission("new-2", "c-new-2", "new2@example.com", created="2026-07-09T12:00:00Z"),
                    ],
                    "meta": {"currentPage": 1, "nextPage": 2},
                }),
                Response(200, json={
                    "submissions": [
                        _submission("match", "c-match", "match@example.com", created="2026-06-15T12:00:00Z"),
                    ],
                    "meta": {"currentPage": 2, "nextPage": 3},
                }),
                Response(200, json={
                    "submissions": [
                        _submission("old", "c-old", "old@example.com", created="2026-05-01T12:00:00Z"),
                    ],
                    "meta": {"currentPage": 3, "nextPage": 4},
                }),
            ]
        )
        router.get(url__startswith=f"{GHL}/opportunities/search").mock(
            return_value=Response(200, json={"opportunities": []})
        )
        response = client.post(
            "/api/ghl/sync",
            params={"start_date": "2026-06-01", "end_date": "2026-06-30", "limit": 1},
        )

    body = response.json()
    assert body["errors"] == []
    assert body["optins"] == 1
    assert body["synced"] == 1
    assert forms_route.call_count == 2
    rows = sql_rows(api_db, "SELECT conversion_id FROM conversions WHERE LOWER(type)='optin'")
    assert rows == [{"conversion_id": gs._sha256("ghl_submission|match")}]


def test_sync_not_configured_returns_error(client, api_db, monkeypatch):
    monkeypatch.delenv("GHL_API_TOKEN", raising=False)
    monkeypatch.delenv("GHL_LOCATION_ID", raising=False)
    r = client.post("/api/ghl/sync")
    body = r.json()
    assert body["synced"] == 0
    assert "error" in body
