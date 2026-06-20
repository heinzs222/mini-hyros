"""Tests for the remaining ``backend/main.py`` endpoints not covered elsewhere.

Already covered in other test modules (NOT duplicated here): GET /api/health,
GET / (service banner), GET /api/report, GET /api/report/children, and the auth
middleware token enforcement.

This module focuses on the leftover endpoints defined directly in ``main.py``:

  * the root TikTok-OAuth redirect branch (``/`` with ``auth_code``)
  * the analytics passthrough GETs: /api/spend, /api/attribution, /api/platforms,
    /api/integrations, /api/tracking, /api/forecasting, /api/reported
  * the tracking-pixel script serving routes (/t/hyros.js, /v1/lst/universal-script,
    /t/hyros-ghl.js) — public, JS content-type
  * the HTML setup pages (/t/setup, /t/ghl-setup) — public, render env-driven host/token
  * the /ws websocket
  * public-path behaviour of the tracking routes when auth is enabled
"""

from __future__ import annotations

import respx

from tests.helpers import insert_rows, order, spend, touchpoint


# ── seeding ────────────────────────────────────────────────────────────────────
def _seed(db: str) -> None:
    insert_rows(db, "spend", [spend(date="2026-01-14", platform="meta", account_id="acc_meta",
                                    campaign_id="cmp1", adset_id="set1", ad_id="ad1",
                                    clicks=100, cost=200, impressions=5000)])
    insert_rows(db, "touchpoints", [touchpoint("2026-01-14T09:00:00Z", "c1", platform="meta",
                                              campaign_id="cmp1", adset_id="set1", ad_id="ad1")])
    insert_rows(db, "sessions", [{"session_id": "sess1", "ts": "2026-01-14T08:00:00Z",
                                  "customer_key": "c1", "landing_page": "/", "fbclid": "fb123"}])
    insert_rows(db, "orders", [order("o1", "2026-01-15T12:00:00Z", "c1", gross=1000, net=800,
                                     cogs=100, fees=50)])
    insert_rows(db, "conversions", [{"conversion_id": "k1", "ts": "2026-01-15T12:00:00Z",
                                     "type": "Purchase", "value": "800", "customer_key": "c1"}])
    insert_rows(db, "reported_value", [{
        "platform": "meta", "date": "2026-01-14", "account_id": "acc_meta",
        "campaign_id": "cmp1", "adset_id": "set1", "ad_id": "ad1",
        "conversion_type": "Purchase", "reported_value": "800",
    }])


DATES = {"start_date": "2026-01-01", "end_date": "2026-01-31"}


# ── root OAuth redirect branch ──────────────────────────────────────────────────
def test_root_with_auth_code_redirects_to_platform_auth(client):
    # auth_code present -> 307 redirect into the TikTok platform-auth callback,
    # forwarding both auth_code and state. Do not follow (callback hits TikTok).
    r = client.get("/", params={"auth_code": "abc123", "state": "xyz"}, follow_redirects=False)
    assert r.status_code == 307
    loc = r.headers["location"]
    assert loc.startswith("/api/platform-auth/tiktok/callback")
    assert "auth_code=abc123" in loc
    assert "state=xyz" in loc


def test_root_without_auth_code_is_banner(client):
    # Sanity guard for the non-redirect branch boundary.
    r = client.get("/", follow_redirects=False)
    assert r.status_code == 200
    assert r.json() == {"status": "ok", "service": "mini-hyros"}


# ── /api/health HEAD method ─────────────────────────────────────────────────────
def test_health_head(client, api_db):
    r = client.head("/api/health")
    assert r.status_code == 200


# ── analytics passthrough GET endpoints ─────────────────────────────────────────
def test_spend_endpoint(client, api_db):
    _seed(api_db)
    r = client.get("/api/spend", params={**DATES, "platform": "all", "breakdown": "campaign"})
    assert r.status_code == 200
    body = r.json()
    assert body["breakdown"] == "campaign"
    assert isinstance(body["rows"], list)
    # The seeded meta campaign spend should surface.
    assert any(str(row.get("cost")) not in ("", "0", "0.0") for row in body["rows"])


def test_attribution_endpoint(client, api_db):
    _seed(api_db)
    r = client.get("/api/attribution", params={**DATES, "model": "last_click"})
    assert r.status_code == 200
    body = r.json()
    assert "rows" in body
    assert isinstance(body["rows"], list)


def test_platforms_endpoint_lists_seeded_platform(client, api_db):
    _seed(api_db)
    r = client.get("/api/platforms")
    assert r.status_code == 200
    assert "meta" in r.json()["platforms"]


def test_platforms_endpoint_empty_db(client, api_db):
    r = client.get("/api/platforms")
    assert r.status_code == 200
    assert r.json() == {"platforms": []}


def test_integrations_endpoint_reports_local_warehouse(client, api_db):
    _seed(api_db)
    r = client.get("/api/integrations")
    assert r.status_code == 200
    body = r.json()
    assert body["connected"] is True
    assert body["mode"] == "local_warehouse"
    # Schema tables are reflected back.
    assert "sessions" in body["warehouse"]["tables"]
    # Freshness reflects the seeded session timestamp.
    assert body["tracking"]["sessions_last_ts"] == "2026-01-14T08:00:00Z"


def test_tracking_health_endpoint(client, api_db):
    _seed(api_db)
    r = client.get("/api/tracking")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] in ("ok", "warn")
    assert "coverage" in body
    assert "freshness" in body


def test_forecasting_endpoint(client, api_db):
    _seed(api_db)
    r = client.get("/api/forecasting", params={**DATES, "cohort_window": 30, "method": "simple"})
    assert r.status_code == 200
    body = r.json()
    assert body["method"] == "simple"
    assert body["cohort_window"] == 30
    assert "rows" in body


def test_reported_endpoint(client, api_db):
    _seed(api_db)
    r = client.get("/api/reported", params={**DATES, "platform": "all", "breakdown": "campaign",
                                            "conversion_type": "Purchase"})
    assert r.status_code == 200
    body = r.json()
    assert body["breakdown"] == "campaign"
    assert isinstance(body["rows"], list)


# ── tracking-pixel script serving ───────────────────────────────────────────────
def test_serve_hyros_js(client):
    r = client.get("/t/hyros.js")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/javascript")
    assert "public" in r.headers.get("cache-control", "")
    assert "Mini Hyros Tracking Pixel" in r.text


def test_universal_script_alias_serves_same_js(client):
    r = client.get("/v1/lst/universal-script")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/javascript")
    assert "Mini Hyros Tracking Pixel" in r.text


def test_serve_ghl_js(client):
    r = client.get("/t/hyros-ghl.js")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/javascript")
    assert "GoHighLevel" in r.text


# ── HTML setup pages ────────────────────────────────────────────────────────────
def test_setup_page_renders_env_host_and_token(client, monkeypatch):
    monkeypatch.setenv("TRACKING_DOMAIN", "https://track.example.com")
    monkeypatch.setenv("SITE_TOKEN", "site-tok-123")
    monkeypatch.setenv("STAPE_ENDPOINT", "https://stape.example.com/")
    r = client.get("/t/setup")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    body = r.text
    # Host + token are interpolated into the copy-paste snippet.
    assert "https://track.example.com/t/hyros.js" in body
    assert 'data-token="site-tok-123"' in body
    # STAPE_ENDPOINT (trailing slash stripped) is injected as a data attribute.
    assert 'data-stape-endpoint="https://stape.example.com"' in body


def test_setup_page_without_stape_omits_attr(client, monkeypatch):
    monkeypatch.delenv("STAPE_ENDPOINT", raising=False)
    monkeypatch.setenv("TRACKING_DOMAIN", "http://localhost:8000")
    r = client.get("/t/setup")
    assert r.status_code == 200
    assert "data-stape-endpoint" not in r.text


def test_ghl_setup_page_renders(client, monkeypatch):
    monkeypatch.setenv("TRACKING_DOMAIN", "https://track.example.com")
    monkeypatch.setenv("SITE_TOKEN", "tok-abc")
    r = client.get("/t/ghl-setup")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    body = r.text
    assert "GoHighLevel Setup" in body
    assert "https://track.example.com/t/hyros.js" in body
    assert "https://track.example.com/t/hyros-ghl.js" in body
    assert "https://track.example.com/api/webhooks/ghl" in body


# ── websocket ───────────────────────────────────────────────────────────────────
def test_websocket_ping_pong_and_disconnect(client):
    with client.websocket_connect("/ws") as ws:
        ws.send_text("ping")
        assert ws.receive_json() == {"type": "pong"}
    # Exiting the context manager disconnects cleanly (no exception).


def test_websocket_connects_without_auth_by_default(client):
    # Auth is disabled in the fixture, so no token is required to connect.
    with client.websocket_connect("/ws") as ws:
        ws.send_text("ping")
        assert ws.receive_json()["type"] == "pong"


# ── public-path behaviour under auth ────────────────────────────────────────────
def _auth_env(monkeypatch):
    monkeypatch.setenv("AUTH_ENABLED", "true")
    monkeypatch.setenv("AUTH_USERNAME", "admin")
    monkeypatch.setenv("AUTH_PASSWORD", "hunter2")
    monkeypatch.setenv("AUTH_SECRET_KEY", "s3cret-key")


def test_tracking_script_is_public_when_auth_enabled(client, monkeypatch):
    _auth_env(monkeypatch)
    # /t/* is whitelisted as public, so it serves even without a token while a
    # protected route (sanity check) returns 401.
    assert client.get("/api/integrations").status_code == 401
    r = client.get("/t/hyros.js")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("application/javascript")


def test_universal_script_is_public_when_auth_enabled(client, monkeypatch):
    _auth_env(monkeypatch)
    # /v1/lst/* is whitelisted as public.
    assert client.get("/v1/lst/universal-script").status_code == 200


def test_setup_page_is_public_when_auth_enabled(client, monkeypatch):
    _auth_env(monkeypatch)
    assert client.get("/t/setup").status_code == 200


def test_websocket_rejected_without_token_when_auth_enabled(client, monkeypatch):
    _auth_env(monkeypatch)
    # The /ws handler closes with code 4401 when auth is on and no valid token.
    import pytest
    from starlette.websockets import WebSocketDisconnect

    with pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect("/ws") as ws:
            ws.receive_json()
    assert exc.value.code == 4401


# ── outbound HTTP isolation guard ───────────────────────────────────────────────
def test_no_real_network_on_analytics_calls(client, api_db):
    # None of these endpoints make outbound HTTP; assert_all_mocked=False keeps the
    # in-process ASGI request flowing while forbidding any real network egress.
    _seed(api_db)
    with respx.mock(assert_all_mocked=False):
        assert client.get("/api/platforms").status_code == 200
        assert client.get("/api/integrations").status_code == 200
        assert client.get("/api/tracking").status_code == 200
