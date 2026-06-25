"""Tests for the platform OAuth integration (``api.platform_auth``).

Router mounted under ``/api/platform-auth`` in ``main.py``:
  GET  /api/platform-auth/tiktok/connect
  GET  /api/platform-auth/tiktok/callback   (public OAuth redirect target)
  POST /api/platform-auth/tiktok/refresh
  GET  /api/platform-auth/tiktok/status

All outbound TikTok HTTP is mocked with ``respx`` (token + refresh endpoints
on ``https://business-api.tiktok.com``). No real network, no sleeps. We assert
on response JSON / redirects AND on the ``platform_tokens`` DB table.
"""

from __future__ import annotations

import sqlite3
from urllib.parse import quote

import asyncio
from datetime import datetime, timedelta, timezone

import httpx
import respx


def _token_row(db_path: str):
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                "SELECT * FROM platform_tokens WHERE platform='tiktok'"
            ).fetchall()
        except sqlite3.OperationalError:
            # Table only created lazily by the endpoint on a successful path.
            return []
    return [dict(r) for r in rows]


def _seed_token(db_path: str, **overrides):
    row = {
        "platform": "tiktok",
        "access_token": "old_access",
        "refresh_token": "old_refresh",
        "advertiser_id": "adv_seed",
        "expires_at": "",
        "updated_at": "2026-01-01T00:00:00Z",
    }
    row.update(overrides)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS platform_tokens (
                platform TEXT PRIMARY KEY, access_token TEXT, refresh_token TEXT,
                advertiser_id TEXT, expires_at TEXT, updated_at TEXT)"""
        )
        conn.execute(
            "INSERT OR REPLACE INTO platform_tokens (platform, access_token, refresh_token, advertiser_id, expires_at, updated_at) "
            "VALUES (?,?,?,?,?,?)",
            (row["platform"], row["access_token"], row["refresh_token"],
             row["advertiser_id"], row["expires_at"], row["updated_at"]),
        )
        conn.commit()


# ── connect ─────────────────────────────────────────────────────────────────────

def test_connect_returns_auth_url_with_params(client, api_db, monkeypatch):
    monkeypatch.setenv("TIKTOK_APP_ID", "app-12345")
    monkeypatch.setenv("BACKEND_URL", "https://my-backend.example.com")

    resp = client.get("/api/platform-auth/tiktok/connect")
    assert resp.status_code == 200
    body = resp.json()
    assert body["app_id"] == "app-12345"
    assert body["redirect_uri"] == "https://my-backend.example.com"

    url = body["auth_url"]
    assert url.startswith("https://business-api.tiktok.com/portal/auth")
    assert "app_id=app-12345" in url
    assert "state=tiktok_oauth" in url
    # redirect_uri and scope are URL-encoded.
    assert f"redirect_uri={quote('https://my-backend.example.com', safe='')}" in url
    assert "scope=" in url
    assert quote("advertiser.read,campaign.read,adgroup.read,ad.read,report.read", safe="") in url


def test_connect_without_app_id_returns_error(client, api_db, monkeypatch):
    monkeypatch.delenv("TIKTOK_APP_ID", raising=False)
    resp = client.get("/api/platform-auth/tiktok/connect")
    assert resp.status_code == 200
    assert resp.json() == {"error": "TIKTOK_APP_ID not set in environment"}


# ── callback ────────────────────────────────────────────────────────────────────

def test_callback_missing_auth_code_returns_400(client, api_db, monkeypatch):
    monkeypatch.setenv("TIKTOK_APP_ID", "app-1")
    monkeypatch.setenv("TIKTOK_SECRET", "sec-1")
    resp = client.get("/api/platform-auth/tiktok/callback")
    assert resp.status_code == 400
    assert "Missing auth_code" in resp.text


def test_callback_missing_credentials_returns_500(client, api_db, monkeypatch):
    monkeypatch.delenv("TIKTOK_APP_ID", raising=False)
    monkeypatch.delenv("TIKTOK_SECRET", raising=False)
    resp = client.get("/api/platform-auth/tiktok/callback", params={"auth_code": "abc"})
    assert resp.status_code == 500
    assert "not configured" in resp.text


def test_callback_success_stores_token_and_redirects(client, api_db, monkeypatch):
    monkeypatch.setenv("TIKTOK_APP_ID", "app-1")
    monkeypatch.setenv("TIKTOK_SECRET", "sec-1")
    monkeypatch.setenv("DASHBOARD_URL", "https://dash.example.com")

    token_response = {
        "code": 0,
        "message": "OK",
        "data": {
            "access_token": "fresh_access_token",
            "refresh_token": "fresh_refresh_token",
            "advertiser_ids": ["adv-999"],
        },
    }

    with respx.mock(assert_all_mocked=False) as router:
        route = router.post(
            url__startswith="https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token"
        ).mock(return_value=httpx.Response(200, json=token_response))

        resp = client.get(
            "/api/platform-auth/tiktok/callback",
            params={"auth_code": "the_code", "state": "tiktok_oauth"},
            follow_redirects=False,
        )

    assert route.called
    assert resp.status_code in (302, 307)
    assert resp.headers["location"] == "https://dash.example.com?tiktok_connected=1"

    rows = _token_row(api_db)
    assert len(rows) == 1
    assert rows[0]["access_token"] == "fresh_access_token"
    assert rows[0]["refresh_token"] == "fresh_refresh_token"
    assert rows[0]["advertiser_id"] == "adv-999"


def test_callback_uses_env_advertiser_when_none_returned(client, api_db, monkeypatch):
    monkeypatch.setenv("TIKTOK_APP_ID", "app-1")
    monkeypatch.setenv("TIKTOK_SECRET", "sec-1")
    monkeypatch.setenv("TIKTOK_ADVERTISER_ID", "env-adv")
    monkeypatch.setenv("DASHBOARD_URL", "http://localhost:3000")

    token_response = {"code": 0, "data": {"access_token": "a", "refresh_token": "r", "advertiser_ids": []}}
    with respx.mock(assert_all_mocked=False) as router:
        router.post(url__startswith="https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token").mock(
            return_value=httpx.Response(200, json=token_response)
        )
        resp = client.get(
            "/api/platform-auth/tiktok/callback",
            params={"auth_code": "code"},
            follow_redirects=False,
        )
    assert resp.status_code in (302, 307)
    assert _token_row(api_db)[0]["advertiser_id"] == "env-adv"


def test_callback_tiktok_error_code_returns_400(client, api_db, monkeypatch):
    monkeypatch.setenv("TIKTOK_APP_ID", "app-1")
    monkeypatch.setenv("TIKTOK_SECRET", "sec-1")

    err = {"code": 40001, "message": "invalid auth_code"}
    with respx.mock(assert_all_mocked=False) as router:
        router.post(url__startswith="https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token").mock(
            return_value=httpx.Response(200, json=err)
        )
        resp = client.get("/api/platform-auth/tiktok/callback", params={"auth_code": "bad"})

    assert resp.status_code == 400
    assert "invalid auth_code" in resp.text
    # Nothing stored on error.
    assert _token_row(api_db) == []


def test_callback_no_access_token_returns_400(client, api_db, monkeypatch):
    monkeypatch.setenv("TIKTOK_APP_ID", "app-1")
    monkeypatch.setenv("TIKTOK_SECRET", "sec-1")

    resp_json = {"code": 0, "data": {"access_token": "", "refresh_token": "r"}}
    with respx.mock(assert_all_mocked=False) as router:
        router.post(url__startswith="https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token").mock(
            return_value=httpx.Response(200, json=resp_json)
        )
        resp = client.get("/api/platform-auth/tiktok/callback", params={"auth_code": "x"})

    assert resp.status_code == 400
    assert "No access_token" in resp.text
    assert _token_row(api_db) == []


# ── refresh ─────────────────────────────────────────────────────────────────────

def test_refresh_without_stored_token_returns_error(client, api_db, monkeypatch):
    monkeypatch.setenv("TIKTOK_APP_ID", "app-1")
    monkeypatch.setenv("TIKTOK_SECRET", "sec-1")
    resp = client.post("/api/platform-auth/tiktok/refresh")
    assert resp.status_code == 200
    assert resp.json() == {"error": "No refresh_token stored. Please re-connect TikTok."}


def test_refresh_success_updates_token(client, api_db, monkeypatch):
    monkeypatch.setenv("TIKTOK_APP_ID", "app-1")
    monkeypatch.setenv("TIKTOK_SECRET", "sec-1")
    _seed_token(api_db, access_token="old_access", refresh_token="old_refresh", advertiser_id="adv-seed")

    refresh_response = {
        "code": 0,
        "data": {"access_token": "renewed_access", "refresh_token": "renewed_refresh"},
    }
    with respx.mock(assert_all_mocked=False) as router:
        route = router.post(
            url__startswith="https://business-api.tiktok.com/open_api/v1.3/oauth2/refresh_token"
        ).mock(return_value=httpx.Response(200, json=refresh_response))

        resp = client.post("/api/platform-auth/tiktok/refresh")

    assert route.called
    assert resp.status_code == 200
    body = resp.json()
    assert body["refreshed"] is True
    assert body["advertiser_id"] == "adv-seed"

    row = _token_row(api_db)[0]
    assert row["access_token"] == "renewed_access"
    assert row["refresh_token"] == "renewed_refresh"


def test_refresh_keeps_old_refresh_token_when_not_returned(client, api_db, monkeypatch):
    monkeypatch.setenv("TIKTOK_APP_ID", "app-1")
    monkeypatch.setenv("TIKTOK_SECRET", "sec-1")
    _seed_token(api_db, refresh_token="keep_me")

    refresh_response = {"code": 0, "data": {"access_token": "new_only"}}
    with respx.mock(assert_all_mocked=False) as router:
        router.post(url__startswith="https://business-api.tiktok.com/open_api/v1.3/oauth2/refresh_token").mock(
            return_value=httpx.Response(200, json=refresh_response)
        )
        resp = client.post("/api/platform-auth/tiktok/refresh")

    assert resp.json()["refreshed"] is True
    row = _token_row(api_db)[0]
    assert row["access_token"] == "new_only"
    assert row["refresh_token"] == "keep_me"


def test_refresh_tiktok_error_returns_error_message(client, api_db, monkeypatch):
    monkeypatch.setenv("TIKTOK_APP_ID", "app-1")
    monkeypatch.setenv("TIKTOK_SECRET", "sec-1")
    _seed_token(api_db, access_token="stay", refresh_token="stay_refresh")

    err = {"code": 40105, "message": "refresh token expired"}
    with respx.mock(assert_all_mocked=False) as router:
        router.post(url__startswith="https://business-api.tiktok.com/open_api/v1.3/oauth2/refresh_token").mock(
            return_value=httpx.Response(200, json=err)
        )
        resp = client.post("/api/platform-auth/tiktok/refresh")

    assert resp.status_code == 200
    body = resp.json()
    assert "refresh token expired" in body["error"]
    # Token unchanged on error.
    assert _token_row(api_db)[0]["access_token"] == "stay"


# ── status ──────────────────────────────────────────────────────────────────────

def test_status_disconnected_on_fresh_db(client, api_db, monkeypatch):
    monkeypatch.delenv("TIKTOK_ACCESS_TOKEN", raising=False)
    monkeypatch.setenv("TIKTOK_APP_ID", "app-1")
    resp = client.get("/api/platform-auth/tiktok/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["connected"] is False
    # auth_url is still provided so the UI can offer a connect link.
    assert body["auth_url"].startswith("https://business-api.tiktok.com/portal/auth")
    assert "app_id=app-1" in body["auth_url"]


def test_status_connected_from_database(client, api_db, monkeypatch):
    monkeypatch.setenv("TIKTOK_APP_ID", "app-1")
    _seed_token(api_db, access_token="db_token", refresh_token="db_refresh", advertiser_id="adv-db")

    resp = client.get("/api/platform-auth/tiktok/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["connected"] is True
    assert body["source"] == "database"
    assert body["advertiser_id"] == "adv-db"
    assert body["has_refresh_token"] is True


def test_status_connected_from_env_token(client, api_db, monkeypatch):
    # No DB token, but an env access token is present.
    monkeypatch.setenv("TIKTOK_ACCESS_TOKEN", "env_only_token")
    monkeypatch.setenv("TIKTOK_ADVERTISER_ID", "adv-env")
    monkeypatch.setenv("TIKTOK_APP_ID", "app-1")

    resp = client.get("/api/platform-auth/tiktok/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["connected"] is True
    assert body["source"] == "env"
    assert body["advertiser_id"] == "adv-env"
    assert body["has_refresh_token"] is False


# ── Token accessor helpers (used by sibling modules e.g. spend sync) ────────────

def test_get_tiktok_token_prefers_db_then_env(api_db, monkeypatch):
    import api.platform_auth as pa

    # No DB token → env fallback.
    monkeypatch.setenv("TIKTOK_ACCESS_TOKEN", "env_token")
    assert pa.get_tiktok_token(api_db) == "env_token"

    # DB token takes precedence.
    _seed_token(api_db, access_token="db_token")
    assert pa.get_tiktok_token(api_db) == "db_token"


def test_get_tiktok_advertiser_id_prefers_db_then_env(api_db, monkeypatch):
    import api.platform_auth as pa

    monkeypatch.setenv("TIKTOK_ADVERTISER_ID", "env_adv")
    assert pa.get_tiktok_advertiser_id(api_db) == "env_adv"

    _seed_token(api_db, advertiser_id="db_adv")
    assert pa.get_tiktok_advertiser_id(api_db) == "db_adv"


def test_get_or_refresh_returns_env_when_no_db_token(api_db, monkeypatch):
    import api.platform_auth as pa

    monkeypatch.setenv("TIKTOK_ACCESS_TOKEN", "env_token")
    result = asyncio.run(pa.get_or_refresh_tiktok_token(api_db))
    assert result == "env_token"


def test_get_or_refresh_returns_current_token_when_not_expiring(api_db, monkeypatch):
    import api.platform_auth as pa

    future = (datetime.now(timezone.utc) + timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    _seed_token(api_db, access_token="valid_token", refresh_token="r", expires_at=future)
    result = asyncio.run(pa.get_or_refresh_tiktok_token(api_db))
    assert result == "valid_token"


def test_get_or_refresh_auto_refreshes_when_expiring(api_db, monkeypatch):
    import api.platform_auth as pa

    monkeypatch.setenv("TIKTOK_APP_ID", "app-1")
    monkeypatch.setenv("TIKTOK_SECRET", "sec-1")
    past = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    _seed_token(api_db, access_token="stale", refresh_token="r1", advertiser_id="adv-x", expires_at=past)

    refresh_response = {"code": 0, "data": {"access_token": "auto_refreshed", "refresh_token": "r2"}}
    with respx.mock(assert_all_mocked=False) as router:
        route = router.post(
            url__startswith="https://business-api.tiktok.com/open_api/v1.3/oauth2/refresh_token"
        ).mock(return_value=httpx.Response(200, json=refresh_response))
        result = asyncio.run(pa.get_or_refresh_tiktok_token(api_db))

    assert route.called
    assert result == "auto_refreshed"
    # The refreshed token is persisted.
    assert _token_row(api_db)[0]["access_token"] == "auto_refreshed"
