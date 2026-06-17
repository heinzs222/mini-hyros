"""Tests for the integrations/connections API (prefix /api/connections).

  GET /api/connections/status       — platform + webhook connection status from env vars
  GET /api/connections/setup-guide  — static setup instructions

Connection status is derived purely from environment variables, so tests toggle
them with monkeypatch. The module makes no DB access and no outbound HTTP.
"""

from __future__ import annotations

import api.connections as connections

# Every env var the module reads (platform creds + webhook secrets).
ALL_ENV_VARS = [
    env_var
    for keys in connections.PLATFORM_ENV_KEYS.values()
    for env_var in keys.values()
] + ["SHOPIFY_WEBHOOK_SECRET", "STRIPE_WEBHOOK_SECRET"]


def _clear_all(monkeypatch):
    for var in ALL_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


# ── GET /status ─────────────────────────────────────────────────────────────────


def test_status_all_disconnected_by_default(client, monkeypatch):
    _clear_all(monkeypatch)
    r = client.get("/api/connections/status")
    assert r.status_code == 200
    body = r.json()

    names = [p["platform"] for p in body["platforms"]]
    assert names == ["meta", "google", "tiktok"]
    for p in body["platforms"]:
        assert p["connected"] is False
        # Each field reflects whether its env var is set (all False here).
        assert all(v is False for v in p["fields"].values())
        assert p["notes"].endswith(f"connect {p['platform']}.")
        assert p["env_keys"]  # mapping of field -> env var name is exposed

    assert body["webhooks"]["shopify"]["configured"] is False
    assert body["webhooks"]["shopify"]["endpoint"] == "/api/webhooks/shopify"
    assert body["webhooks"]["stripe"]["configured"] is False
    assert body["webhooks"]["stripe"]["endpoint"] == "/api/webhooks/stripe"


def test_status_meta_fully_connected(client, monkeypatch):
    _clear_all(monkeypatch)
    monkeypatch.setenv("META_ACCESS_TOKEN", "tok")
    monkeypatch.setenv("META_APP_SECRET", "sec")
    monkeypatch.setenv("META_PIXEL_ID", "px")
    monkeypatch.setenv("META_AD_ACCOUNT_ID", "act_1")

    body = client.get("/api/connections/status").json()
    by_platform = {p["platform"]: p for p in body["platforms"]}

    meta = by_platform["meta"]
    assert meta["connected"] is True
    assert meta["fields"] == {
        "access_token": True,
        "app_secret": True,
        "pixel_id": True,
        "ad_account_id": True,
    }
    # Other platforms remain disconnected.
    assert by_platform["google"]["connected"] is False
    assert by_platform["tiktok"]["connected"] is False


def test_status_partial_creds_not_connected(client, monkeypatch):
    _clear_all(monkeypatch)
    # Only one of meta's four required vars is set.
    monkeypatch.setenv("META_ACCESS_TOKEN", "tok")

    meta = next(p for p in client.get("/api/connections/status").json()["platforms"] if p["platform"] == "meta")
    assert meta["connected"] is False
    assert meta["fields"]["access_token"] is True
    assert meta["fields"]["app_secret"] is False


def test_status_google_and_tiktok_connected(client, monkeypatch):
    _clear_all(monkeypatch)
    for var in connections.PLATFORM_ENV_KEYS["google"].values():
        monkeypatch.setenv(var, "x")
    for var in connections.PLATFORM_ENV_KEYS["tiktok"].values():
        monkeypatch.setenv(var, "y")

    by_platform = {p["platform"]: p for p in client.get("/api/connections/status").json()["platforms"]}
    assert by_platform["google"]["connected"] is True
    assert by_platform["tiktok"]["connected"] is True
    assert by_platform["meta"]["connected"] is False


def test_status_webhooks_configured(client, monkeypatch):
    _clear_all(monkeypatch)
    monkeypatch.setenv("SHOPIFY_WEBHOOK_SECRET", "shh")
    monkeypatch.setenv("STRIPE_WEBHOOK_SECRET", "whsec")

    webhooks = client.get("/api/connections/status").json()["webhooks"]
    assert webhooks["shopify"]["configured"] is True
    assert webhooks["stripe"]["configured"] is True


# ── unit: _get_connection_status edge cases ─────────────────────────────────────


def test_get_connection_status_unknown_platform(monkeypatch):
    # No env keys for an unknown platform -> never "connected".
    status = connections._get_connection_status("snapchat")
    assert status["platform"] == "snapchat"
    assert status["connected"] is False
    assert status["fields"] == {}
    assert status["env_keys"] == {}


# ── GET /setup-guide ─────────────────────────────────────────────────────────────


def test_setup_guide_returns_all_platforms(client):
    r = client.get("/api/connections/setup-guide")
    assert r.status_code == 200
    body = r.json()
    assert set(body) == {"meta", "google", "tiktok", "shopify", "stripe"}

    # Ad platforms carry steps + api_docs.
    for plat in ("meta", "google", "tiktok"):
        assert isinstance(body[plat]["steps"], list) and body[plat]["steps"]
        assert body[plat]["api_docs"].startswith("https://")

    # Webhook platforms carry steps only.
    for plat in ("shopify", "stripe"):
        assert isinstance(body[plat]["steps"], list) and body[plat]["steps"]
        assert "api_docs" not in body[plat]
