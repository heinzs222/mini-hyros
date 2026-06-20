"""Tests for the integrations/connections API (prefix /api/connections).

  GET /api/connections/status       — platform + webhook connection status
  GET /api/connections/setup-guide  — static setup instructions

Without ``?validate=true`` the status endpoint derives everything from
environment variables (no DB access, no outbound HTTP), so tests toggle the
relevant vars with monkeypatch. Each platform reports ``configured`` (its
required env vars are present) and a ``state`` that is ``not_configured`` until
configured and ``unknown`` until a live validation is run.
"""

from __future__ import annotations

import asyncio

import api.connections as connections

# Every env var the module reads (platform creds + Stripe key + webhook secrets).
ALL_ENV_VARS = (
    [env_var for keys in connections.PLATFORM_ENV_KEYS.values() for env_var in keys.values()]
    + ["STRIPE_API_SECRET_KEY", "STRIPE_SECRET_KEY"]
    + ["SHOPIFY_WEBHOOK_SECRET", "STRIPE_WEBHOOK_SECRET"]
)


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
    assert names == ["meta", "google", "tiktok", "stripe"]
    for p in body["platforms"]:
        # Without any env vars set, nothing is configured and the env-only path
        # never reaches a live check, so state is "not_configured".
        assert p["configured"] is False
        assert p["state"] == "not_configured"
        assert p["checked_at"] is None
        # Each field reflects whether its env var is set (all False here).
        assert all(v is False for v in p["fields"].values())
        assert isinstance(p["required_env"], list)
        assert p["detail"] == "Required credentials are not set."

    assert body["webhooks"]["shopify"]["configured"] is False
    assert body["webhooks"]["shopify"]["endpoint"] == "/api/webhooks/shopify"
    assert body["webhooks"]["stripe"]["configured"] is False
    assert body["webhooks"]["stripe"]["endpoint"] == "/api/webhooks/stripe"

    # Without ?validate=true the response is not validated.
    assert body["validated"] is False
    assert body["summary"]["connected"] == 0
    assert body["summary"]["configured"] == 0
    assert body["summary"]["total"] == 4


def test_status_meta_configured(client, monkeypatch):
    _clear_all(monkeypatch)
    # Meta only requires an access token + ad account id to be "configured".
    monkeypatch.setenv("META_ACCESS_TOKEN", "tok")
    monkeypatch.setenv("META_AD_ACCOUNT_ID", "act_1")

    body = client.get("/api/connections/status").json()
    by_platform = {p["platform"]: p for p in body["platforms"]}

    meta = by_platform["meta"]
    assert meta["configured"] is True
    # Configured but not yet live-validated.
    assert meta["state"] == "unknown"
    assert meta["fields"] == {
        "access_token": True,
        "app_secret": False,
        "pixel_id": False,
        "ad_account_id": True,
    }
    # Other platforms remain unconfigured.
    assert by_platform["google"]["configured"] is False
    assert by_platform["tiktok"]["configured"] is False
    assert by_platform["stripe"]["configured"] is False


def test_status_partial_creds_not_configured(client, monkeypatch):
    _clear_all(monkeypatch)
    # Only one of meta's required vars is set.
    monkeypatch.setenv("META_ACCESS_TOKEN", "tok")

    meta = next(p for p in client.get("/api/connections/status").json()["platforms"] if p["platform"] == "meta")
    assert meta["configured"] is False
    assert meta["state"] == "not_configured"
    assert meta["fields"]["access_token"] is True
    assert meta["fields"]["ad_account_id"] is False


def test_status_google_and_tiktok_configured(client, monkeypatch):
    _clear_all(monkeypatch)
    for var in connections.PLATFORM_ENV_KEYS["google"].values():
        monkeypatch.setenv(var, "x")
    for var in connections.PLATFORM_ENV_KEYS["tiktok"].values():
        monkeypatch.setenv(var, "y")

    by_platform = {p["platform"]: p for p in client.get("/api/connections/status").json()["platforms"]}
    assert by_platform["google"]["configured"] is True
    assert by_platform["tiktok"]["configured"] is True
    assert by_platform["meta"]["configured"] is False


def test_status_stripe_configured(client, monkeypatch):
    _clear_all(monkeypatch)
    monkeypatch.setenv("STRIPE_API_SECRET_KEY", "sk_test_123")

    stripe = next(p for p in client.get("/api/connections/status").json()["platforms"] if p["platform"] == "stripe")
    assert stripe["configured"] is True
    assert stripe["fields"] == {"secret_key": True}


def test_status_webhooks_configured(client, monkeypatch):
    _clear_all(monkeypatch)
    monkeypatch.setenv("SHOPIFY_WEBHOOK_SECRET", "shh")
    monkeypatch.setenv("STRIPE_WEBHOOK_SECRET", "whsec")

    webhooks = client.get("/api/connections/status").json()["webhooks"]
    assert webhooks["shopify"]["configured"] is True
    assert webhooks["stripe"]["configured"] is True


# ── unit: _platform_status edge cases ───────────────────────────────────────────


def test_platform_status_unknown_platform(monkeypatch):
    _clear_all(monkeypatch)
    # An unknown platform has no required env and no field map; with validation
    # off it returns its base record without touching the network, so it is
    # never "connected".
    status = asyncio.run(connections._platform_status("snapchat", validate=False, client=None))
    assert status["platform"] == "snapchat"
    assert status["state"] != "connected"
    assert status["fields"] == {}
    assert status["checked_at"] is None


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
