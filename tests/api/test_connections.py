"""Tests for the integrations/connections API (prefix /api/connections).

  GET /api/connections/status       — platform + webhook connection status
  GET /api/connections/setup-guide  — static setup instructions

Without ?validate=true the status is derived from credential presence (env vars,
plus stored credentials for GHL) and makes no outbound HTTP, so configured-but-
unverified platforms report state "unknown" rather than "connected".
"""

from __future__ import annotations

import asyncio

import api.connections as connections

# Every credential env var the module reads (ad platforms + stripe + GHL + webhooks).
ALL_ENV_VARS = [
    env_var
    for keys in connections.PLATFORM_ENV_KEYS.values()
    for env_var in keys.values()
] + [
    "STRIPE_API_SECRET_KEY", "STRIPE_SECRET_KEY",
    "SHOPIFY_WEBHOOK_SECRET", "STRIPE_WEBHOOK_SECRET",
    "GHL_API_TOKEN", "GHL_ACCESS_TOKEN", "GHL_LOCATION_ID",
]

EXPECTED_PLATFORMS = ["meta", "google", "tiktok", "stripe", "ghl"]


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
    assert names == EXPECTED_PLATFORMS
    for p in body["platforms"]:
        # Without any credentials set, nothing is configured (env-only path never
        # reaches a live check, so state is "not_configured").
        assert p["configured"] is False
        assert p["state"] == "not_configured"
        # Each field reflects whether its credential is set (all False here).
        assert all(v is False for v in p["fields"].values())
        assert isinstance(p["required_env"], list)

    assert body["webhooks"]["shopify"]["configured"] is False
    assert body["webhooks"]["shopify"]["endpoint"] == "/api/webhooks/shopify"
    assert body["webhooks"]["stripe"]["configured"] is False
    assert body["webhooks"]["stripe"]["endpoint"] == "/api/webhooks/stripe"
    assert body["summary"]["total"] == len(EXPECTED_PLATFORMS)
    assert body["summary"]["configured"] == 0
    assert body["validated"] is False


def test_status_meta_configured(client, monkeypatch):
    _clear_all(monkeypatch)
    monkeypatch.setenv("META_ACCESS_TOKEN", "tok")
    monkeypatch.setenv("META_APP_SECRET", "sec")
    monkeypatch.setenv("META_PIXEL_ID", "px")
    monkeypatch.setenv("META_AD_ACCOUNT_ID", "act_1")

    body = client.get("/api/connections/status").json()
    by_platform = {p["platform"]: p for p in body["platforms"]}

    meta = by_platform["meta"]
    assert meta["configured"] is True
    # Without validate=true the live check is skipped, so state stays "unknown".
    assert meta["state"] == "unknown"
    assert meta["fields"] == {
        "access_token": True,
        "app_secret": True,
        "pixel_id": True,
        "ad_account_id": True,
    }
    # Other platforms remain unconfigured.
    assert by_platform["google"]["configured"] is False
    assert by_platform["tiktok"]["configured"] is False


def test_status_partial_creds_not_configured(client, monkeypatch):
    _clear_all(monkeypatch)
    # Only one of meta's required vars is set.
    monkeypatch.setenv("META_ACCESS_TOKEN", "tok")

    meta = next(p for p in client.get("/api/connections/status").json()["platforms"] if p["platform"] == "meta")
    assert meta["configured"] is False
    assert meta["fields"]["access_token"] is True
    assert meta["fields"]["app_secret"] is False


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
    assert stripe["fields"]["secret_key"] is True


def test_status_ghl_configured_via_env(client, monkeypatch):
    _clear_all(monkeypatch)
    monkeypatch.setenv("GHL_API_TOKEN", "pit-token")
    monkeypatch.setenv("GHL_LOCATION_ID", "loc_123")

    ghl = next(p for p in client.get("/api/connections/status").json()["platforms"] if p["platform"] == "ghl")
    assert ghl["label"] == "GoHighLevel"
    assert ghl["configured"] is True
    assert ghl["fields"] == {"api_token": True, "location_id": True}


def test_status_webhooks_configured(client, monkeypatch):
    _clear_all(monkeypatch)
    monkeypatch.setenv("SHOPIFY_WEBHOOK_SECRET", "shh")
    monkeypatch.setenv("STRIPE_WEBHOOK_SECRET", "whsec")

    webhooks = client.get("/api/connections/status").json()["webhooks"]
    assert webhooks["shopify"]["configured"] is True
    assert webhooks["stripe"]["configured"] is True


# ── unit: status/field-helper edge cases ────────────────────────────────────────


def test_platform_status_unknown_platform(monkeypatch):
    # Unknown platform has no field map; the env-only path performs no live check,
    # so it is never "connected".
    status = asyncio.run(connections._platform_status("snapchat", validate=False, client=None))
    assert status["platform"] == "snapchat"
    assert status["state"] != "connected"
    assert status["fields"] == {}
    assert status["checked_at"] is None


def test_env_fields_unknown_platform_is_empty():
    # No env keys for an unknown platform -> empty field map and no missing vars.
    assert connections._env_fields("snapchat") == {}
    assert connections._missing_env("snapchat") == []


# ── GET /setup-guide ─────────────────────────────────────────────────────────────


def test_setup_guide_returns_all_platforms(client):
    r = client.get("/api/connections/setup-guide")
    assert r.status_code == 200
    body = r.json()
    assert set(body) == {"meta", "google", "tiktok", "shopify", "stripe", "ghl"}

    # Platforms that carry steps + api_docs.
    for plat in ("meta", "google", "tiktok", "ghl"):
        assert isinstance(body[plat]["steps"], list) and body[plat]["steps"]
        assert body[plat]["api_docs"].startswith("https://")

    # Webhook / key platforms carry steps only.
    for plat in ("shopify", "stripe"):
        assert isinstance(body[plat]["steps"], list) and body[plat]["steps"]
        assert "api_docs" not in body[plat]
