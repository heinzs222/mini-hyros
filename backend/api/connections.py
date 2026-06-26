from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import APIRouter

router = APIRouter()


# ── Platform env-var maps ─────────────────────────────────────────────────────

PLATFORM_ENV_KEYS = {
    "meta": {
        "access_token": "META_ACCESS_TOKEN",
        "app_secret": "META_APP_SECRET",
        "pixel_id": "META_PIXEL_ID",
        "ad_account_id": "META_AD_ACCOUNT_ID",
    },
    "google": {
        "client_id": "GOOGLE_ADS_CLIENT_ID",
        "client_secret": "GOOGLE_ADS_CLIENT_SECRET",
        "refresh_token": "GOOGLE_ADS_REFRESH_TOKEN",
        "developer_token": "GOOGLE_ADS_DEVELOPER_TOKEN",
        "customer_id": "GOOGLE_ADS_CUSTOMER_ID",
    },
    "tiktok": {
        "access_token": "TIKTOK_ACCESS_TOKEN",
        "app_id": "TIKTOK_APP_ID",
        "secret": "TIKTOK_SECRET",
        "advertiser_id": "TIKTOK_ADVERTISER_ID",
        "pixel_id": "TIKTOK_PIXEL_ID",
    },
}

# Minimal env vars required before we even attempt a live check.
REQUIRED_ENV = {
    "meta": ["META_ACCESS_TOKEN", "META_AD_ACCOUNT_ID"],
    "google": [
        "GOOGLE_ADS_CLIENT_ID",
        "GOOGLE_ADS_CLIENT_SECRET",
        "GOOGLE_ADS_REFRESH_TOKEN",
        "GOOGLE_ADS_DEVELOPER_TOKEN",
        "GOOGLE_ADS_CUSTOMER_ID",
    ],
    "tiktok": ["TIKTOK_ACCESS_TOKEN", "TIKTOK_ADVERTISER_ID"],
}

PLATFORM_LABELS = {
    "meta": "Meta", "google": "Google Ads", "tiktok": "TikTok",
    "stripe": "Stripe", "ghl": "GoHighLevel",
}

GHL_API_BASE = "https://services.leadconnectorhq.com"
GHL_API_VERSION = "2021-07-28"

HTTP_TIMEOUT = 8.0

TIKTOK_REQUIRED_ENDPOINT_PERMISSIONS = [
    "/advertiser/info/:GET",
    "/campaign/get/:GET",
    "/adgroup/get/:GET",
    "/ad/get/:GET",
    "/report/integrated/get/:GET",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _ghl_credentials() -> tuple[str, str]:
    """Return (token, location_id) for GHL from the warehouse or env."""
    try:
        from api.ghl_sync import get_ghl_credentials
        from attributionops.config import default_db_path
        db_path = os.environ.get("ATTRIBUTIONOPS_DB_PATH", default_db_path())
        return get_ghl_credentials(db_path)
    except Exception:
        token = (os.environ.get("GHL_API_TOKEN", "") or os.environ.get("GHL_ACCESS_TOKEN", "")).strip()
        return token, os.environ.get("GHL_LOCATION_ID", "").strip()


def _missing_env(platform: str) -> list[str]:
    return [v for v in REQUIRED_ENV.get(platform, []) if not os.environ.get(v, "").strip()]


def _stripe_key() -> str:
    return (os.environ.get("STRIPE_API_SECRET_KEY", "") or os.environ.get("STRIPE_SECRET_KEY", "")).strip()


# ── Live validators: return (state, detail) ───────────────────────────────────
# state ∈ {"connected", "expired", "invalid", "error", "not_configured"}

async def _validate_meta(client: httpx.AsyncClient) -> tuple[str, str]:
    token = os.environ.get("META_ACCESS_TOKEN", "").strip()
    try:
        resp = await client.get(
            "https://graph.facebook.com/v18.0/me",
            params={"fields": "id,name", "access_token": token},
        )
        body = resp.json()
        if resp.status_code == 200 and body.get("id"):
            return "connected", f"Token valid (account {body.get('name') or body['id']})."
        err = body.get("error") or {}
        code = err.get("code")
        msg = err.get("message") or "Unknown Meta API error."
        if code == 190:
            return "expired", f"Access token expired or revoked — {msg}"
        if code in (10, 200, 803):
            return "invalid", f"Token lacks required permissions — {msg}"
        return "invalid", msg
    except Exception as exc:  # network / timeout / DNS
        return "error", f"Could not reach Meta API: {exc}"


async def _validate_google(client: httpx.AsyncClient) -> tuple[str, str]:
    try:
        resp = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": os.environ.get("GOOGLE_ADS_CLIENT_ID", "").strip(),
                "client_secret": os.environ.get("GOOGLE_ADS_CLIENT_SECRET", "").strip(),
                "refresh_token": os.environ.get("GOOGLE_ADS_REFRESH_TOKEN", "").strip(),
                "grant_type": "refresh_token",
            },
        )
        body = resp.json()
        if resp.status_code == 200 and body.get("access_token"):
            return "connected", "OAuth refresh token valid; access token issued."
        err = str(body.get("error") or "")
        desc = str(body.get("error_description") or "")
        if err in ("invalid_grant",):
            return "expired", f"Refresh token expired or revoked — {desc or err}"
        if err in ("invalid_client", "unauthorized_client"):
            return "invalid", f"OAuth client credentials invalid — {desc or err}"
        return "invalid", desc or err or "Google OAuth token exchange failed."
    except Exception as exc:
        return "error", f"Could not reach Google OAuth: {exc}"


async def _validate_tiktok(client: httpx.AsyncClient) -> tuple[str, str]:
    token = os.environ.get("TIKTOK_ACCESS_TOKEN", "").strip()
    advertiser_id = os.environ.get("TIKTOK_ADVERTISER_ID", "").strip()
    token_source = "env"
    try:
        from api.platform_auth import get_or_refresh_tiktok_token, get_tiktok_advertiser_id
        from attributionops.config import default_db_path

        db_path = os.environ.get("ATTRIBUTIONOPS_DB_PATH", default_db_path())
        db_token = (await get_or_refresh_tiktok_token(db_path)).strip()
        db_advertiser_id = get_tiktok_advertiser_id(db_path).strip()
        if db_token:
            token = db_token
            token_source = "oauth"
        if db_advertiser_id:
            advertiser_id = db_advertiser_id
    except Exception:
        pass

    if not token or not advertiser_id:
        return "not_configured", "TikTok access token or advertiser ID is missing."

    try:
        resp = await client.get(
            "https://business-api.tiktok.com/open_api/v1.3/advertiser/info/",
            headers={"Access-Token": token},
            params={"advertiser_ids": json.dumps([advertiser_id]), "fields": json.dumps(["name", "status"])},
        )
        body = resp.json()
        code = body.get("code")
        msg = body.get("message") or "Unknown TikTok API error."
        if code == 0:
            return "connected", f"Access token valid with advertiser scope ({token_source})."
        if code in (40001, 40002, 40100, 40105):
            required = ", ".join(TIKTOK_REQUIRED_ENDPOINT_PERMISSIONS)
            return "invalid", (
                "Token missing TikTok Marketing API endpoint permission. "
                f"TikTok said: {msg}. Required for Mini Hyros: {required}. "
                "Enable these permissions for the TikTok app, then reconnect TikTok."
            )
        if code in (40010, 40104):
            return "expired", f"Access token expired — {msg}"
        return "invalid", msg
    except Exception as exc:
        return "error", f"Could not reach TikTok API: {exc}"


async def _validate_stripe(client: httpx.AsyncClient) -> tuple[str, str]:
    key = _stripe_key()
    try:
        resp = await client.get(
            "https://api.stripe.com/v1/charges",
            params={"limit": 1},
            headers={"Authorization": f"Bearer {key}"},
        )
        if resp.status_code == 200:
            return "connected", "Secret key valid."
        body = resp.json()
        msg = (body.get("error") or {}).get("message") or f"HTTP {resp.status_code}"
        if resp.status_code in (401, 403):
            return "invalid", f"Stripe key invalid or revoked — {msg}"
        return "error", msg
    except Exception as exc:
        return "error", f"Could not reach Stripe API: {exc}"


async def _validate_ghl(client: httpx.AsyncClient) -> tuple[str, str]:
    token, location_id = _ghl_credentials()
    if not token or not location_id:
        return "not_configured", "GHL API token or location id not set."
    try:
        resp = await client.get(
            f"{GHL_API_BASE}/locations/{location_id}",
            headers={"Authorization": f"Bearer {token}", "Version": GHL_API_VERSION, "Accept": "application/json"},
        )
        if resp.status_code == 200:
            return "connected", "API token valid for this location."
        if resp.status_code in (401, 403):
            return "invalid", "Token rejected — invalid or missing scope."
        if resp.status_code == 404:
            return "invalid", "Location not found for this token."
        return "error", f"GHL API returned HTTP {resp.status_code}."
    except Exception as exc:
        return "error", f"Could not reach GHL API: {exc}"


_VALIDATORS = {
    "meta": _validate_meta,
    "google": _validate_google,
    "tiktok": _validate_tiktok,
    "stripe": _validate_stripe,
    "ghl": _validate_ghl,
}


def _env_fields(platform: str) -> dict[str, bool]:
    return {label: bool(os.environ.get(env, "").strip()) for label, env in PLATFORM_ENV_KEYS.get(platform, {}).items()}


async def _platform_status(platform: str, validate: bool, client: httpx.AsyncClient | None) -> dict[str, Any]:
    if platform == "stripe":
        configured = bool(_stripe_key())
        required = ["STRIPE_API_SECRET_KEY (or STRIPE_SECRET_KEY)"]
        fields = {"secret_key": configured}
    elif platform == "ghl":
        token, location_id = _ghl_credentials()
        configured = bool(token and location_id)
        required = ["GHL_API_TOKEN + GHL_LOCATION_ID (or connect in Settings)"]
        fields = {"api_token": bool(token), "location_id": bool(location_id)}
    else:
        missing = _missing_env(platform)
        configured = not missing
        required = REQUIRED_ENV.get(platform, [])
        fields = _env_fields(platform)
        if platform == "tiktok" and not configured:
            try:
                from api.platform_auth import get_tiktok_advertiser_id, get_tiktok_token
                from attributionops.config import default_db_path

                db_path = os.environ.get("ATTRIBUTIONOPS_DB_PATH", default_db_path())
                if get_tiktok_token(db_path).strip() and get_tiktok_advertiser_id(db_path).strip():
                    configured = True
            except Exception:
                pass

    base = {
        "platform": platform,
        "label": PLATFORM_LABELS.get(platform, platform.title()),
        "configured": configured,
        "fields": fields,
        "required_env": required,
        "state": "not_configured" if not configured else "unknown",
        "detail": "" if configured else "Required credentials are not set.",
        "checked_at": None,
    }

    if not configured or not validate or client is None:
        return base

    state, detail = await _VALIDATORS[platform](client)
    base["state"] = state
    base["detail"] = detail
    base["checked_at"] = _now_iso()
    return base


@router.get("/status")
async def connections_status(validate: bool = False):
    """Connection status for each platform.

    By default returns env-var presence only (fast). With ?validate=true, each
    configured platform is checked against its live API so the UI can show
    Connected / Expired / Invalid-scope / Error instead of just 'configured'.
    """
    platform_names = ["meta", "google", "tiktok", "stripe", "ghl"]

    if validate:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            platforms = await asyncio.gather(
                *[_platform_status(p, True, client) for p in platform_names]
            )
        platforms = list(platforms)
    else:
        platforms = [await _platform_status(p, False, None) for p in platform_names]

    webhooks = {
        "shopify": {
            "configured": bool(os.environ.get("SHOPIFY_WEBHOOK_SECRET", "").strip()),
            "endpoint": "/api/webhooks/shopify",
        },
        "stripe": {
            "configured": bool(os.environ.get("STRIPE_WEBHOOK_SECRET", "").strip()),
            "endpoint": "/api/webhooks/stripe",
        },
    }

    summary = {
        "connected": sum(1 for p in platforms if p["state"] == "connected"),
        "configured": sum(1 for p in platforms if p["configured"]),
        "needs_attention": sum(1 for p in platforms if p["state"] in ("expired", "invalid", "error")),
        "total": len(platforms),
    }

    return {"platforms": platforms, "webhooks": webhooks, "summary": summary, "validated": validate}


@router.get("/setup-guide")
async def setup_guide():
    """Return a setup guide for connecting platforms."""
    return {
        "meta": {
            "steps": [
                "1. Go to https://developers.facebook.com/ and create an app",
                "2. Generate a long-lived access token with ads_read permission",
                "3. Find your Ad Account ID (act_XXXXX) in Ads Manager",
                "4. Set META_ACCESS_TOKEN, META_AD_ACCOUNT_ID in .env",
                "5. Optional: set META_PIXEL_ID for conversion tracking",
            ],
            "api_docs": "https://developers.facebook.com/docs/marketing-api/",
        },
        "google": {
            "steps": [
                "1. Create a Google Ads API developer token at https://ads.google.com/",
                "2. Set up OAuth2 credentials in Google Cloud Console",
                "3. Generate a refresh token using the OAuth2 flow",
                "4. Set GOOGLE_ADS_CLIENT_ID, GOOGLE_ADS_CLIENT_SECRET, GOOGLE_ADS_REFRESH_TOKEN, GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CUSTOMER_ID in .env",
            ],
            "api_docs": "https://developers.google.com/google-ads/api/docs/start",
        },
        "tiktok": {
            "steps": [
                "1. Apply for TikTok Marketing API access at https://ads.tiktok.com/marketing_api/",
                "2. Give the app endpoint permissions for advertiser/info, campaign/get, adgroup/get, ad/get, and report/integrated/get",
                "3. Generate/reconnect OAuth with advertiser.read, campaign.read, adgroup.read, ad.read, and report.read scopes",
                "4. Find your Advertiser ID in TikTok Ads Manager",
                "5. Set TIKTOK_ACCESS_TOKEN, TIKTOK_APP_ID, TIKTOK_SECRET, TIKTOK_ADVERTISER_ID in .env",
            ],
            "api_docs": "https://business-api.tiktok.com/marketing_api/docs",
        },
        "stripe": {
            "steps": [
                "1. In Stripe Dashboard → Developers → API keys, copy your secret key",
                "2. Set STRIPE_API_SECRET_KEY in .env for order import",
                "3. In Developers → Webhooks, add https://your-domain.com/api/webhooks/stripe",
                "4. Select checkout.session.completed + charge.succeeded; set STRIPE_WEBHOOK_SECRET in .env",
            ],
        },
        "shopify": {
            "steps": [
                "1. In Shopify Admin → Settings → Notifications → Webhooks",
                "2. Create webhook for 'Order payment' event",
                "3. Set URL to https://your-domain.com/api/webhooks/shopify",
                "4. Copy the webhook secret and set SHOPIFY_WEBHOOK_SECRET in .env",
            ],
        },
        "ghl": {
            "steps": [
                "1. In GoHighLevel → Settings → Private Integrations, create a token",
                "2. Grant the contacts.readonly and opportunities.readonly scopes",
                "3. Copy your Location ID from Settings → Business Profile",
                "4. In Settings → Connections here, paste the token + Location ID and Connect",
                "5. Leads, opportunities and booked calls then sync into the warehouse",
            ],
            "api_docs": "https://highlevel.stoplight.io/docs/integrations/",
        },
    }
