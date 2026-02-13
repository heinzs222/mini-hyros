from __future__ import annotations

import os
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()


class ConnectionConfig(BaseModel):
    platform: str
    api_key: str = ""
    api_secret: str = ""
    access_token: str = ""
    account_id: str = ""
    pixel_id: str = ""


# ── Platform API configs (read from env vars) ─────────────────────────────────

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


def _get_connection_status(platform: str) -> dict[str, Any]:
    env_keys = PLATFORM_ENV_KEYS.get(platform, {})
    configured = {}
    all_set = True
    for label, env_var in env_keys.items():
        val = os.environ.get(env_var, "")
        configured[label] = bool(val)
        if not val:
            all_set = False

    return {
        "platform": platform,
        "connected": all_set and bool(env_keys),
        "fields": configured,
        "env_keys": env_keys,
        "notes": f"Set env vars in .env file to connect {platform}.",
    }


@router.get("/status")
async def connections_status():
    """Show which ad platforms are connected via env vars."""
    platforms = []
    for p in ("meta", "google", "tiktok"):
        platforms.append(_get_connection_status(p))

    webhooks = {
        "shopify": {
            "configured": bool(os.environ.get("SHOPIFY_WEBHOOK_SECRET", "")),
            "endpoint": "/api/webhooks/shopify",
        },
        "stripe": {
            "configured": bool(os.environ.get("STRIPE_WEBHOOK_SECRET", "")),
            "endpoint": "/api/webhooks/stripe",
        },
    }

    return {"platforms": platforms, "webhooks": webhooks}


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
                "2. Create an app and get your access token",
                "3. Find your Advertiser ID in TikTok Ads Manager",
                "4. Set TIKTOK_ACCESS_TOKEN, TIKTOK_APP_ID, TIKTOK_SECRET, TIKTOK_ADVERTISER_ID in .env",
            ],
            "api_docs": "https://business-api.tiktok.com/marketing_api/docs",
        },
        "shopify": {
            "steps": [
                "1. In Shopify Admin → Settings → Notifications → Webhooks",
                "2. Create webhook for 'Order payment' event",
                "3. Set URL to https://your-domain.com/api/webhooks/shopify",
                "4. Copy the webhook secret and set SHOPIFY_WEBHOOK_SECRET in .env",
            ],
        },
        "stripe": {
            "steps": [
                "1. In Stripe Dashboard → Developers → Webhooks",
                "2. Add endpoint: https://your-domain.com/api/webhooks/stripe",
                "3. Select events: checkout.session.completed, charge.succeeded",
                "4. Copy signing secret and set STRIPE_WEBHOOK_SECRET in .env",
            ],
        },
    }
