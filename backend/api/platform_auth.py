"""Platform OAuth token management.

Handles OAuth flows for ad platforms that require user authorization (TikTok).
Tokens are stored in a platform_tokens table in the DB.

Endpoints:
  GET  /api/platform-auth/tiktok/connect      → returns authorization URL
  GET  /api/platform-auth/tiktok/callback     → exchanges auth_code, stores token
  POST /api/platform-auth/tiktok/refresh      → refreshes access token
  GET  /api/platform-auth/tiktok/status       → current token info
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, Query
from fastapi.responses import RedirectResponse, HTMLResponse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from attributionops.config import default_db_path
from attributionops.db import connect, sql_rows

router = APIRouter()
UTC = timezone.utc

TIKTOK_TOKEN_URL = "https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/"
TIKTOK_REFRESH_URL = "https://business-api.tiktok.com/open_api/v1.3/oauth2/refresh_token/"
TIKTOK_AUTH_BASE = "https://business-api.tiktok.com/portal/auth"


def _db() -> str:
    return os.environ.get("ATTRIBUTIONOPS_DB_PATH", default_db_path())


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ensure_tokens_table(db_path: str) -> None:
    with connect(db_path) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS platform_tokens (
            platform TEXT PRIMARY KEY,
            access_token TEXT,
            refresh_token TEXT,
            advertiser_id TEXT,
            expires_at TEXT,
            updated_at TEXT
        )""")
        conn.commit()


def get_tiktok_token(db_path: str) -> str:
    """Return best available TikTok access token (DB first, env fallback)."""
    _ensure_tokens_table(db_path)
    rows = sql_rows(db_path, "SELECT access_token FROM platform_tokens WHERE platform='tiktok'")
    if rows and rows[0].get("access_token"):
        return str(rows[0]["access_token"])
    return os.environ.get("TIKTOK_ACCESS_TOKEN", "")


def get_tiktok_advertiser_id(db_path: str) -> str:
    """Return best available TikTok advertiser_id (DB first, env fallback)."""
    _ensure_tokens_table(db_path)
    rows = sql_rows(db_path, "SELECT advertiser_id FROM platform_tokens WHERE platform='tiktok'")
    if rows and rows[0].get("advertiser_id"):
        return str(rows[0]["advertiser_id"])
    return os.environ.get("TIKTOK_ADVERTISER_ID", "")


def _save_tiktok_token(db_path: str, access_token: str, refresh_token: str,
                        advertiser_id: str, expires_at: str = "") -> None:
    _ensure_tokens_table(db_path)
    with connect(db_path) as conn:
        conn.execute(
            """INSERT OR REPLACE INTO platform_tokens
               (platform, access_token, refresh_token, advertiser_id, expires_at, updated_at)
               VALUES ('tiktok', ?, ?, ?, ?, ?)""",
            (access_token, refresh_token, advertiser_id, expires_at, _now()),
        )
        conn.commit()


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/tiktok/connect")
async def tiktok_connect():
    """Return the TikTok authorization URL for the user to visit."""
    app_id = os.environ.get("TIKTOK_APP_ID", "").strip()
    if not app_id:
        return {"error": "TIKTOK_APP_ID not set in environment"}

    backend_url = os.environ.get("BACKEND_URL", "https://mini-hyros.onrender.com").rstrip("/")
    # Use base URL as redirect_uri — matches TikTok's registered URL.
    # The root GET / handler forwards auth_code to /api/platform-auth/tiktok/callback.
    redirect_uri = backend_url

    from urllib.parse import quote
    auth_url = (
        f"{TIKTOK_AUTH_BASE}"
        f"?app_id={app_id}"
        f"&state=tiktok_oauth"
        f"&redirect_uri={quote(redirect_uri, safe='')}"
    )
    return {"auth_url": auth_url, "app_id": app_id, "redirect_uri": redirect_uri}


@router.get("/tiktok/callback")
async def tiktok_callback(auth_code: str = Query(default=""), state: str = Query(default="")):
    """Handle TikTok OAuth redirect — exchange auth_code for access token."""
    if not auth_code:
        return HTMLResponse("<h3>Missing auth_code. Please try connecting again.</h3>", status_code=400)

    app_id = os.environ.get("TIKTOK_APP_ID", "").strip()
    secret = os.environ.get("TIKTOK_SECRET", "").strip()

    if not app_id or not secret:
        return HTMLResponse(
            "<h3>TIKTOK_APP_ID or TIKTOK_SECRET not configured on server.</h3>", status_code=500
        )

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(
                TIKTOK_TOKEN_URL,
                json={"app_id": app_id, "auth_code": auth_code, "secret": secret},
                headers={"Content-Type": "application/json"},
            )
            data = resp.json()

        code = int(data.get("code") or 0)
        if code != 0:
            msg = data.get("message") or str(data)
            return HTMLResponse(f"<h3>TikTok error ({code}): {msg}</h3>", status_code=400)

        token_data: dict[str, Any] = data.get("data") or {}
        access_token = str(token_data.get("access_token") or "")
        refresh_token = str(token_data.get("refresh_token") or "")
        advertiser_ids: list = token_data.get("advertiser_ids") or []
        advertiser_id = str(advertiser_ids[0]) if advertiser_ids else os.environ.get("TIKTOK_ADVERTISER_ID", "")

        if not access_token:
            return HTMLResponse("<h3>No access_token in TikTok response.</h3>", status_code=400)

        _save_tiktok_token(_db(), access_token, refresh_token, advertiser_id)

        # Redirect back to dashboard with success
        dashboard_url = os.environ.get("DASHBOARD_URL", "http://localhost:3000")
        return RedirectResponse(url=f"{dashboard_url}?tiktok_connected=1")

    except Exception as e:
        return HTMLResponse(f"<h3>Error: {e}</h3>", status_code=500)


@router.post("/tiktok/refresh")
async def tiktok_refresh():
    """Refresh TikTok access token using stored refresh_token."""
    app_id = os.environ.get("TIKTOK_APP_ID", "").strip()
    secret = os.environ.get("TIKTOK_SECRET", "").strip()
    db_path = _db()

    _ensure_tokens_table(db_path)
    rows = sql_rows(db_path, "SELECT refresh_token, advertiser_id FROM platform_tokens WHERE platform='tiktok'")
    if not rows or not rows[0].get("refresh_token"):
        return {"error": "No refresh_token stored. Please re-connect TikTok."}

    refresh_token = str(rows[0]["refresh_token"])
    advertiser_id = str(rows[0].get("advertiser_id") or os.environ.get("TIKTOK_ADVERTISER_ID", ""))

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(
                TIKTOK_REFRESH_URL,
                json={"app_id": app_id, "secret": secret, "refresh_token": refresh_token},
                headers={"Content-Type": "application/json"},
            )
            data = resp.json()

        code = int(data.get("code") or 0)
        if code != 0:
            return {"error": f"TikTok refresh error ({code}): {data.get('message')}"}

        token_data: dict[str, Any] = data.get("data") or {}
        new_access = str(token_data.get("access_token") or "")
        new_refresh = str(token_data.get("refresh_token") or refresh_token)

        if not new_access:
            return {"error": "No access_token returned from refresh"}

        _save_tiktok_token(db_path, new_access, new_refresh, advertiser_id)
        return {"refreshed": True, "advertiser_id": advertiser_id}

    except Exception as e:
        return {"error": str(e)}


@router.get("/tiktok/status")
async def tiktok_status():
    """Return current TikTok token status."""
    db_path = _db()
    _ensure_tokens_table(db_path)
    rows = sql_rows(db_path, "SELECT access_token, refresh_token, advertiser_id, updated_at FROM platform_tokens WHERE platform='tiktok'")

    app_id = os.environ.get("TIKTOK_APP_ID", "")
    env_token = os.environ.get("TIKTOK_ACCESS_TOKEN", "")
    backend_url = os.environ.get("BACKEND_URL", "https://mini-hyros.onrender.com").rstrip("/")
    redirect_uri = f"{backend_url}/api/platform-auth/tiktok/callback"
    auth_url = f"{TIKTOK_AUTH_BASE}?app_id={app_id}&state=tiktok_oauth&redirect_uri={redirect_uri}" if app_id else ""

    if rows and rows[0].get("access_token"):
        r = rows[0]
        return {
            "connected": True,
            "source": "database",
            "advertiser_id": r.get("advertiser_id"),
            "has_refresh_token": bool(r.get("refresh_token")),
            "updated_at": r.get("updated_at"),
            "auth_url": auth_url,
        }
    if env_token:
        return {
            "connected": True,
            "source": "env",
            "advertiser_id": os.environ.get("TIKTOK_ADVERTISER_ID"),
            "has_refresh_token": False,
            "auth_url": auth_url,
        }
    return {"connected": False, "auth_url": auth_url}
