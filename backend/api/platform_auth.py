"""Platform OAuth token management.

Handles OAuth flows for ad platforms that require user authorization (TikTok),
and self-service credential updates for platforms whose tokens expire (Meta).
Tokens are stored in a platform_tokens table in the DB, which every consumer
reads before falling back to environment variables — so rotating a token never
requires a redeploy.

Endpoints:
  GET  /api/platform-auth/tiktok/connect      → returns authorization URL
  GET  /api/platform-auth/tiktok/callback     → exchanges auth_code, stores token
  POST /api/platform-auth/tiktok/refresh      → refreshes access token
  GET  /api/platform-auth/tiktok/status       → current token info
  POST /api/platform-auth/meta/connect        → validates + stores a pasted token
  POST /api/platform-auth/meta/disconnect     → drops the stored token (env wins)
  GET  /api/platform-auth/meta/status         → current token info
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, Query, Request
from fastapi.responses import RedirectResponse, HTMLResponse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from attributionops.config import default_db_path
from attributionops.db import connect, sql_rows

router = APIRouter()
UTC = timezone.utc
logger = logging.getLogger(__name__)

META_GRAPH_BASE = "https://graph.facebook.com"

TIKTOK_TOKEN_URL = "https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token/"
TIKTOK_REFRESH_URL = "https://business-api.tiktok.com/open_api/v1.3/oauth2/refresh_token/"
TIKTOK_AUTH_BASE = "https://business-api.tiktok.com/portal/auth"
DEFAULT_TIKTOK_SCOPES = "advertiser.read,campaign.read,adgroup.read,ad.read,report.read"
TIKTOK_REQUIRED_ENDPOINT_PERMISSIONS = [
    "/advertiser/info/:GET",
    "/campaign/get/:GET",
    "/adgroup/get/:GET",
    "/ad/get/:GET",
    "/report/integrated/get/:GET",
]


def _db() -> str:
    return default_db_path()


def _request_origin(request: Request) -> str:
    proto = request.headers.get("x-forwarded-proto") or request.url.scheme
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc
    return f"{proto}://{host}".rstrip("/")


def _tiktok_redirect_uri(request: Request) -> str:
    explicit = os.environ.get("TIKTOK_REDIRECT_URI", "").strip().rstrip("/")
    if explicit:
        return explicit

    backend_url = os.environ.get("BACKEND_URL", "").strip().rstrip("/")
    frontend_url = os.environ.get("FRONTEND_URL", "").strip().rstrip("/")
    if backend_url and backend_url != frontend_url and "vercel.app" not in backend_url:
        return backend_url

    return (
        os.environ.get("PUBLIC_API_URL")
        or os.environ.get("API_PUBLIC_URL")
        or _request_origin(request)
        or "https://vigil-api.vercel.app"
    )


def _dashboard_url() -> str:
    """Return a usable dashboard URL for OAuth success redirects."""
    for key in ("DASHBOARD_URL", "FRONTEND_URL"):
        value = os.environ.get(key, "").strip().rstrip("/")
        if not value:
            continue
        lowered = value.lower()
        if "your-actual-dashboard" in lowered or "placeholder" in lowered:
            continue
        if "://" not in value:
            scheme = "http" if value.startswith(("localhost", "127.0.0.1")) else "https"
            value = f"{scheme}://{value}"
        return value
    return "https://mini-hyros.vercel.app"


def _tiktok_scopes() -> str:
    """Return OAuth scopes requested from TikTok.

    TikTok apps must also have endpoint permissions enabled in the TikTok
    developer portal. Keeping this configurable avoids code changes if TikTok
    changes the public scope names again.
    """
    return os.environ.get("TIKTOK_SCOPES", DEFAULT_TIKTOK_SCOPES).strip() or DEFAULT_TIKTOK_SCOPES


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


def _meta_api_version() -> str:
    raw = str(os.environ.get("META_API_VERSION", "v18.0") or "").strip()
    return raw or "v18.0"


def _clean_ad_account_id(value: str) -> str:
    return str(value or "").strip().replace("act_", "").strip()


def _meta_token_row(db_path: str) -> dict[str, Any]:
    _ensure_tokens_table(db_path)
    rows = sql_rows(
        db_path,
        "SELECT access_token, advertiser_id, expires_at, updated_at FROM platform_tokens WHERE platform='meta'",
    )
    return dict(rows[0]) if rows else {}


def get_meta_credentials(db_path: str) -> tuple[str, str]:
    """Return (access_token, ad_account_id), preferring the DB then env vars.

    A token pasted into Settings has to win over the environment: it is the one
    the operator just rotated, while the env var is whatever the last deploy
    baked in (and is exactly the value that expired).
    """
    try:
        row = _meta_token_row(db_path)
    except Exception:
        row = {}

    token = str(row.get("access_token") or "").strip()
    account = str(row.get("advertiser_id") or "").strip()
    if not token:
        token = os.environ.get("META_ACCESS_TOKEN", "").strip()
    if not account:
        account = os.environ.get("META_AD_ACCOUNT_ID", "").strip()
    return token, account


def get_meta_access_token(db_path: str) -> str:
    """Return the best available Meta access token (DB first, env fallback)."""
    return get_meta_credentials(db_path)[0]


def save_meta_credentials(
    db_path: str, access_token: str, ad_account_id: str, expires_at: str = ""
) -> None:
    _ensure_tokens_table(db_path)
    with connect(db_path) as conn:
        conn.execute(
            """INSERT OR REPLACE INTO platform_tokens
               (platform, access_token, refresh_token, advertiser_id, expires_at, updated_at)
               VALUES ('meta', ?, '', ?, ?, ?)""",
            (access_token.strip(), _clean_ad_account_id(ad_account_id), expires_at, _now()),
        )
        conn.commit()


def clear_meta_credentials(db_path: str) -> None:
    _ensure_tokens_table(db_path)
    with connect(db_path) as conn:
        conn.execute("DELETE FROM platform_tokens WHERE platform='meta'")
        conn.commit()


def get_tiktok_token(db_path: str) -> str:
    """Return best available TikTok access token (DB first, env fallback)."""
    _ensure_tokens_table(db_path)
    rows = sql_rows(db_path, "SELECT access_token FROM platform_tokens WHERE platform='tiktok'")
    if rows and rows[0].get("access_token"):
        return str(rows[0]["access_token"])
    return os.environ.get("TIKTOK_ACCESS_TOKEN", "")


async def get_or_refresh_tiktok_token(db_path: str) -> str:
    """Return a valid TikTok access token, auto-refreshing if expired or expiring soon.
    Falls back to env token if no DB token exists. Re-auth is only needed once per year."""
    _ensure_tokens_table(db_path)
    rows = sql_rows(
        db_path,
        "SELECT access_token, refresh_token, advertiser_id, expires_at FROM platform_tokens WHERE platform='tiktok'",
    )

    if rows and rows[0].get("access_token"):
        r = rows[0]
        access_token = str(r["access_token"])
        refresh_token = str(r.get("refresh_token") or "")
        advertiser_id = str(r.get("advertiser_id") or os.environ.get("TIKTOK_ADVERTISER_ID", ""))
        expires_at_str = str(r.get("expires_at") or "")

        # Check if token is expired or expiring within the next hour. When the
        # stored expiry is missing or unparseable but we hold a refresh_token,
        # refresh proactively rather than risk using a possibly-stale token.
        should_refresh = False
        if expires_at_str:
            try:
                expires_at = datetime.fromisoformat(expires_at_str.replace("Z", "+00:00"))
                should_refresh = expires_at <= datetime.now(UTC) + timedelta(hours=1)
            except Exception:
                should_refresh = bool(refresh_token)
        else:
            should_refresh = bool(refresh_token)

        if should_refresh and refresh_token:
            app_id = os.environ.get("TIKTOK_APP_ID", "").strip()
            secret = os.environ.get("TIKTOK_SECRET", "").strip()
            try:
                async with httpx.AsyncClient(timeout=20) as client:
                    resp = await client.post(
                        TIKTOK_REFRESH_URL,
                        json={"app_id": app_id, "secret": secret, "refresh_token": refresh_token},
                        headers={"Content-Type": "application/json"},
                    )
                    data = resp.json()
                if int(data.get("code") or 0) == 0:
                    token_data = data.get("data") or {}
                    new_access = str(token_data.get("access_token") or "")
                    new_refresh = str(token_data.get("refresh_token") or refresh_token)
                    expires_at = _tiktok_expires_at(token_data)
                    if new_access:
                        _save_tiktok_token(db_path, new_access, new_refresh, advertiser_id, expires_at)
                        return new_access
                else:
                    logger.warning(
                        "TikTok token refresh returned error code %s: %s",
                        data.get("code"),
                        data.get("message"),
                    )
            except Exception as exc:
                # Fall through to return current token, but do not swallow silently.
                logger.warning("TikTok token refresh failed: %s", exc)

        return access_token

    # No DB token — fall back to env
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


def _tiktok_expires_at(token_data: dict[str, Any]) -> str:
    """Return an ISO expiry timestamp from TikTok token response metadata."""
    for key in ("expires_in", "access_token_expire_in", "access_token_expires_in"):
        raw = token_data.get(key)
        try:
            seconds = int(raw)
        except (TypeError, ValueError):
            continue
        if seconds > 0:
            return (datetime.now(UTC) + timedelta(seconds=seconds)).isoformat()
    return ""


def _meta_error_text(payload: Any, fallback: str) -> str:
    error = payload.get("error") if isinstance(payload, dict) else None
    if not isinstance(error, dict):
        return fallback
    message = str(error.get("message") or "").strip()
    subcode = error.get("error_subcode")
    code = error.get("code")
    bits = [bit for bit in (f"code={code}" if code is not None else "",
                            f"subcode={subcode}" if subcode is not None else "") if bit]
    if message and bits:
        return f"{message} ({' '.join(bits)})"
    return message or fallback


async def _exchange_meta_long_lived(client: httpx.AsyncClient, token: str) -> tuple[str, str, str]:
    """Trade a short-lived token for a ~60-day one.

    Returns (token, expires_at, note). Falls back to the original token — a
    short-lived token that works today beats refusing to connect at all.
    """
    app_id = os.environ.get("META_APP_ID", "").strip()
    app_secret = os.environ.get("META_APP_SECRET", "").strip()
    if not app_id or not app_secret:
        return token, "", (
            "Set META_APP_ID and META_APP_SECRET to auto-upgrade pasted tokens to "
            "long-lived (60-day) ones."
        )

    try:
        resp = await client.get(
            f"{META_GRAPH_BASE}/{_meta_api_version()}/oauth/access_token",
            params={
                "grant_type": "fb_exchange_token",
                "client_id": app_id,
                "client_secret": app_secret,
                "fb_exchange_token": token,
            },
        )
        payload = resp.json()
    except Exception as exc:
        return token, "", f"Long-lived token exchange failed ({exc}); stored the token as pasted."

    long_lived = str((payload or {}).get("access_token") or "").strip()
    if not long_lived:
        detail = _meta_error_text(payload, "no access_token returned")
        return token, "", f"Long-lived token exchange failed ({detail}); stored the token as pasted."

    expires_at = ""
    try:
        seconds = int((payload or {}).get("expires_in") or 0)
        if seconds > 0:
            expires_at = (datetime.now(UTC) + timedelta(seconds=seconds)).replace(microsecond=0).isoformat()
    except (TypeError, ValueError):
        expires_at = ""
    return long_lived, expires_at, ""


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/meta/connect")
async def meta_connect(request: Request):
    """Validate and store a Meta access token pasted from the dashboard.

    Meta tokens die on password changes and security resets (OAuth subcode 460),
    which used to mean editing a hosting env var and redeploying just to resume
    spend syncing.
    """
    try:
        body: dict[str, Any] = await request.json()
    except Exception:
        body = {}

    token = str(body.get("access_token") or body.get("token") or "").strip()
    if not token:
        return {"connected": False, "error": "An access token is required."}

    db_path = _db()
    _, current_account = get_meta_credentials(db_path)
    ad_account_id = _clean_ad_account_id(
        body.get("ad_account_id") or body.get("adAccountId") or current_account
    )

    api_version = _meta_api_version()
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(
                f"{META_GRAPH_BASE}/{api_version}/me",
                params={"fields": "id,name", "access_token": token},
            )
            payload = resp.json()
            if resp.status_code != 200 or not (payload or {}).get("id"):
                return {
                    "connected": False,
                    "error": _meta_error_text(payload, f"Meta rejected the token (HTTP {resp.status_code})."),
                }
            account_name = str((payload or {}).get("name") or "")

            token, expires_at, note = await _exchange_meta_long_lived(client, token)

            # A token can be valid and still not see the ad account we sync.
            account_detail = ""
            if ad_account_id:
                acct_resp = await client.get(
                    f"{META_GRAPH_BASE}/{api_version}/act_{ad_account_id}",
                    params={"fields": "name,currency,account_status", "access_token": token},
                )
                acct_payload = acct_resp.json()
                if acct_resp.status_code != 200:
                    return {
                        "connected": False,
                        "error": _meta_error_text(
                            acct_payload,
                            f"Token cannot read ad account act_{ad_account_id} (HTTP {acct_resp.status_code}).",
                        ),
                    }
                account_detail = str((acct_payload or {}).get("name") or "")
    except Exception as exc:
        return {"connected": False, "error": f"Could not reach the Meta API: {exc}"}

    save_meta_credentials(db_path, token, ad_account_id, expires_at)
    return {
        "connected": True,
        "user": account_name,
        "ad_account_id": ad_account_id,
        "ad_account_name": account_detail,
        "expires_at": expires_at,
        "note": note,
    }


@router.post("/meta/disconnect")
async def meta_disconnect():
    """Forget the stored Meta token and fall back to the environment."""
    clear_meta_credentials(_db())
    token, ad_account_id = get_meta_credentials(_db())
    return {"connected": False, "env_fallback": bool(token and ad_account_id)}


@router.get("/meta/status")
async def meta_status():
    """Return where the active Meta credentials come from and when they expire."""
    db_path = _db()
    try:
        row = _meta_token_row(db_path)
    except Exception:
        row = {}
    token, ad_account_id = get_meta_credentials(db_path)
    return {
        "connected": bool(token and ad_account_id),
        "source": "database" if row.get("access_token") else ("env" if token else "none"),
        "ad_account_id": _clean_ad_account_id(ad_account_id),
        "expires_at": str(row.get("expires_at") or ""),
        "updated_at": str(row.get("updated_at") or ""),
        "can_exchange_long_lived": bool(
            os.environ.get("META_APP_ID", "").strip() and os.environ.get("META_APP_SECRET", "").strip()
        ),
        "token_help_url": "https://developers.facebook.com/tools/explorer/",
    }


@router.get("/tiktok/connect")
async def tiktok_connect(request: Request):
    """Return the TikTok authorization URL for the user to visit."""
    app_id = os.environ.get("TIKTOK_APP_ID", "").strip()
    if not app_id:
        return {"error": "TIKTOK_APP_ID not set in environment"}

    # Use base URL as redirect_uri — matches TikTok's registered URL.
    # The root GET / handler forwards auth_code to /api/platform-auth/tiktok/callback.
    redirect_uri = _tiktok_redirect_uri(request)

    from urllib.parse import quote
    scopes = _tiktok_scopes()
    auth_url = (
        f"{TIKTOK_AUTH_BASE}"
        f"?app_id={app_id}"
        f"&state=tiktok_oauth"
        f"&redirect_uri={quote(redirect_uri, safe='')}"
        f"&scope={quote(scopes, safe='')}"
    )
    return {
        "auth_url": auth_url,
        "url": auth_url,
        "app_id": app_id,
        "redirect_uri": redirect_uri,
        "scopes": scopes,
        "required_endpoint_permissions": TIKTOK_REQUIRED_ENDPOINT_PERMISSIONS,
    }


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
        expires_at = _tiktok_expires_at(token_data)
        advertiser_ids: list = token_data.get("advertiser_ids") or []
        advertiser_id = str(advertiser_ids[0]) if advertiser_ids else os.environ.get("TIKTOK_ADVERTISER_ID", "")

        if not access_token:
            return HTMLResponse("<h3>No access_token in TikTok response.</h3>", status_code=400)

        _save_tiktok_token(_db(), access_token, refresh_token, advertiser_id, expires_at)

        return RedirectResponse(url=f"{_dashboard_url()}?tiktok_connected=1")

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
        expires_at = _tiktok_expires_at(token_data)

        if not new_access:
            return {"error": "No access_token returned from refresh"}

        _save_tiktok_token(db_path, new_access, new_refresh, advertiser_id, expires_at)
        return {"refreshed": True, "advertiser_id": advertiser_id}

    except Exception as e:
        return {"error": str(e)}


@router.get("/tiktok/status")
async def tiktok_status(request: Request):
    """Return current TikTok token status."""
    db_path = _db()
    _ensure_tokens_table(db_path)
    rows = sql_rows(db_path, "SELECT access_token, refresh_token, advertiser_id, updated_at FROM platform_tokens WHERE platform='tiktok'")

    app_id = os.environ.get("TIKTOK_APP_ID", "")
    env_token = os.environ.get("TIKTOK_ACCESS_TOKEN", "")
    redirect_uri = _tiktok_redirect_uri(request)
    from urllib.parse import quote
    scopes = _tiktok_scopes()
    auth_url = (
        f"{TIKTOK_AUTH_BASE}"
        f"?app_id={app_id}"
        f"&state=tiktok_oauth"
        f"&redirect_uri={quote(redirect_uri, safe='')}"
        f"&scope={quote(scopes, safe='')}"
    ) if app_id else ""

    if rows and rows[0].get("access_token"):
        r = rows[0]
        return {
            "connected": True,
            "source": "database",
            "advertiser_id": r.get("advertiser_id"),
            "has_refresh_token": bool(r.get("refresh_token")),
            "updated_at": r.get("updated_at"),
            "auth_url": auth_url,
            "url": auth_url,
            "scopes": scopes,
            "required_endpoint_permissions": TIKTOK_REQUIRED_ENDPOINT_PERMISSIONS,
        }
    if env_token:
        return {
            "connected": True,
            "source": "env",
            "advertiser_id": os.environ.get("TIKTOK_ADVERTISER_ID"),
            "has_refresh_token": False,
            "auth_url": auth_url,
            "url": auth_url,
            "scopes": scopes,
            "required_endpoint_permissions": TIKTOK_REQUIRED_ENDPOINT_PERMISSIONS,
        }
    return {
        "connected": False,
        "auth_url": auth_url,
        "url": auth_url,
        "scopes": scopes,
        "required_endpoint_permissions": TIKTOK_REQUIRED_ENDPOINT_PERMISSIONS,
    }
