"""Conversion API Pushback — send tracked conversions back to ad platforms.

Endpoints:
  POST /api/capi/push          — manually push a conversion to a platform
  POST /api/capi/auto-sync     — auto-push all un-synced conversions
  GET  /api/capi/status         — check sync status per platform
  GET  /api/capi/log            — view push history

Supported platforms:
  - Meta (Facebook) Conversions API
  - Google Ads Offline Conversions
  - TikTok Events API
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time as _time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, Query, HTTPException

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from attributionops.config import default_db_path
from attributionops.db import connect, sql_rows as db_query

router = APIRouter()
UTC = timezone.utc


def _db() -> str:
    return os.environ.get("ATTRIBUTIONOPS_DB_PATH", default_db_path())


def _iso_ts(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _ensure_capi_log_table(db_path: str) -> None:
    with connect(db_path) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS capi_log (
            id TEXT PRIMARY KEY,
            ts TEXT,
            platform TEXT,
            event_name TEXT,
            customer_key TEXT,
            order_id TEXT,
            value TEXT,
            status TEXT,
            response TEXT,
            error TEXT
        )""")
        conn.commit()


def _log_push(db_path: str, entry: dict) -> None:
    _ensure_capi_log_table(db_path)
    with connect(db_path) as conn:
        conn.execute(
            """INSERT OR REPLACE INTO capi_log (id, ts, platform, event_name, customer_key, order_id, value, status, response, error)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                entry["id"], entry["ts"], entry["platform"], entry["event_name"],
                entry.get("customer_key", ""), entry.get("order_id", ""),
                str(entry.get("value", 0)), entry["status"],
                entry.get("response", ""), entry.get("error", ""),
            ),
        )
        conn.commit()


# ── Meta Conversions API ──────────────────────────────────────────────────────

async def _push_meta(event: dict) -> dict:
    """Push conversion to Meta (Facebook) Conversions API."""
    access_token = os.environ.get("META_ACCESS_TOKEN", "")
    pixel_id = os.environ.get("META_PIXEL_ID", "")

    if not access_token or not pixel_id:
        return {"ok": False, "error": "META_ACCESS_TOKEN and META_PIXEL_ID env vars required"}

    # Hash PII for Meta (they require SHA256 hashing)
    customer_key = event.get("customer_key", "")
    email_hash = customer_key if len(customer_key) == 64 else _sha256(customer_key)

    payload = {
        "data": [{
            "event_name": event.get("event_name", "Purchase"),
            "event_time": int(datetime.fromisoformat(event["ts"].replace("Z", "+00:00")).timestamp()),
            "event_source_url": event.get("landing_page", ""),
            "action_source": "website",
            "user_data": {
                "em": [email_hash],
                "client_ip_address": event.get("ip", ""),
                "client_user_agent": event.get("user_agent", ""),
                "fbp": event.get("fbclid", ""),
                "fbc": event.get("fbc", ""),
            },
            "custom_data": {
                "currency": event.get("currency", "USD"),
                "value": float(event.get("value", 0)),
                "order_id": event.get("order_id", ""),
                "content_type": "product",
            },
        }],
        "access_token": access_token,
    }

    # Remove empty user_data fields
    payload["data"][0]["user_data"] = {
        k: v for k, v in payload["data"][0]["user_data"].items() if v
    }

    url = f"https://graph.facebook.com/v18.0/{pixel_id}/events"

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url, json=payload)
        result = resp.json()

    return {
        "ok": resp.status_code == 200,
        "status_code": resp.status_code,
        "response": result,
        "events_received": result.get("events_received", 0),
    }


# ── Google Ads Offline Conversions ────────────────────────────────────────────

async def _push_google(event: dict) -> dict:
    """Push conversion to Google Ads Offline Conversions API."""
    developer_token = os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN", "")
    customer_id = os.environ.get("GOOGLE_ADS_CUSTOMER_ID", "")
    conversion_action = os.environ.get("GOOGLE_ADS_CONVERSION_ACTION", "")
    access_token = os.environ.get("GOOGLE_ADS_ACCESS_TOKEN", "")

    if not all([developer_token, customer_id, conversion_action, access_token]):
        return {"ok": False, "error": "Google Ads env vars required: GOOGLE_ADS_DEVELOPER_TOKEN, GOOGLE_ADS_CUSTOMER_ID, GOOGLE_ADS_CONVERSION_ACTION, GOOGLE_ADS_ACCESS_TOKEN"}

    gclid = event.get("gclid", "")
    if not gclid:
        return {"ok": False, "error": "No gclid available for this conversion"}

    payload = {
        "conversions": [{
            "conversionAction": f"customers/{customer_id}/conversionActions/{conversion_action}",
            "conversionDateTime": event["ts"].replace("T", " ").replace("Z", "+00:00"),
            "conversionValue": float(event.get("value", 0)),
            "currencyCode": event.get("currency", "USD"),
            "orderId": event.get("order_id", ""),
            "gclid": gclid,
        }],
        "partialFailure": True,
    }

    url = f"https://googleads.googleapis.com/v15/customers/{customer_id}:uploadClickConversions"

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            url,
            json=payload,
            headers={
                "Authorization": f"Bearer {access_token}",
                "developer-token": developer_token,
                "Content-Type": "application/json",
            },
        )
        result = resp.json()

    return {
        "ok": resp.status_code == 200,
        "status_code": resp.status_code,
        "response": result,
    }


# ── TikTok Events API ────────────────────────────────────────────────────────

async def _push_tiktok(event: dict) -> dict:
    """Push conversion to TikTok Events API."""
    access_token = os.environ.get("TIKTOK_ACCESS_TOKEN", "")
    pixel_id = os.environ.get("TIKTOK_PIXEL_ID", "")

    if not access_token or not pixel_id:
        return {"ok": False, "error": "TIKTOK_ACCESS_TOKEN and TIKTOK_PIXEL_ID env vars required"}

    customer_key = event.get("customer_key", "")
    email_hash = customer_key if len(customer_key) == 64 else _sha256(customer_key)

    payload = {
        "pixel_code": pixel_id,
        "event": event.get("event_name", "CompletePayment"),
        "event_id": event.get("order_id", ""),
        "timestamp": event["ts"],
        "context": {
            "user": {
                "email": email_hash,
                "ttclid": event.get("ttclid", ""),
            },
            "page": {
                "url": event.get("landing_page", ""),
            },
            "ip": event.get("ip", ""),
            "user_agent": event.get("user_agent", ""),
        },
        "properties": {
            "currency": event.get("currency", "USD"),
            "value": float(event.get("value", 0)),
            "contents": [{"content_type": "product", "content_id": event.get("order_id", "")}],
        },
    }

    url = "https://business-api.tiktok.com/open_api/v1.3/pixel/track/"

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            url,
            json=payload,
            headers={
                "Access-Token": access_token,
                "Content-Type": "application/json",
            },
        )
        result = resp.json()

    return {
        "ok": resp.status_code == 200 and result.get("code") == 0,
        "status_code": resp.status_code,
        "response": result,
    }


# ── Platform dispatcher ──────────────────────────────────────────────────────

PLATFORM_PUSHERS = {
    "meta": _push_meta,
    "facebook": _push_meta,
    "google": _push_google,
    "tiktok": _push_tiktok,
}


# ── API Endpoints ─────────────────────────────────────────────────────────────

@router.post("/push")
async def push_conversion(event: dict):
    """Manually push a single conversion to a platform's API.
    
    Body: { platform, event_name, ts, customer_key, order_id, value, gclid?, fbclid?, ttclid?, ... }
    """
    platform = (event.get("platform") or "").lower()
    pusher = PLATFORM_PUSHERS.get(platform)
    if not pusher:
        raise HTTPException(400, f"Unsupported platform: {platform}. Use: meta, google, tiktok")

    db_path = _db()
    now = _iso_ts(datetime.now(UTC))
    log_id = _sha256(f"capi|{platform}|{event.get('order_id','')}|{now}")[:32]

    try:
        result = await pusher(event)
        _log_push(db_path, {
            "id": log_id, "ts": now, "platform": platform,
            "event_name": event.get("event_name", "Purchase"),
            "customer_key": event.get("customer_key", ""),
            "order_id": event.get("order_id", ""),
            "value": event.get("value", 0),
            "status": "success" if result.get("ok") else "failed",
            "response": json.dumps(result.get("response", {}))[:500],
            "error": result.get("error", ""),
        })
        return result
    except Exception as e:
        _log_push(db_path, {
            "id": log_id, "ts": now, "platform": platform,
            "event_name": event.get("event_name", "Purchase"),
            "customer_key": event.get("customer_key", ""),
            "order_id": event.get("order_id", ""),
            "value": event.get("value", 0),
            "status": "error",
            "response": "", "error": str(e),
        })
        return {"ok": False, "error": str(e)}


@router.post("/auto-sync")
async def auto_sync_conversions():
    """Find all un-synced conversions and push them to the appropriate platform APIs."""
    db_path = _db()
    _ensure_capi_log_table(db_path)

    # Get all conversions that haven't been pushed yet
    already_pushed = set()
    try:
        pushed_rows = db_query(db_path, "SELECT order_id FROM capi_log WHERE status = 'success'")
        already_pushed = {r["order_id"] for r in pushed_rows}
    except Exception:
        pass

    # Get conversions with their touchpoint data
    conversions = db_query(db_path, """
        SELECT c.conversion_id, c.ts, c.type, c.value, c.order_id, c.customer_key,
               t.platform, t.gclid, t.fbclid, t.ttclid, t.campaign_id, t.session_id
        FROM conversions c
        LEFT JOIN touchpoints t ON c.customer_key = t.customer_key
        WHERE c.order_id NOT IN ({})
        GROUP BY c.order_id
        ORDER BY c.ts DESC
        LIMIT 100
    """.format(",".join(f"'{oid}'" for oid in already_pushed) if already_pushed else "'__none__'"))

    results = {"total": 0, "pushed": 0, "failed": 0, "skipped": 0, "details": []}

    for conv in conversions:
        results["total"] += 1
        platform = (conv.get("platform") or "").lower()

        if not platform or platform not in PLATFORM_PUSHERS:
            results["skipped"] += 1
            continue

        event = {
            "platform": platform,
            "event_name": "Purchase" if conv.get("type") == "Purchase" else conv.get("type", "Lead"),
            "ts": conv["ts"],
            "customer_key": conv.get("customer_key", ""),
            "order_id": conv.get("order_id", ""),
            "value": float(conv.get("value", 0)),
            "gclid": conv.get("gclid", ""),
            "fbclid": conv.get("fbclid", ""),
            "ttclid": conv.get("ttclid", ""),
        }

        try:
            result = await PLATFORM_PUSHERS[platform](event)
            now = _iso_ts(datetime.now(UTC))
            log_id = _sha256(f"autosync|{platform}|{conv['order_id']}|{now}")[:32]
            status = "success" if result.get("ok") else "failed"
            _log_push(db_path, {
                "id": log_id, "ts": now, "platform": platform,
                "event_name": event["event_name"],
                "customer_key": event["customer_key"],
                "order_id": event["order_id"],
                "value": event["value"],
                "status": status,
                "response": json.dumps(result.get("response", {}))[:500],
                "error": result.get("error", ""),
            })
            if result.get("ok"):
                results["pushed"] += 1
            else:
                results["failed"] += 1
            results["details"].append({"order_id": conv["order_id"], "platform": platform, "status": status})
        except Exception as e:
            results["failed"] += 1
            results["details"].append({"order_id": conv["order_id"], "platform": platform, "status": "error", "error": str(e)})

    return results


@router.get("/status")
async def capi_status():
    """Check which platforms have API credentials configured and sync stats."""
    db_path = _db()
    _ensure_capi_log_table(db_path)

    platforms = {
        "meta": {
            "configured": bool(os.environ.get("META_ACCESS_TOKEN") and os.environ.get("META_PIXEL_ID")),
            "env_vars": ["META_ACCESS_TOKEN", "META_PIXEL_ID"],
        },
        "google": {
            "configured": bool(os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN") and os.environ.get("GOOGLE_ADS_CUSTOMER_ID")),
            "env_vars": ["GOOGLE_ADS_DEVELOPER_TOKEN", "GOOGLE_ADS_CUSTOMER_ID", "GOOGLE_ADS_CONVERSION_ACTION", "GOOGLE_ADS_ACCESS_TOKEN"],
        },
        "tiktok": {
            "configured": bool(os.environ.get("TIKTOK_ACCESS_TOKEN") and os.environ.get("TIKTOK_PIXEL_ID")),
            "env_vars": ["TIKTOK_ACCESS_TOKEN", "TIKTOK_PIXEL_ID"],
        },
    }

    # Get sync stats
    try:
        stats = db_query(db_path,
            "SELECT platform, status, COUNT(*) as cnt FROM capi_log GROUP BY platform, status")
        for row in stats:
            p = row["platform"]
            if p in platforms:
                if "stats" not in platforms[p]:
                    platforms[p]["stats"] = {}
                platforms[p]["stats"][row["status"]] = int(row["cnt"])
    except Exception:
        pass

    # Count un-synced conversions
    try:
        total_convs = db_query(db_path, "SELECT COUNT(*) as cnt FROM conversions")[0]["cnt"]
        synced = db_query(db_path, "SELECT COUNT(DISTINCT order_id) as cnt FROM capi_log WHERE status='success'")[0]["cnt"]
    except Exception:
        total_convs = 0
        synced = 0

    return {
        "platforms": platforms,
        "total_conversions": int(total_convs),
        "synced": int(synced),
        "unsynced": int(total_convs) - int(synced),
    }


@router.get("/log")
async def capi_log(
    platform: str = Query(default=""),
    limit: int = Query(default=50),
):
    """View CAPI push history."""
    db_path = _db()
    _ensure_capi_log_table(db_path)

    sql = "SELECT * FROM capi_log"
    params = []
    if platform:
        sql += " WHERE platform = ?"
        params.append(platform)
    sql += " ORDER BY ts DESC LIMIT ?"
    params.append(limit)

    try:
        rows = db_query(db_path, sql, params)
    except Exception:
        rows = []

    return {"rows": rows, "count": len(rows)}
