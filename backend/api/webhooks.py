from __future__ import annotations

import hashlib
import hmac
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Request, HTTPException

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from attributionops.config import default_db_path
from attributionops.db import connect

router = APIRouter()

UTC = timezone.utc


def _db() -> str:
    return os.environ.get("ATTRIBUTIONOPS_DB_PATH", default_db_path())


def _iso_ts(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def _meta_campaign_from_adset(db_path: str, adset_id: str) -> str:
    """Resolve Meta campaign_id from adset_id using ad_names parent mapping."""
    adset_id = str(adset_id or "").strip()
    if not adset_id:
        return ""

    try:
        with connect(db_path) as conn:
            row = conn.execute(
                """SELECT parent_id FROM ad_names
                   WHERE platform = 'meta' AND entity_type = 'adset' AND entity_id = ?
                   LIMIT 1""",
                (adset_id,),
            ).fetchone()
            return str(row[0] if row and row[0] else "").strip()
    except sqlite3.Error:
        return ""


def _insert_order(db_path: str, order: dict[str, Any]) -> None:
    with connect(db_path) as conn:
        conn.execute(
            """INSERT OR IGNORE INTO orders (order_id, ts, gross, net, refunds, chargebacks, cogs, fees, customer_key, subscription_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                str(order["order_id"]),
                str(order["ts"]),
                str(order["gross"]),
                str(order["net"]),
                str(order.get("refunds", 0)),
                str(order.get("chargebacks", 0)),
                str(order.get("cogs", 0)),
                str(order.get("fees", 0)),
                str(order.get("customer_key", "")),
                str(order.get("subscription_id", "")),
            ),
        )
        conn.execute(
            """INSERT OR IGNORE INTO conversions (conversion_id, ts, type, value, order_id, customer_key)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                _sha256(f"conv|{order['order_id']}"),
                str(order["ts"]),
                "Purchase",
                str(order["gross"]),
                str(order["order_id"]),
                str(order.get("customer_key", "")),
            ),
        )
        conn.commit()


# ── Shopify webhook ────────────────────────────────────────────────────────────

@router.post("/shopify")
async def shopify_webhook(request: Request):
    """Receive Shopify order/paid webhook."""
    secret = os.environ.get("SHOPIFY_WEBHOOK_SECRET", "")
    body = await request.body()

    if secret:
        hmac_header = request.headers.get("X-Shopify-Hmac-Sha256", "")
        digest = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(digest, hmac_header):
            raise HTTPException(status_code=401, detail="Invalid HMAC")

    payload = json.loads(body)
    email = str(payload.get("email") or payload.get("customer", {}).get("email", ""))
    customer_key = _sha256(email) if email else ""

    gross = float(payload.get("total_price", 0))
    refunds_total = sum(
        float(r.get("transactions", [{}])[0].get("amount", 0))
        for r in payload.get("refunds", [])
        if r.get("transactions")
    )
    net = max(0.0, gross - refunds_total)

    order = {
        "order_id": str(payload.get("id", "")),
        "ts": payload.get("created_at", _iso_ts(datetime.now(UTC))),
        "gross": round(gross, 2),
        "net": round(net, 2),
        "refunds": round(refunds_total, 2),
        "chargebacks": 0,
        "cogs": 0,
        "fees": round(gross * 0.029 + 0.30, 2),  # Shopify payments estimate
        "customer_key": customer_key,
        "subscription_id": "",
    }

    _insert_order(_db(), order)

    # Broadcast to WebSocket clients
    from main import manager
    import asyncio
    asyncio.create_task(manager.broadcast({
        "type": "new_order",
        "order_id": order["order_id"],
        "gross": order["gross"],
        "customer_key": customer_key,
        "ts": order["ts"],
    }))

    return {"ok": True, "order_id": order["order_id"]}


# ── Stripe webhook ─────────────────────────────────────────────────────────────

@router.post("/stripe")
async def stripe_webhook(request: Request):
    """Receive Stripe checkout.session.completed or charge.succeeded webhook."""
    secret = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
    body = await request.body()

    if secret:
        sig = request.headers.get("Stripe-Signature", "")
        # In production, use stripe.Webhook.construct_event(body, sig, secret)
        # For now we just log the warning
        if not sig:
            raise HTTPException(status_code=401, detail="Missing Stripe-Signature")

    payload = json.loads(body)
    event_type = payload.get("type", "")

    if event_type not in ("checkout.session.completed", "charge.succeeded", "payment_intent.succeeded"):
        return {"ok": True, "skipped": True, "event_type": event_type}

    data = payload.get("data", {}).get("object", {})
    email = str(data.get("customer_email") or data.get("receipt_email") or "")
    customer_key = _sha256(email) if email else ""

    amount = float(data.get("amount_total", data.get("amount", 0))) / 100.0  # cents → dollars

    order = {
        "order_id": str(data.get("id") or data.get("payment_intent", "")),
        "ts": _iso_ts(datetime.now(UTC)),
        "gross": round(amount, 2),
        "net": round(amount, 2),
        "refunds": 0,
        "chargebacks": 0,
        "cogs": 0,
        "fees": round(amount * 0.029 + 0.30, 2),  # Stripe fee estimate
        "customer_key": customer_key,
        "subscription_id": str(data.get("subscription", "") or ""),
    }

    _insert_order(_db(), order)

    from main import manager
    import asyncio
    asyncio.create_task(manager.broadcast({
        "type": "new_order",
        "order_id": order["order_id"],
        "gross": order["gross"],
        "customer_key": customer_key,
        "ts": order["ts"],
    }))

    return {"ok": True, "order_id": order["order_id"]}


# ── Tracking pixel endpoint ───────────────────────────────────────────────────

@router.post("/track")
async def track_event(request: Request):
    """Generic tracking pixel / JS beacon endpoint.
    Expects JSON: { session_id, customer_key?, utm_source, utm_medium, utm_campaign,
                    utm_content?, gclid?, fbclid?, ttclid?, landing_page?, device? }
    """
    payload = await request.json()
    db_path = _db()
    now = _iso_ts(datetime.now(UTC))

    session_id = str(payload.get("session_id", _sha256(now)))
    customer_key = str(payload.get("customer_key", "")).strip().lower()
    custom_data = payload.get("custom_data") if isinstance(payload.get("custom_data"), dict) else {}
    if not customer_key and custom_data.get("email"):
        customer_key = str(custom_data.get("email", "")).strip().lower()
    if "@" in customer_key:
        customer_key = _sha256(customer_key)
    utm_source = str(payload.get("utm_source", ""))
    utm_medium = str(payload.get("utm_medium", ""))

    # Determine platform from UTM or custom params
    detected_platform = str(payload.get("detected_platform", ""))
    fbc_id = str(payload.get("fbc_id", ""))
    ttc_id = str(payload.get("ttc_id", ""))
    gc_id = str(payload.get("gc_id", ""))
    g_special_campaign = str(payload.get("g_special_campaign", ""))

    platform = ""
    channel = "organic"
    if detected_platform == "meta" or fbc_id or utm_source in ("facebook", "fb", "meta", "ig", "instagram"):
        platform = "meta"
        channel = "paid_social"
    elif detected_platform == "tiktok" or ttc_id or utm_source in ("tiktok", "tt"):
        platform = "tiktok"
        channel = "paid_social"
    elif detected_platform == "google" or gc_id or g_special_campaign or utm_source in ("google", "gads"):
        platform = "google"
        channel = "paid_search"
    elif utm_medium == "email":
        channel = "email"

    # Map custom params to standard fields (backend-side fallback)
    h_ad_id = str(payload.get("h_ad_id", ""))
    adset_id = str(payload.get("adset_id", "")) or fbc_id
    ad_id = str(payload.get("ad_id", ""))
    campaign_id = str(payload.get("campaign_id", "")) or str(payload.get("utm_campaign", "")) or gc_id
    if h_ad_id:
        if platform == "meta":
            ad_id = ad_id or h_ad_id
        elif platform == "tiktok":
            campaign_id = campaign_id or h_ad_id
        else:
            ad_id = ad_id or h_ad_id

    if platform == "meta" and not campaign_id and adset_id:
        campaign_id = _meta_campaign_from_adset(db_path, adset_id)

    with connect(db_path) as conn:
        conn.execute(
            """INSERT INTO sessions (session_id, ts, utm_source, utm_medium, utm_campaign, utm_content, utm_term, referrer, landing_page, device, gclid, fbclid, ttclid, customer_key)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                session_id,
                now,
                utm_source,
                utm_medium,
                campaign_id,
                str(payload.get("utm_content", "")),
                str(payload.get("utm_term", "")),
                str(payload.get("referrer", "")),
                str(payload.get("landing_page", "/")),
                str(payload.get("device", "unknown")),
                str(payload.get("gclid", "")),
                str(payload.get("fbclid", "")),
                str(payload.get("ttclid", "")),
                customer_key,
            ),
        )
        conn.execute(
            """INSERT INTO touchpoints (ts, channel, platform, campaign_id, adset_id, ad_id, creative_id, gclid, fbclid, ttclid, customer_key, session_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                now,
                channel,
                platform,
                campaign_id,
                adset_id,
                ad_id,
                str(payload.get("creative_id", "")),
                str(payload.get("gclid", "")),
                str(payload.get("fbclid", "")),
                str(payload.get("ttclid", "")),
                customer_key,
                session_id,
            ),
        )
        conn.commit()

    from main import manager
    import asyncio
    event_name = str(payload.get("event", ""))
    if event_name == "FormSubmit":
        asyncio.create_task(manager.broadcast({
            "type": "new_lead",
            "form_name": str(custom_data.get("form_name", "")),
            "landing_page": str(payload.get("landing_page", "/")),
            "utm_source": utm_source,
            "customer_key": customer_key,
            "ts": now,
            "source": "pixel",
        }))
    elif event_name == "BookingConfirmed":
        asyncio.create_task(manager.broadcast({
            "type": "new_booking",
            "calendar": str(custom_data.get("calendar", "")),
            "landing_page": str(payload.get("landing_page", "/")),
            "utm_source": utm_source,
            "customer_key": customer_key,
            "ts": now,
            "source": "pixel",
        }))
    else:
        msg = {
            "type": "new_session",
            "session_id": session_id,
            "utm_source": utm_source,
            "landing_page": str(payload.get("landing_page", "/")),
            "ts": now,
        }
        if event_name and event_name != "pageview":
            msg["event"] = event_name
        asyncio.create_task(manager.broadcast(msg))

    return {"ok": True, "session_id": session_id}


# ── Identity resolution ───────────────────────────────────────────────────────

@router.post("/identify")
async def identify_visitor(request: Request):
    """Link a visitor_id to a customer email (identity stitching).
    Called by hyros.identify(email) from the tracking pixel.
    This updates all prior sessions/touchpoints for that visitor with the customer_key.
    """
    payload = await request.json()
    email = str(payload.get("email", "")).strip().lower()
    visitor_id = str(payload.get("visitor_id", ""))
    session_id = str(payload.get("session_id", ""))

    if not email:
        return {"ok": False, "error": "email is required"}

    customer_key = _sha256(email)
    db_path = _db()

    with connect(db_path) as conn:
        # Back-fill all sessions from this visitor that had no customer_key
        conn.execute(
            """UPDATE sessions SET customer_key = ?
               WHERE customer_key = '' AND session_id IN (
                   SELECT session_id FROM sessions WHERE session_id = ?
                   UNION
                   SELECT session_id FROM sessions
                   WHERE customer_key = '' AND session_id IN (
                       SELECT session_id FROM touchpoints WHERE customer_key = ''
                   )
               )""",
            (customer_key, session_id),
        )
        # Back-fill touchpoints for this visitor
        conn.execute(
            """UPDATE touchpoints SET customer_key = ?
               WHERE customer_key = '' AND session_id IN (
                   SELECT session_id FROM sessions WHERE customer_key = ?
                   UNION SELECT ?
               )""",
            (customer_key, customer_key, session_id),
        )
        conn.commit()

    from main import manager
    import asyncio
    asyncio.create_task(manager.broadcast({
        "type": "identify",
        "customer_key": customer_key,
        "visitor_id": visitor_id,
        "ts": _iso_ts(datetime.now(UTC)),
    }))

    return {"ok": True, "customer_key": customer_key}


# ── Conversion tracking (from JS pixel) ──────────────────────────────────────

@router.post("/conversion")
async def track_conversion(request: Request):
    """Track a conversion event fired from hyros.conversion({...}).
    Creates an order + conversion record and a touchpoint for the last-touch params.
    """
    payload = await request.json()
    db_path = _db()
    now = _iso_ts(datetime.now(UTC))

    email = str(payload.get("customer_key", "")).strip().lower()
    customer_key = _sha256(email) if email and "@" in email else (email if email else "")

    # If they sent a raw email, hash it
    if "@" in str(payload.get("customer_key", "")):
        customer_key = _sha256(payload["customer_key"].strip().lower())

    order_id = str(payload.get("order_id", "")) or _sha256(f"px|{payload.get('visitor_id','')}|{now}")
    gross = float(payload.get("value", 0))
    conv_type = str(payload.get("type", "Purchase"))
    conv_lower = conv_type.strip().lower()

    utm_source = str(payload.get("utm_source", ""))
    utm_medium = str(payload.get("utm_medium", ""))

    # Determine platform from UTM or custom params
    detected_platform = str(payload.get("detected_platform", ""))
    fbc_id = str(payload.get("fbc_id", ""))
    ttc_id = str(payload.get("ttc_id", ""))
    gc_id = str(payload.get("gc_id", ""))
    g_special_campaign = str(payload.get("g_special_campaign", ""))

    platform = ""
    channel = "organic"
    if detected_platform == "meta" or fbc_id or utm_source in ("facebook", "fb", "meta", "ig", "instagram"):
        platform = "meta"
        channel = "paid_social"
    elif detected_platform == "tiktok" or ttc_id or utm_source in ("tiktok", "tt"):
        platform = "tiktok"
        channel = "paid_social"
    elif detected_platform == "google" or gc_id or g_special_campaign or utm_source in ("google", "gads"):
        platform = "google"
        channel = "paid_search"
    elif utm_medium == "email":
        channel = "email"

    session_id = str(payload.get("session_id", _sha256(now)))

    # Map custom params to standard fields (backend-side fallback)
    h_ad_id = str(payload.get("h_ad_id", ""))
    adset_id = str(payload.get("adset_id", "")) or fbc_id
    ad_id = str(payload.get("ad_id", ""))
    campaign_id = str(payload.get("campaign_id", "")) or str(payload.get("utm_campaign", "")) or gc_id
    if h_ad_id:
        if platform == "meta":
            ad_id = ad_id or h_ad_id
        elif platform == "tiktok":
            campaign_id = campaign_id or h_ad_id
        else:
            ad_id = ad_id or h_ad_id

    if platform == "meta" and not campaign_id and adset_id:
        campaign_id = _meta_campaign_from_adset(db_path, adset_id)

    is_purchase = conv_lower in ("purchase", "payment")

    with connect(db_path) as conn:
        if is_purchase:
            conn.execute(
                """INSERT OR IGNORE INTO orders (order_id, ts, gross, net, refunds, chargebacks, cogs, fees, customer_key, subscription_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    order_id,
                    now,
                    str(gross),
                    str(gross),
                    "0",
                    "0",
                    "0",
                    str(round(gross * 0.029 + 0.30, 2)),
                    customer_key,
                    "",
                ),
            )

        conn.execute(
            """INSERT OR IGNORE INTO conversions (conversion_id, ts, type, value, order_id, customer_key)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (_sha256(f"pxconv|{order_id}"), now, conv_type, str(gross), order_id, customer_key),
        )
        conn.execute(
            """INSERT INTO touchpoints (ts, channel, platform, campaign_id, adset_id, ad_id, creative_id, gclid, fbclid, ttclid, customer_key, session_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                now,
                channel,
                platform,
                campaign_id,
                adset_id,
                ad_id,
                str(payload.get("creative_id", "")),
                str(payload.get("gclid", "")),
                str(payload.get("fbclid", "")),
                str(payload.get("ttclid", "")),
                customer_key,
                session_id,
            ),
        )
        conn.commit()

    from main import manager
    import asyncio
    ws_type = "new_order" if is_purchase else "new_conversion"
    if conv_lower in ("lead", "formsubmission", "form_submission", "signup"):
        ws_type = "new_lead"
    elif conv_lower in ("booking", "appointmentbooked", "appointment_booked"):
        ws_type = "new_booking"

    asyncio.create_task(manager.broadcast({
        "type": ws_type,
        "conversion_type": conv_type,
        "order_id": order_id,
        "gross": gross,
        "utm_source": utm_source,
        "landing_page": str(payload.get("landing_page", "")),
        "customer_key": customer_key,
        "ts": now,
        "source": "pixel",
    }))

    return {"ok": True, "order_id": order_id, "customer_key": customer_key}
