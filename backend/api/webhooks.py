from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import sqlite3
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import anyio
import httpx
from fastapi import APIRouter, Request, HTTPException

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from attributionops.config import default_db_path
from attributionops.db import connect
from attributionops.schema import ensure_order_semantics

router = APIRouter()
logger = logging.getLogger("webhooks")

UTC = timezone.utc

# Stripe payment-processing fee estimate used only when the real fee is not
# available from the source event.
_FEE_RATE = 0.029
_FEE_FIXED = 0.30


def _db() -> str:
    return os.environ.get("ATTRIBUTIONOPS_DB_PATH", default_db_path())


def _norm_email(value: Any) -> str:
    return str(value or "").strip().lower()


def _stripe_order_id(payment_intent: str, fallback_id: str) -> str:
    """Canonical Stripe order id shared by the webhook and the backfill sync.

    Prefer the payment_intent so the three event types (checkout.session/
    charge/payment_intent) for one payment collapse to a single order.
    """
    pi = str(payment_intent or "").strip()
    fb = str(fallback_id or "").strip()
    return "stripe|" + (pi or fb)


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


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.Error:
        return set()
    return {str(row[1]) for row in rows}


_tracking_schema_lock = threading.Lock()
_tracking_schema_ready: set[str] = set()


def _ensure_tracking_schema(db_path: str) -> None:
    # Memoized per process+path: the ALTER/CREATE INDEX DDL only needs to run
    # once, not on every tracking hit (which serialized on the write lock).
    with _tracking_schema_lock:
        if db_path in _tracking_schema_ready:
            return
    _run_tracking_schema(db_path)
    with _tracking_schema_lock:
        _tracking_schema_ready.add(db_path)


def _run_tracking_schema(db_path: str) -> None:
    with connect(db_path) as conn:
        session_cols = _table_columns(conn, "sessions")
        if "visitor_id" not in session_cols:
            conn.execute("ALTER TABLE sessions ADD COLUMN visitor_id TEXT")
        if "event_name" not in session_cols:
            conn.execute("ALTER TABLE sessions ADD COLUMN event_name TEXT")
        if "page_title" not in session_cols:
            conn.execute("ALTER TABLE sessions ADD COLUMN page_title TEXT")
        if "custom_data_json" not in session_cols:
            conn.execute("ALTER TABLE sessions ADD COLUMN custom_data_json TEXT")

        order_cols = _table_columns(conn, "orders")
        for col in (
            "session_id",
            "visitor_id",
            "channel",
            "platform",
            "campaign_id",
            "adset_id",
            "ad_id",
            "creative_id",
            "gclid",
            "fbclid",
            "ttclid",
        ):
            if col not in order_cols:
                conn.execute(f"ALTER TABLE orders ADD COLUMN {col} TEXT DEFAULT ''")
        ensure_order_semantics(conn)

        conversion_cols = _table_columns(conn, "conversions")
        for col in ("session_id", "visitor_id"):
            if col not in conversion_cols:
                conn.execute(f"ALTER TABLE conversions ADD COLUMN {col} TEXT DEFAULT ''")

        touchpoint_cols = _table_columns(conn, "touchpoints")
        if "visitor_id" not in touchpoint_cols:
            conn.execute("ALTER TABLE touchpoints ADD COLUMN visitor_id TEXT DEFAULT ''")

        conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_session_id ON sessions(session_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_visitor_id ON sessions(visitor_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_customer_key ON sessions(customer_key)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_touchpoints_session_id ON touchpoints(session_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_touchpoints_visitor_id ON touchpoints(visitor_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_touchpoints_customer_key ON touchpoints(customer_key)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_customer_key ON orders(customer_key)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_session_id ON orders(session_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_visitor_id ON orders(visitor_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_conversions_customer_key ON conversions(customer_key)")
        conn.commit()


def _lookup_customer_key(db_path: str, visitor_id: str = "", session_id: str = "") -> str:
    with connect(db_path) as conn:
        if visitor_id:
            row = conn.execute(
                """SELECT customer_key FROM sessions
                   WHERE visitor_id = ? AND COALESCE(customer_key, '') != ''
                   ORDER BY ts DESC LIMIT 1""",
                (visitor_id,),
            ).fetchone()
            if row and row[0]:
                return str(row[0]).strip()
        if session_id:
            row = conn.execute(
                """SELECT customer_key FROM sessions
                   WHERE session_id = ? AND COALESCE(customer_key, '') != ''
                   ORDER BY ts DESC LIMIT 1""",
                (session_id,),
            ).fetchone()
            if row and row[0]:
                return str(row[0]).strip()
    return ""


def _stape_endpoint() -> str:
    return os.environ.get("STAPE_ENDPOINT", "").strip().rstrip("/")


def _compact_params(params: dict[str, Any]) -> dict[str, str]:
    return {
        str(key): str(value)
        for key, value in params.items()
        if value is not None and str(value) != ""
    }


async def _forward_purchase_to_stape(payload: dict[str, Any], order_id: str, session_id: str, value: float) -> None:
    endpoint = _stape_endpoint()
    if not endpoint:
        return

    params = _compact_params({
        "v": os.environ.get("STAPE_VERSION", "2").strip() or "2",
        "event": "purchase",
        "value": value,
        "currency": payload.get("currency", "USD"),
        "transaction_id": order_id,
        "order_id": order_id,
        "event_id": payload.get("event_id") or order_id,
        "visitor_id": payload.get("visitor_id"),
        "session_id": session_id,
        "utm_source": payload.get("utm_source"),
        "utm_medium": payload.get("utm_medium"),
        "utm_campaign": payload.get("utm_campaign"),
        "utm_content": payload.get("utm_content"),
        "utm_term": payload.get("utm_term"),
        "gclid": payload.get("gclid"),
        "wbraid": payload.get("wbraid"),
        "gbraid": payload.get("gbraid"),
        "fbclid": payload.get("fbclid"),
        "ttclid": payload.get("ttclid"),
        "ad_id": payload.get("ad_id"),
        "adset_id": payload.get("adset_id"),
        "campaign_id": payload.get("campaign_id"),
        "creative_id": payload.get("creative_id"),
        "landing_page": payload.get("landing_page"),
        "referrer": payload.get("referrer"),
        "device": payload.get("device"),
    })

    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            await client.get(f"{endpoint}/data", params=params)
    except Exception:
        # Stape forwarding must never make the conversion endpoint fail.
        return


def _backfill_customer_key(conn: sqlite3.Connection, customer_key: str, visitor_id: str = "", session_id: str = "") -> None:
    if not customer_key:
        return
    if visitor_id:
        conn.execute(
            "UPDATE sessions SET customer_key = ? WHERE COALESCE(customer_key, '') = '' AND visitor_id = ?",
            (customer_key, visitor_id),
        )
        conn.execute(
            "UPDATE touchpoints SET customer_key = ? WHERE COALESCE(customer_key, '') = '' AND visitor_id = ?",
            (customer_key, visitor_id),
        )
        conn.execute(
            """UPDATE touchpoints SET customer_key = ?
               WHERE COALESCE(customer_key, '') = '' AND session_id IN (
                   SELECT DISTINCT session_id FROM sessions WHERE visitor_id = ?
               )""",
            (customer_key, visitor_id),
        )
    if session_id:
        conn.execute(
            "UPDATE sessions SET customer_key = ? WHERE COALESCE(customer_key, '') = '' AND session_id = ?",
            (customer_key, session_id),
        )
        conn.execute(
            "UPDATE touchpoints SET customer_key = ? WHERE COALESCE(customer_key, '') = '' AND session_id = ?",
            (customer_key, session_id),
        )


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
        # Shopify sends the HMAC base64-encoded, not hex.
        digest = base64.b64encode(
            hmac.new(secret.encode(), body, hashlib.sha256).digest()
        ).decode()
        if not hmac.compare_digest(digest, hmac_header):
            raise HTTPException(status_code=401, detail="Invalid HMAC")
    else:
        logger.warning("SHOPIFY_WEBHOOK_SECRET not set — accepting webhook without verification")

    payload = json.loads(body)
    # payload["customer"] can be null (guest/POS orders) — guard against .get on None.
    email = _norm_email(payload.get("email") or (payload.get("customer") or {}).get("email"))
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
        "fees": round(gross * _FEE_RATE + _FEE_FIXED, 2),  # Shopify payments estimate
        "customer_key": customer_key,
        "subscription_id": "",
    }

    await anyio.to_thread.run_sync(_insert_order, _db(), order)

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

def _verify_stripe_signature(body: bytes, sig_header: str, secret: str, tolerance: int = 300) -> bool:
    """Verify a Stripe-Signature header (t=…,v1=…) against the raw body."""
    if not sig_header:
        return False
    parts = dict(
        p.split("=", 1) for p in sig_header.split(",") if "=" in p
    )
    t = parts.get("t", "")
    v1 = parts.get("v1", "")
    if not t or not v1:
        return False
    try:
        if abs(time.time() - int(t)) > tolerance:
            return False
    except ValueError:
        return False
    signed = f"{t}.{body.decode('utf-8', 'replace')}".encode()
    expected = hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, v1)


@router.post("/stripe")
async def stripe_webhook(request: Request):
    """Receive Stripe checkout.session.completed or charge.succeeded webhook."""
    secret = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
    body = await request.body()

    if secret:
        sig = request.headers.get("Stripe-Signature", "")
        if not _verify_stripe_signature(body, sig, secret):
            raise HTTPException(status_code=401, detail="Invalid Stripe signature")
    else:
        logger.warning("STRIPE_WEBHOOK_SECRET not set — accepting webhook without verification")

    payload = json.loads(body)
    event_type = payload.get("type", "")

    if event_type not in ("checkout.session.completed", "charge.succeeded", "payment_intent.succeeded"):
        return {"ok": True, "skipped": True, "event_type": event_type}

    data = payload.get("data", {}).get("object", {})
    email = _norm_email(data.get("customer_email") or data.get("receipt_email"))
    customer_key = _sha256(email) if email else ""

    amount = float(data.get("amount_total", data.get("amount", 0))) / 100.0  # cents → dollars

    # One canonical order id per payment (payment_intent) so the three event
    # types for the same payment collapse under the UNIQUE index; matches the
    # scheme used by /api/stripe/sync.
    payment_intent = str(data.get("payment_intent") or "")
    order_id = _stripe_order_id(payment_intent, str(data.get("id") or ""))

    # Stamp with the event's own creation time, not the receive time, so delayed
    # deliveries/retries land in the correct day bucket.
    created = payload.get("created") or data.get("created")
    try:
        ts = _iso_ts(datetime.fromtimestamp(int(created), tz=UTC)) if created else _iso_ts(datetime.now(UTC))
    except (ValueError, TypeError, OSError):
        ts = _iso_ts(datetime.now(UTC))

    order = {
        "order_id": order_id,
        "ts": ts,
        "gross": round(amount, 2),
        "net": round(amount, 2),
        "refunds": 0,
        "chargebacks": 0,
        "cogs": 0,
        "fees": round(amount * _FEE_RATE + _FEE_FIXED, 2),  # Stripe fee estimate
        "customer_key": customer_key,
        "subscription_id": str(data.get("subscription", "") or ""),
    }

    await anyio.to_thread.run_sync(_insert_order, _db(), order)

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
    _ensure_tracking_schema(db_path)
    now = _iso_ts(datetime.now(UTC))

    session_id = str(payload.get("session_id", _sha256(now)))
    visitor_id = str(payload.get("visitor_id", "")).strip()
    customer_key = str(payload.get("customer_key", "")).strip().lower()
    custom_data = payload.get("custom_data") if isinstance(payload.get("custom_data"), dict) else {}
    if not customer_key and custom_data.get("email"):
        customer_key = str(custom_data.get("email", "")).strip().lower()
    if "@" in customer_key:
        customer_key = _sha256(customer_key)
    if not customer_key:
        customer_key = await anyio.to_thread.run_sync(
            _lookup_customer_key, db_path, visitor_id, session_id
        )
    utm_source = str(payload.get("utm_source", ""))
    utm_medium = str(payload.get("utm_medium", ""))
    event_name = str(payload.get("event", "")).strip() or "pageview"
    custom_data_json = json.dumps(custom_data, separators=(",", ":"), sort_keys=True) if custom_data else ""

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
        campaign_id = await anyio.to_thread.run_sync(_meta_campaign_from_adset, db_path, adset_id)

    # Preserve the real UTM campaign string in the utm_campaign column; the
    # derived campaign_id (which may be a numeric/gc id) only belongs on the
    # touchpoints row.
    utm_campaign = str(payload.get("utm_campaign", ""))

    def _write_track() -> None:
        with connect(db_path) as conn:
            conn.execute(
                """INSERT INTO sessions (session_id, visitor_id, ts, event_name, page_title, utm_source, utm_medium, utm_campaign, utm_content, utm_term, referrer, landing_page, device, gclid, fbclid, ttclid, customer_key, custom_data_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    session_id,
                    visitor_id,
                    now,
                    event_name,
                    str(payload.get("page_title", "")),
                    utm_source,
                    utm_medium,
                    utm_campaign,
                    str(payload.get("utm_content", "")),
                    str(payload.get("utm_term", "")),
                    str(payload.get("referrer", "")),
                    str(payload.get("landing_page", "/")),
                    str(payload.get("device", "unknown")),
                    str(payload.get("gclid", "")),
                    str(payload.get("fbclid", "")),
                    str(payload.get("ttclid", "")),
                    customer_key,
                    custom_data_json,
                ),
            )
            conn.execute(
                """INSERT INTO touchpoints (ts, channel, platform, campaign_id, adset_id, ad_id, creative_id, gclid, fbclid, ttclid, customer_key, session_id, visitor_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                    visitor_id,
                ),
            )
            conn.commit()

    await anyio.to_thread.run_sync(_write_track)

    from main import manager
    import asyncio
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
    _ensure_tracking_schema(db_path)

    def _write_identify() -> None:
        with connect(db_path) as conn:
            _backfill_customer_key(conn, customer_key, visitor_id=visitor_id, session_id=session_id)
            conn.commit()

    await anyio.to_thread.run_sync(_write_identify)

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
    _ensure_tracking_schema(db_path)
    now = _iso_ts(datetime.now(UTC))
    visitor_id = str(payload.get("visitor_id", "")).strip()

    email = str(payload.get("customer_key", "")).strip().lower()
    customer_key = _sha256(email) if email and "@" in email else (email if email else "")

    # If they sent a raw email, hash it
    if "@" in str(payload.get("customer_key", "")):
        customer_key = _sha256(payload["customer_key"].strip().lower())

    gross = float(payload.get("value", 0))
    conv_type = str(payload.get("type", "Purchase"))
    conv_lower = conv_type.strip().lower()
    # Include a content nonce (type + value) so distinct same-second products
    # from one visitor (bundle checkouts) don't collapse to a single order id.
    order_id = str(payload.get("order_id", "")) or _sha256(
        f"px|{visitor_id}|{now}|{conv_lower}|{gross}"
    )

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
    if not customer_key:
        customer_key = await anyio.to_thread.run_sync(
            _lookup_customer_key, db_path, visitor_id, session_id
        )

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
        campaign_id = await anyio.to_thread.run_sync(_meta_campaign_from_adset, db_path, adset_id)

    is_purchase = conv_lower in ("purchase", "payment")
    fees = str(round(gross * _FEE_RATE + _FEE_FIXED, 2))

    def _write_conversion() -> None:
        with connect(db_path) as conn:
            _backfill_customer_key(conn, customer_key, visitor_id=visitor_id, session_id=session_id)
            if is_purchase:
                conn.execute(
                    """INSERT OR IGNORE INTO orders (
                           order_id, ts, gross, net, refunds, chargebacks, cogs, fees,
                           customer_key, subscription_id, session_id, visitor_id, channel,
                           platform, campaign_id, adset_id, ad_id, creative_id, gclid, fbclid, ttclid
                       )
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        order_id,
                        now,
                        str(gross),
                        str(gross),
                        "0",
                        "0",
                        "0",
                        fees,
                        customer_key,
                        "",
                        session_id,
                        visitor_id,
                        channel,
                        platform,
                        campaign_id,
                        adset_id,
                        ad_id,
                        str(payload.get("creative_id", "")),
                        str(payload.get("gclid", "")),
                        str(payload.get("fbclid", "")),
                        str(payload.get("ttclid", "")),
                    ),
                )

            conn.execute(
                """INSERT OR IGNORE INTO conversions (
                       conversion_id, ts, type, value, order_id, customer_key, session_id, visitor_id
                   )
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (_sha256(f"pxconv|{order_id}"), now, conv_type, str(gross), order_id, customer_key, session_id, visitor_id),
            )
            conn.execute(
                """INSERT INTO touchpoints (ts, channel, platform, campaign_id, adset_id, ad_id, creative_id, gclid, fbclid, ttclid, customer_key, session_id, visitor_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                    visitor_id,
                ),
            )
            conn.commit()

    await anyio.to_thread.run_sync(_write_conversion)

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
    if is_purchase:
        asyncio.create_task(_forward_purchase_to_stape(payload, order_id, session_id, gross))

    return {"ok": True, "order_id": order_id, "customer_key": customer_key}
