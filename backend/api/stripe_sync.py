"""Stripe order sync — pull charges from Stripe API and store as orders.

Endpoints:
  POST /api/stripe/sync   — fetch Stripe charges for a date range, store as orders
  GET  /api/stripe/status — connection status + recent sync info
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, Query

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from attributionops.config import default_db_path
from attributionops.db import connect, sql_rows

router = APIRouter()
UTC = timezone.utc

STRIPE_CHARGES_URL = "https://api.stripe.com/v1/charges"
STRIPE_PAYMENT_INTENTS_URL = "https://api.stripe.com/v1/payment_intents"


def _db() -> str:
    return os.environ.get("ATTRIBUTIONOPS_DB_PATH", default_db_path())


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def _get_stripe_key() -> str:
    key = os.environ.get("STRIPE_API_SECRET_KEY", "").strip()
    if not key:
        key = os.environ.get("STRIPE_SECRET_KEY", "").strip()
    return key


def _ensure_orders_table(db_path: str) -> None:
    with connect(db_path) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            ts TEXT,
            gross TEXT DEFAULT '0',
            net TEXT DEFAULT '0',
            refunds TEXT DEFAULT '0',
            chargebacks TEXT DEFAULT '0',
            cogs TEXT DEFAULT '0',
            fees TEXT DEFAULT '0',
            customer_key TEXT DEFAULT '',
            subscription_id TEXT DEFAULT ''
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS conversions (
            conversion_id TEXT PRIMARY KEY,
            ts TEXT,
            type TEXT,
            value TEXT DEFAULT '0',
            order_id TEXT DEFAULT '',
            customer_key TEXT DEFAULT ''
        )""")
        conn.commit()


def _date_to_unix(date_str: str) -> int:
    """Convert YYYY-MM-DD to unix timestamp (start of day UTC)."""
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    return int(dt.timestamp())


def _date_to_unix_end(date_str: str) -> int:
    """Convert YYYY-MM-DD to unix timestamp (end of day UTC)."""
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    dt = dt.replace(hour=23, minute=59, second=59)
    return int(dt.timestamp())


async def _fetch_stripe_charges(
    api_key: str, start_date: str, end_date: str
) -> list[dict[str, Any]]:
    """Fetch all successful Stripe charges in the date range."""
    created_gte = _date_to_unix(start_date)
    created_lte = _date_to_unix_end(end_date)

    all_charges: list[dict] = []
    starting_after: str | None = None
    headers = {"Authorization": f"Bearer {api_key}"}

    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            params: dict[str, Any] = {
                "created[gte]": created_gte,
                "created[lte]": created_lte,
                "limit": 100,
                "expand[]": "data.customer",
            }
            if starting_after:
                params["starting_after"] = starting_after

            resp = await client.get(STRIPE_CHARGES_URL, headers=headers, params=params)
            resp.raise_for_status()
            data = resp.json()

            charges = data.get("data", [])
            all_charges.extend(charges)

            if not data.get("has_more") or not charges:
                break
            starting_after = charges[-1]["id"]

    # Only return successful, non-refunded charges
    return [c for c in all_charges if c.get("paid") and not c.get("refunded") and c.get("amount", 0) > 0]


def _extract_email_from_charge(charge: dict) -> str:
    """Extract customer email from a Stripe charge."""
    # billing_details.email
    billing = charge.get("billing_details") or {}
    if billing.get("email"):
        return str(billing["email"]).strip().lower()

    # customer object (expanded)
    customer = charge.get("customer") or {}
    if isinstance(customer, dict) and customer.get("email"):
        return str(customer["email"]).strip().lower()

    # receipt_email
    if charge.get("receipt_email"):
        return str(charge["receipt_email"]).strip().lower()

    # metadata
    meta = charge.get("metadata") or {}
    if meta.get("email"):
        return str(meta["email"]).strip().lower()

    return ""


def _write_stripe_orders(db_path: str, charges: list[dict]) -> dict[str, int]:
    """Write Stripe charges as orders + conversions into the DB."""
    _ensure_orders_table(db_path)

    inserted = 0
    skipped = 0
    now = _now()

    with connect(db_path) as conn:
        for charge in charges:
            charge_id = str(charge.get("id", ""))
            if not charge_id:
                continue

            order_id = _sha256(f"stripe|{charge_id}")

            # Check if already exists
            existing = conn.execute(
                "SELECT order_id FROM orders WHERE order_id = ?", (order_id,)
            ).fetchone()
            if existing:
                skipped += 1
                continue

            amount_cents = int(charge.get("amount", 0))
            amount_refunded_cents = int(charge.get("amount_refunded", 0))
            gross = round(amount_cents / 100, 2)
            refunds = round(amount_refunded_cents / 100, 2)
            net = round(gross - refunds, 2)
            fees = round(gross * 0.029 + 0.30, 2)  # Stripe standard fee estimate
            currency = str(charge.get("currency", "usd")).upper()

            # Convert amount if not USD (basic — just note currency)
            ts_unix = int(charge.get("created", 0))
            ts = datetime.fromtimestamp(ts_unix, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ") if ts_unix else now

            email = _extract_email_from_charge(charge)
            customer_key = _sha256(email) if email else ""

            subscription_id = str(charge.get("subscription") or "")

            conn.execute(
                """INSERT OR IGNORE INTO orders
                   (order_id, ts, gross, net, refunds, chargebacks, cogs, fees, customer_key, subscription_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (order_id, ts, str(gross), str(net), str(refunds), "0", "0", str(fees),
                 customer_key, subscription_id),
            )

            # Insert Purchase conversion
            conv_id = _sha256(f"stripe_conv|{charge_id}")
            conn.execute(
                """INSERT OR IGNORE INTO conversions
                   (conversion_id, ts, type, value, order_id, customer_key)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (conv_id, ts, "Purchase", str(gross), order_id, customer_key),
            )

            inserted += 1

        conn.commit()

    return {"inserted": inserted, "skipped": skipped}


@router.post("/sync")
async def stripe_sync(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
):
    """Pull Stripe charges for date range and store as orders + conversions."""
    api_key = _get_stripe_key()
    if not api_key:
        return {"synced": 0, "error": "STRIPE_API_SECRET_KEY not set in environment"}

    # Default: last 90 days
    if not end_date:
        end_date = datetime.now(UTC).strftime("%Y-%m-%d")
    if not start_date:
        start_date = (datetime.now(UTC) - timedelta(days=90)).strftime("%Y-%m-%d")

    try:
        charges = await _fetch_stripe_charges(api_key, start_date, end_date)
        db_path = _db()
        stats = _write_stripe_orders(db_path, charges)
        return {
            "synced": stats["inserted"],
            "skipped": stats["skipped"],
            "fetched": len(charges),
            "start_date": start_date,
            "end_date": end_date,
        }
    except httpx.HTTPStatusError as exc:
        detail = exc.response.text[:400] if exc.response is not None else str(exc)
        return {"synced": 0, "error": f"Stripe API error: {detail}"}
    except Exception as exc:
        return {"synced": 0, "error": str(exc)}


@router.get("/status")
async def stripe_status():
    """Return Stripe connection status."""
    api_key = _get_stripe_key()
    if not api_key:
        return {"connected": False, "error": "STRIPE_API_SECRET_KEY not set"}

    db_path = _db()
    _ensure_orders_table(db_path)
    try:
        rows = sql_rows(db_path, "SELECT COUNT(*) as cnt, MAX(ts) as last_ts FROM orders")
        count = int(rows[0]["cnt"]) if rows else 0
        last_ts = rows[0].get("last_ts") if rows else None

        # Quick connectivity check
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://api.stripe.com/v1/charges?limit=1",
                headers={"Authorization": f"Bearer {api_key}"},
            )
            connected = resp.status_code == 200

        return {
            "connected": connected,
            "orders_in_db": count,
            "last_order_ts": last_ts,
            "key_prefix": api_key[:12] + "...",
        }
    except Exception as exc:
        return {"connected": False, "error": str(exc)}
