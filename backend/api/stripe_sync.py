"""Stripe order sync — pull charges from Stripe API and store as orders.

Endpoints:
  POST /api/stripe/sync   — fetch Stripe charges for a date range, store as orders
  GET  /api/stripe/status — connection status + recent sync info
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, Query

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from attributionops.config import default_db_path
from attributionops.db import connect, sql_rows
from attributionops.schema import ensure_order_semantics
from attributionops.util import local_day_bounds_utc, parse_iso_ts

router = APIRouter()
UTC = timezone.utc

STRIPE_CHARGES_URL = "https://api.stripe.com/v1/charges"
STRIPE_PAYMENT_INTENTS_URL = "https://api.stripe.com/v1/payment_intents"

# Fee estimate used only when the real balance_transaction fee is unavailable.
_FEE_RATE = 0.029
_FEE_FIXED = 0.30


def _db() -> str:
    return os.environ.get("ATTRIBUTIONOPS_DB_PATH", default_db_path())


def _payment_intent_id(charge: dict) -> str:
    """Extract the payment_intent id whether or not it was expanded to an object."""
    pi = charge.get("payment_intent")
    if isinstance(pi, dict):
        return str(pi.get("id") or "")
    return str(pi or "")


def _stripe_order_id(charge: dict) -> str:
    """Canonical Stripe order id shared with the realtime webhook.

    Keyed on payment_intent (falling back to charge id) and NOT hashed, so a
    charge ingested via the webhook and via this backfill produce the identical
    order id and dedupe against each other.
    """
    return "stripe|" + (_payment_intent_id(charge) or str(charge.get("id") or ""))


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.Error:
        return set()
    return {str(row[1]) for row in rows}


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
        conn.execute("""CREATE TABLE IF NOT EXISTS stripe_sync_coverage (
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (start_date, end_date)
        )""")
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
        conversion_cols = _table_columns(conn, "conversions")
        for col in ("session_id", "visitor_id"):
            if col not in conversion_cols:
                conn.execute(f"ALTER TABLE conversions ADD COLUMN {col} TEXT DEFAULT ''")
        session_cols = _table_columns(conn, "sessions")
        if session_cols and "visitor_id" not in session_cols:
            conn.execute("ALTER TABLE sessions ADD COLUMN visitor_id TEXT DEFAULT ''")
        touchpoint_cols = _table_columns(conn, "touchpoints")
        if touchpoint_cols and "visitor_id" not in touchpoint_cols:
            conn.execute("ALTER TABLE touchpoints ADD COLUMN visitor_id TEXT DEFAULT ''")
        ensure_order_semantics(conn)
        conn.commit()


def _history_backfill_days() -> int:
    """Return the number of pre-window days used to classify repeat buyers."""
    raw = str(os.environ.get("STRIPE_HISTORY_BACKFILL_DAYS", "365") or "365").strip()
    try:
        return max(0, min(int(raw), 3650))
    except ValueError:
        return 365


def _parse_calendar_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _missing_stripe_coverage(db_path: str, start_date: str, end_date: str) -> list[tuple[str, str]]:
    """Return uncovered inclusive calendar-date ranges."""
    start = _parse_calendar_date(start_date)
    end = _parse_calendar_date(end_date)
    if start > end:
        return []

    _ensure_orders_table(db_path)
    with connect(db_path) as conn:
        rows = conn.execute(
            """SELECT start_date, end_date
               FROM stripe_sync_coverage
               WHERE end_date >= ? AND start_date <= ?
               ORDER BY start_date, end_date""",
            (start_date, end_date),
        ).fetchall()

    missing: list[tuple[str, str]] = []
    cursor = start
    for row in rows:
        covered_start = max(start, _parse_calendar_date(str(row[0])))
        covered_end = min(end, _parse_calendar_date(str(row[1])))
        if covered_end < cursor:
            continue
        if covered_start > cursor:
            missing.append((cursor.isoformat(), (covered_start - timedelta(days=1)).isoformat()))
        cursor = max(cursor, covered_end + timedelta(days=1))
        if cursor > end:
            break
    if cursor <= end:
        missing.append((cursor.isoformat(), end.isoformat()))
    return missing


def _record_stripe_coverage(db_path: str, start_date: str, end_date: str) -> None:
    """Merge a successfully fetched range into the Stripe coverage ledger."""
    start = _parse_calendar_date(start_date)
    end = _parse_calendar_date(end_date)
    if start > end:
        start, end = end, start

    _ensure_orders_table(db_path)
    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT start_date, end_date FROM stripe_sync_coverage ORDER BY start_date, end_date"
        ).fetchall()
        ranges = [(start, end)] + [
            (_parse_calendar_date(str(row[0])), _parse_calendar_date(str(row[1])))
            for row in rows
        ]
        ranges.sort(key=lambda item: item[0])

        merged: list[tuple[date, date]] = []
        for range_start, range_end in ranges:
            if not merged or range_start > merged[-1][1] + timedelta(days=1):
                merged.append((range_start, range_end))
            else:
                previous_start, previous_end = merged[-1]
                merged[-1] = (previous_start, max(previous_end, range_end))

        conn.execute("DELETE FROM stripe_sync_coverage")
        conn.executemany(
            "INSERT INTO stripe_sync_coverage (start_date, end_date, updated_at) VALUES (?, ?, ?)",
            [(range_start.isoformat(), range_end.isoformat(), _now()) for range_start, range_end in merged],
        )
        conn.commit()


def _date_to_unix(date_str: str) -> int:
    """Convert a reporting-zone local day start to a Unix timestamp."""
    start_utc, _ = local_day_bounds_utc(date_str, date_str)
    return int(parse_iso_ts(start_utc).timestamp())


def _date_to_unix_end(date_str: str) -> int:
    """Convert a reporting-zone local day end to an inclusive Unix timestamp."""
    _, end_exclusive_utc = local_day_bounds_utc(date_str, date_str)
    return int(parse_iso_ts(end_exclusive_utc).timestamp()) - 1


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
            params: list[tuple[str, Any]] = [
                ("created[gte]", created_gte),
                ("created[lte]", created_lte),
                ("limit", 100),
                ("expand[]", "data.customer"),
                ("expand[]", "data.payment_intent"),
                ("expand[]", "data.invoice"),
                ("expand[]", "data.balance_transaction"),
            ]
            if starting_after:
                params.append(("starting_after", starting_after))

            resp = await client.get(STRIPE_CHARGES_URL, headers=headers, params=params)
            resp.raise_for_status()
            data = resp.json()

            charges = data.get("data", [])
            all_charges.extend(charges)

            if not data.get("has_more") or not charges:
                break
            starting_after = charges[-1]["id"]

    # Return all successful charges — INCLUDING refunded/fully-refunded ones, so
    # their refund amount is recorded and net revenue is reduced correctly.
    return [c for c in all_charges if c.get("paid") and c.get("amount", 0) > 0]


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


def _normalize_meta_key(key: str) -> str:
    return str(key or "").strip().lower().replace("-", "_").replace(" ", "_")


def _metadata_items(charge: dict):
    containers = [charge]
    for key in ("payment_intent", "customer", "invoice"):
        value = charge.get(key)
        if isinstance(value, dict):
            containers.append(value)

    for container in containers:
        metadata = container.get("metadata") or {}
        if isinstance(metadata, dict):
            for key, value in metadata.items():
                if value is not None and str(value).strip():
                    yield _normalize_meta_key(key), str(value).strip()


def _meta_value(charge: dict, *keys: str) -> str:
    wanted = {_normalize_meta_key(key) for key in keys}
    for key, value in _metadata_items(charge):
        if key in wanted:
            return value
    return ""


def _customer_key_from_charge(charge: dict, email: str) -> str:
    if email:
        return _sha256(email)
    raw_key = _meta_value(charge, "customer_key", "hyros_customer_key", "_hyros_ck")
    if not raw_key:
        return ""
    normalized = raw_key.strip().lower()
    if "@" in normalized:
        return _sha256(normalized)
    return normalized


def _stripe_customer_id(charge: dict) -> str:
    customer = charge.get("customer")
    if isinstance(customer, dict):
        return str(customer.get("id") or "").strip()
    return str(customer or "").strip()


def _payment_fingerprint(charge: dict) -> str:
    details = charge.get("payment_method_details") or {}
    if isinstance(details, dict):
        card = details.get("card") or {}
        if isinstance(card, dict) and card.get("fingerprint"):
            return str(card["fingerprint"]).strip()
    payment_method = charge.get("payment_method")
    if isinstance(payment_method, dict):
        card = payment_method.get("card") or {}
        if isinstance(card, dict) and card.get("fingerprint"):
            return str(card["fingerprint"]).strip()
    return ""


def _product_key(charge: dict, amount_cents: int, currency: str) -> str:
    explicit = _meta_value(
        charge,
        "product_id",
        "product",
        "price_id",
        "price",
        "plan_id",
        "plan",
        "offer_id",
        "offer",
    )
    if explicit:
        return explicit.strip().lower()

    invoice = charge.get("invoice")
    if isinstance(invoice, dict):
        lines = (invoice.get("lines") or {}).get("data") or []
        for line in lines:
            if not isinstance(line, dict):
                continue
            price = line.get("price") or {}
            if isinstance(price, dict) and price.get("id"):
                return str(price["id"]).strip().lower()
            if line.get("description"):
                return str(line["description"]).strip().lower()

    description = str(charge.get("description") or "").strip().lower()
    if description:
        return description
    # Amount is only a weak product identity. Keep it explicit so recurrence
    # heuristics can refuse to use this fallback on its own.
    return f"amount:{currency.lower()}:{amount_cents}"


def _explicit_recurring(charge: dict) -> bool:
    invoice = charge.get("invoice")
    billing_reason = str(
        invoice.get("billing_reason") if isinstance(invoice, dict) else ""
    ).strip().lower()
    if billing_reason == "subscription_cycle":
        return True

    raw = _meta_value(charge, "is_recurring", "recurring", "subscription_cycle")
    return raw.strip().lower() in {"1", "true", "yes", "y", "recurring", "cycle"}


def _configured_recurring_amount(amount_cents: int, currency: str) -> bool:
    """Apply account-owned recurrence rules for metadata-free charges.

    Some legacy payment links emit bare Stripe charges with no customer,
    invoice, product or description. Hyros handles those through configured
    sale-item rules (for this account, ``$stripe-50``). Keep that policy in an
    environment variable so an amount is never assumed recurring globally.
    Format: ``CAD:5000,USD:9900`` (minor currency units).
    """
    candidate = f"{str(currency or '').strip().upper()}:{int(amount_cents)}"
    configured = {
        item.strip().upper()
        for item in os.environ.get("STRIPE_RECURRING_AMOUNT_KEYS", "").split(",")
        if item.strip()
    }
    return candidate in configured


def _canonical_customer_key(
    conn: sqlite3.Connection,
    charge: dict,
    email: str,
    processor_customer_id: str,
    fingerprint: str,
) -> str:
    """Resolve Stripe aliases to one durable customer identity."""
    explicit = _customer_key_from_charge(charge, email)
    existing = None
    if processor_customer_id:
        existing = conn.execute(
            """SELECT customer_key FROM orders
               WHERE processor = 'stripe' AND processor_customer_id = ?
                 AND COALESCE(customer_key, '') != ''
               ORDER BY ts DESC LIMIT 1""",
            (processor_customer_id,),
        ).fetchone()
    if not existing and fingerprint:
        existing = conn.execute(
            """SELECT customer_key FROM orders
               WHERE processor = 'stripe' AND payment_fingerprint = ?
                 AND COALESCE(customer_key, '') != ''
               ORDER BY ts DESC LIMIT 1""",
            (fingerprint,),
        ).fetchone()

    # A newly observed email/explicit tracking key is stronger than an older
    # processor-only alias. Backfill earlier rows to prevent one buyer becoming
    # two customers when Stripe later supplies their email.
    if explicit:
        customer_key = explicit
    elif existing and existing[0]:
        customer_key = str(existing[0])
    elif processor_customer_id:
        customer_key = _sha256(f"stripe_customer|{processor_customer_id}")
    elif fingerprint:
        customer_key = _sha256(f"stripe_card|{fingerprint}")
    else:
        customer_key = ""

    if customer_key and processor_customer_id:
        conn.execute(
            """UPDATE orders SET customer_key = ?
               WHERE processor = 'stripe' AND processor_customer_id = ?
                 AND customer_key != ?""",
            (customer_key, processor_customer_id, customer_key),
        )
    if customer_key and fingerprint:
        conn.execute(
            """UPDATE orders SET customer_key = ?
               WHERE processor = 'stripe' AND payment_fingerprint = ?
                 AND customer_key != ?""",
            (customer_key, fingerprint, customer_key),
        )
    return customer_key


def _matching_identity_sql(
    customer_key: str, processor_customer_id: str, fingerprint: str
) -> tuple[str, list[str]]:
    clauses: list[str] = []
    params: list[str] = []
    if customer_key:
        clauses.append("customer_key = ?")
        params.append(customer_key)
    if processor_customer_id:
        clauses.append("processor_customer_id = ?")
        params.append(processor_customer_id)
    if fingerprint:
        clauses.append("payment_fingerprint = ?")
        params.append(fingerprint)
    return (" OR ".join(clauses) or "0"), params


def _infer_recurring_from_history(
    conn: sqlite3.Connection,
    *,
    ts: str,
    product_key: str,
    customer_key: str,
    processor_customer_id: str,
    fingerprint: str,
    allow_amount_fallback: bool = False,
) -> bool:
    # Normal products use Hyros' 30..365 day recurrence window. Account-specific
    # amount fallbacks may repeat sooner, but still require a shared identity and
    # an older matching charge; amount alone is never enough.
    if not product_key or (product_key.startswith("amount:") and not allow_amount_fallback):
        return False
    identity_sql, identity_params = _matching_identity_sql(
        customer_key, processor_customer_id, fingerprint
    )
    if identity_sql == "0":
        return False
    current = parse_iso_ts(ts)
    start = (current - timedelta(days=365)).strftime("%Y-%m-%dT%H:%M:%SZ")
    if allow_amount_fallback:
        # The current charge may already exist during a resync. Keep the upper
        # bound strictly earlier so a first purchase cannot match itself.
        end = (current - timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        end = (current - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    row = conn.execute(
        f"""SELECT 1 FROM orders
            WHERE processor = 'stripe' AND product_key = ?
              AND ts >= ? AND ts <= ? AND ({identity_sql})
            LIMIT 1""",
        [product_key, start, end, *identity_params],
    ).fetchone()
    return bool(row)


def _sale_group_id(
    conn: sqlite3.Connection,
    *,
    order_id: str,
    ts: str,
    customer_key: str,
    processor_customer_id: str,
    fingerprint: str,
) -> str:
    """Collapse same-buyer charges within the configured 10-minute window."""
    identity_sql, identity_params = _matching_identity_sql(
        customer_key, processor_customer_id, fingerprint
    )
    if identity_sql == "0":
        return order_id
    current = parse_iso_ts(ts)
    lower = (current - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
    upper = (current + timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
    row = conn.execute(
        f"""SELECT COALESCE(NULLIF(sale_group_id, ''), order_id) AS group_id
            FROM orders
            WHERE order_id != ? AND ts >= ? AND ts <= ? AND ({identity_sql})
            ORDER BY ts ASC LIMIT 1""",
        [order_id, lower, upper, *identity_params],
    ).fetchone()
    return str(row[0]) if row and row[0] else order_id


def _extract_tracking_from_charge(charge: dict) -> dict[str, str]:
    def value(*keys: str) -> str:
        return _meta_value(charge, *keys)

    detected_platform = value("detected_platform", "hyros_platform", "platform").lower()
    gclid = value("gclid")
    fbclid = value("fbclid")
    ttclid = value("ttclid")
    fbc_id = value("fbc_id")
    ttc_id = value("ttc_id")
    gc_id = value("gc_id")
    h_ad_id = value("h_ad_id")
    g_special = value("g_special_campaign")
    utm_source = value("utm_source")
    utm_medium = value("utm_medium").lower()
    source = (utm_source or detected_platform or "").lower()

    platform = ""
    channel = ""
    if detected_platform == "meta" or fbc_id or fbclid or source in {"facebook", "fb", "meta", "instagram", "ig"}:
        platform, channel = "meta", "paid_social"
    elif detected_platform == "tiktok" or ttc_id or ttclid or source in {"tiktok", "tt"}:
        platform, channel = "tiktok", "paid_social"
    elif detected_platform == "google" or gc_id or g_special or gclid or source in {"google", "gads", "adwords"}:
        platform, channel = "google", "paid_search"
    elif utm_medium == "email" or source == "email":
        channel = "email"
    elif source:
        channel = "referral"

    campaign_id = value("campaign_id", "campaignid", "gc_id", "utm_campaign")
    adset_id = value("adset_id", "adsetid", "fbc_id")
    ad_id = value("ad_id", "adid")
    if not ad_id and platform != "tiktok":
        ad_id = h_ad_id
    creative_id = value("creative_id", "creativeid", "utm_content")

    return {
        "session_id": value("hyros_session_id", "session_id", "sessionId", "_hyros_sid", "hyros_sid", "mini_hyros_session_id"),
        "visitor_id": value("hyros_visitor_id", "visitor_id", "visitorId", "_hyros_vid", "hyros_vid", "mini_hyros_visitor_id"),
        "channel": channel,
        "platform": platform,
        "campaign_id": campaign_id,
        "adset_id": adset_id,
        "ad_id": ad_id,
        "creative_id": creative_id,
        "gclid": gclid,
        "fbclid": fbclid,
        "ttclid": ttclid,
    }


def _backfill_customer_key(conn: sqlite3.Connection, customer_key: str, visitor_id: str = "", session_id: str = "") -> None:
    if not customer_key or (not visitor_id and not session_id):
        return
    tables = {
        str(row[0])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    if "sessions" not in tables or "touchpoints" not in tables:
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


def _write_stripe_orders(db_path: str, charges: list[dict]) -> dict[str, int]:
    """Write Stripe charges as orders + conversions into the DB."""
    _ensure_orders_table(db_path)

    inserted = 0
    updated = 0
    recurring = 0
    now = _now()

    with connect(db_path) as conn:
        # Stripe returns newest first. Oldest first makes historical recurrence
        # checks and same-minute sale grouping deterministic within this batch.
        for charge in sorted(charges, key=lambda item: int(item.get("created", 0) or 0)):
            charge_id = str(charge.get("id", ""))
            if not charge_id:
                continue

            # Canonical id shared with the webhook (payment_intent-based, unhashed).
            order_id = _stripe_order_id(charge)

            amount_cents = int(charge.get("amount", 0))
            amount_refunded_cents = int(charge.get("amount_refunded", 0))
            gross = round(amount_cents / 100, 2)
            refunds = round(amount_refunded_cents / 100, 2)
            net = round(gross - refunds, 2)
            # Prefer the real Stripe fee from the (expanded) balance_transaction.
            bt = charge.get("balance_transaction")
            if isinstance(bt, dict) and bt.get("fee") is not None:
                fees = round(int(bt.get("fee", 0)) / 100, 2)
            else:
                fees = round(gross * _FEE_RATE + _FEE_FIXED, 2)
            currency = str(charge.get("currency", "usd")).upper()

            # Convert amount if not USD (basic — just note currency)
            ts_unix = int(charge.get("created", 0))
            ts = datetime.fromtimestamp(ts_unix, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ") if ts_unix else now

            email = _extract_email_from_charge(charge)
            tracking = _extract_tracking_from_charge(charge)
            processor_customer_id = _stripe_customer_id(charge)
            fingerprint = _payment_fingerprint(charge)
            customer_key = _canonical_customer_key(
                conn, charge, email, processor_customer_id, fingerprint
            )
            _backfill_customer_key(
                conn,
                customer_key,
                visitor_id=tracking["visitor_id"],
                session_id=tracking["session_id"],
            )

            # subscription lives on the (expanded) invoice, not the charge.
            invoice = charge.get("invoice")
            subscription_id = str(
                (invoice.get("subscription") if isinstance(invoice, dict) else None)
                or charge.get("subscription")
                or ""
            )
            product_key = _product_key(charge, amount_cents, currency)
            is_recurring = (
                _explicit_recurring(charge)
                or _infer_recurring_from_history(
                    conn,
                    ts=ts,
                    product_key=product_key,
                    customer_key=customer_key,
                    processor_customer_id=processor_customer_id,
                    fingerprint=fingerprint,
                    allow_amount_fallback=_configured_recurring_amount(amount_cents, currency),
                )
            )
            sale_group_id = _sale_group_id(
                conn,
                order_id=order_id,
                ts=ts,
                customer_key=customer_key,
                processor_customer_id=processor_customer_id,
                fingerprint=fingerprint,
            )
            recurring += int(is_recurring)

            cur = conn.execute(
                """INSERT OR IGNORE INTO orders
                   (order_id, ts, gross, net, refunds, chargebacks, cogs, fees,
                    customer_key, subscription_id, session_id, visitor_id, channel,
                    platform, campaign_id, adset_id, ad_id, creative_id, gclid, fbclid, ttclid,
                    currency, processor, processor_customer_id, payment_fingerprint,
                    product_key, is_recurring, sale_group_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                           ?, 'stripe', ?, ?, ?, ?, ?)""",
                (order_id, ts, str(gross), str(net), str(refunds), "0", "0", str(fees),
                 customer_key, subscription_id,
                 tracking["session_id"], tracking["visitor_id"], tracking["channel"],
                 tracking["platform"], tracking["campaign_id"], tracking["adset_id"],
                 tracking["ad_id"], tracking["creative_id"], tracking["gclid"],
                 tracking["fbclid"], tracking["ttclid"], currency,
                 processor_customer_id, fingerprint, product_key, int(is_recurring), sale_group_id),
            )
            if cur.rowcount and cur.rowcount > 0:
                inserted += 1
            else:
                # Already synced: refresh refunds/net so refunds issued after the
                # first sync actually reduce reported revenue.
                conn.execute(
                    """UPDATE orders SET ts = ?, gross = ?, refunds = ?, net = ?, fees = ?,
                           customer_key = ?, subscription_id = ?, session_id = ?, visitor_id = ?,
                           channel = ?, platform = ?, campaign_id = ?, adset_id = ?, ad_id = ?,
                           creative_id = ?, gclid = ?, fbclid = ?, ttclid = ?, currency = ?,
                           processor = 'stripe', processor_customer_id = ?, payment_fingerprint = ?,
                           product_key = ?, is_recurring = ?, sale_group_id = ?
                       WHERE order_id = ?""",
                    (
                        ts, str(gross), str(refunds), str(net), str(fees), customer_key,
                        subscription_id, tracking["session_id"], tracking["visitor_id"],
                        tracking["channel"], tracking["platform"], tracking["campaign_id"],
                        tracking["adset_id"], tracking["ad_id"], tracking["creative_id"],
                        tracking["gclid"], tracking["fbclid"], tracking["ttclid"], currency,
                        processor_customer_id, fingerprint, product_key, int(is_recurring),
                        sale_group_id, order_id,
                    ),
                )
                updated += 1

            # Insert Purchase conversion (canonical id shared with the webhook path).
            conv_id = _sha256(f"conv|{order_id}")
            conn.execute(
                """INSERT OR IGNORE INTO conversions
                   (conversion_id, ts, type, value, order_id, customer_key, session_id, visitor_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    conv_id,
                    ts,
                    "Purchase",
                    str(gross),
                    order_id,
                    customer_key,
                    tracking["session_id"],
                    tracking["visitor_id"],
                ),
            )

        conn.commit()

    return {"inserted": inserted, "updated": updated, "recurring": recurring}


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
        start_day = _parse_calendar_date(start_date)
        end_day = _parse_calendar_date(end_date)
        if start_day > end_day:
            start_day, end_day = end_day, start_day
        start_date = start_day.isoformat()
        end_date = end_day.isoformat()

        db_path = _db()
        _ensure_orders_table(db_path)

        history_ranges: list[tuple[str, str]] = []
        history_fetched = 0
        history_synced = 0
        history_updated = 0
        history_days = _history_backfill_days()
        if history_days:
            history_start = (start_day - timedelta(days=history_days)).isoformat()
            history_end = (start_day - timedelta(days=1)).isoformat()
            history_ranges = _missing_stripe_coverage(db_path, history_start, history_end)
            for range_start, range_end in history_ranges:
                history_charges = await _fetch_stripe_charges(api_key, range_start, range_end)
                history_stats = _write_stripe_orders(db_path, history_charges)
                history_fetched += len(history_charges)
                history_synced += history_stats["inserted"]
                history_updated += history_stats["updated"]
                _record_stripe_coverage(db_path, range_start, range_end)

        charges = await _fetch_stripe_charges(api_key, start_date, end_date)
        stats = _write_stripe_orders(db_path, charges)
        _record_stripe_coverage(db_path, start_date, end_date)
        return {
            "synced": stats["inserted"],
            "updated": stats["updated"],
            "recurring": stats["recurring"],
            "fetched": len(charges),
            "history_fetched": history_fetched,
            "history_synced": history_synced,
            "history_updated": history_updated,
            "history_ranges": [
                {"start_date": range_start, "end_date": range_end}
                for range_start, range_end in history_ranges
            ],
            "start_date": start_date,
            "end_date": end_date,
            "query_start_utc": datetime.fromtimestamp(_date_to_unix(start_date), tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "query_end_utc": datetime.fromtimestamp(_date_to_unix_end(end_date), tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
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
