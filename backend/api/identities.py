"""Backfill the names and emails behind rows already in the warehouse.

``customer_key`` is a hash, and the identity behind it is only recorded when a
row is ingested. Everything ingested before identities were captured therefore
renders in the CRM as a hash, and no ordinary sync repairs it: the Stripe sync
skips ranges its coverage ledger already marks as fetched, and lead writers skip
contacts outside the reporting window.

This re-reads the sources for a window and writes identities only — no orders,
no conversions, no coverage bookkeeping — so it is safe to run at any time and
cannot double-count revenue.

Endpoints:
  POST /api/crm/backfill-identities  — pull identities from Stripe + GHL
  GET  /api/crm/identity-coverage    — how many CRM rows currently have a name
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Query

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from attributionops.config import default_db_path
from attributionops.db import connect, sql_rows
from attributionops.schema import ensure_customer_identities, upsert_customer_identity

router = APIRouter()
logger = logging.getLogger("identities")
UTC = timezone.utc


# Backfill reads a year by default; cap the contact scan so one call cannot run
# unbounded against a large CRM.
_CONTACT_LIMIT = 5000


def _db() -> str:
    return default_db_path()


def _normalize_window(start_date: str, end_date: str, default_days: int) -> tuple[str, str]:
    today = datetime.now(UTC).date()
    end = end_date.strip() or today.isoformat()
    start = start_date.strip() or (today - timedelta(days=default_days)).isoformat()
    if start > end:
        start, end = end, start
    return start, end


def _persist(rows: list[dict[str, str]], source: str) -> int:
    if not rows:
        return 0
    db_path = _db()
    written = 0
    with connect(db_path) as conn:
        ensure_customer_identities(conn)
        for row in rows:
            if upsert_customer_identity(
                conn,
                row.get("customer_key", ""),
                email=row.get("email", ""),
                name=row.get("name", ""),
                phone=row.get("phone", ""),
                source=source,
                updated_at=row.get("ts") or None,
            ):
                written += 1
        conn.commit()
    return written


async def _stripe_identities(start_date: str, end_date: str) -> dict[str, Any]:
    from backend.api.stripe_sync import (
        _extract_email_from_charge,
        _extract_name_from_charge,
        _extract_phone_from_charge,
        _fetch_stripe_charges,
        _get_stripe_key,
    )

    api_key = _get_stripe_key()
    if not api_key:
        return {"skipped": True, "reason": "STRIPE_API_SECRET_KEY not set"}

    charges = await _fetch_stripe_charges(api_key, start_date, end_date)
    rows: list[dict[str, str]] = []
    with connect(_db()) as conn:
        for charge in charges:
            email = _extract_email_from_charge(charge)
            name = _extract_name_from_charge(charge)
            phone = _extract_phone_from_charge(charge)
            if not (email or name or phone):
                continue
            # Match the key the order was stored under rather than recomputing
            # it, so an alias-resolved customer still lines up.
            customer_key = _order_key_for_charge(conn, charge)
            if not customer_key:
                continue
            rows.append({
                "customer_key": customer_key,
                "email": email,
                "name": name,
                "phone": phone,
            })

    return {"fetched": len(charges), "written": _persist(rows, "stripe_backfill")}


def _order_key_for_charge(conn: Any, charge: dict) -> str:
    """The customer_key the warehouse already stored for this charge."""
    from backend.api.stripe_sync import (
        _customer_key_from_charge,
        _extract_email_from_charge,
        _stripe_order_id,
    )

    order_id = _stripe_order_id(charge)
    try:
        row = conn.execute(
            "SELECT customer_key FROM orders WHERE order_id = ?", (order_id,)
        ).fetchone()
    except Exception:
        row = None
    if row is not None:
        stored = str((row["customer_key"] if isinstance(row, dict) else row[0]) or "")
        if stored:
            return stored
    return _customer_key_from_charge(charge, _extract_email_from_charge(charge))


async def _ghl_identities(start_date: str, end_date: str) -> dict[str, Any]:
    import httpx

    from backend.api.ghl_sync import (
        HTTP_TIMEOUT,
        _contact_email,
        _contact_name,
        _fetch_contacts,
        get_ghl_credentials,
    )
    from backend.api.ghl import _sha256, normalized_phone_key

    token, location_id = get_ghl_credentials(_db())
    if not token or not location_id:
        return {"skipped": True, "reason": "GHL credentials not set"}

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        contacts = await _fetch_contacts(
            client, token, location_id, _CONTACT_LIMIT, start_date, end_date
        )
    rows: list[dict[str, str]] = []
    for contact in contacts:
        email = _contact_email(contact)
        phone = str(contact.get("phone") or "")
        customer_key = _sha256(email) if email else normalized_phone_key(phone)
        if not customer_key:
            continue
        rows.append({
            "customer_key": customer_key,
            "email": email,
            "name": _contact_name(contact),
            "phone": phone,
        })

    return {"fetched": len(contacts), "written": _persist(rows, "ghl_backfill")}


@router.post("/backfill-identities")
async def backfill_identities(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    days: int = Query(default=365, ge=1, le=3650),
):
    """Re-read Stripe and GHL for a window and record who each customer is."""
    start, end = _normalize_window(start_date, end_date, days)

    results: dict[str, Any] = {"start_date": start, "end_date": end, "sources": {}}
    for name, runner in (("stripe", _stripe_identities), ("ghl", _ghl_identities)):
        try:
            results["sources"][name] = await runner(start, end)
        except Exception as exc:
            logger.exception("%s identity backfill failed", name)
            results["sources"][name] = {"error": str(exc)}

    results["written"] = sum(
        int(info.get("written", 0) or 0)
        for info in results["sources"].values()
        if isinstance(info, dict)
    )
    return results


@router.get("/identity-coverage")
async def identity_coverage():
    """How many known customers currently resolve to a name or email."""
    db_path = _db()
    with connect(db_path) as conn:
        ensure_customer_identities(conn)
        conn.commit()

    def _count(sql: str) -> int:
        try:
            rows = sql_rows(db_path, sql)
            return int((rows[0] or {}).get("n") or 0) if rows else 0
        except Exception:
            return 0

    known = _count(
        "SELECT COUNT(*) AS n FROM customer_identities "
        "WHERE COALESCE(name, '') != '' OR COALESCE(email, '') != ''"
    )
    total = _count(
        "SELECT COUNT(DISTINCT customer_key) AS n FROM conversions "
        "WHERE COALESCE(customer_key, '') != ''"
    )
    return {
        "customers_with_identity": known,
        "customers_total": total,
        "missing": max(total - known, 0),
    }
