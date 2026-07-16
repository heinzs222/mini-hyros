"""GoHighLevel (LeadConnector) API sync — pull leads, opportunities and booked
calls from the GHL API and store them as conversions/orders in the warehouse.

This is the *pull* counterpart to the inbound webhook handler in ``ghl.py``. The
webhook reacts to events in real time; this sync backfills/refreshes from the GHL
API for a date range so the Leads tab, funnel Leads and Cost-per-Lead populate
even before any webhook fires.

Credentials (a LeadConnector v2 Private Integration token + Location ID) are
stored in the shared ``platform_tokens`` table under platform='ghl', with an
environment-variable fallback (GHL_API_TOKEN / GHL_LOCATION_ID).

Endpoints (mounted at /api/ghl):
  POST /api/ghl/connect  — save + validate an API token and location id
  GET  /api/ghl/status   — connection status + lead/contact counts
  POST /api/ghl/sync     — pull contacts/opportunities/appointments for a range
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, Query, Request

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from attributionops.config import default_db_path
from attributionops.db import connect, sql_rows as db_query
from attributionops.util import try_parse_iso_ts, utc_ts_to_local_date

logger = logging.getLogger("ghl_sync")

# Reuse the webhook write-helpers so GHL leads stitch to the same customer_key
# (sha256 of email, or the shared normalized-phone key) as Stripe orders and
# the tracking pixel.
from api.ghl import (
    _sha256,
    _iso_ts,
    _lead_conversion_id,
    _migrate_legacy_lead_conversion,
    _resolve_platform,
    _insert_conversion,
    _insert_touchpoint,
    normalized_phone_key,
)

router = APIRouter()
UTC = timezone.utc

GHL_API_BASE = "https://services.leadconnectorhq.com"
GHL_API_VERSION = "2021-07-28"
HTTP_TIMEOUT = 30.0

LEAD_TYPE = "Lead"
BOOKING_TYPE = "Booking"
PURCHASE_TYPE = "Purchase"


def _db() -> str:
    return os.environ.get("ATTRIBUTIONOPS_DB_PATH", default_db_path())


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── Credential storage (shared platform_tokens table) ──────────────────────────

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


def get_ghl_credentials(db_path: str) -> tuple[str, str]:
    """Return (api_token, location_id), preferring the DB then env vars."""
    _ensure_tokens_table(db_path)
    rows = db_query(
        db_path,
        "SELECT access_token, advertiser_id FROM platform_tokens WHERE platform='ghl'",
    )
    if rows and rows[0].get("access_token"):
        token = str(rows[0]["access_token"])
        location_id = str(rows[0].get("advertiser_id") or os.environ.get("GHL_LOCATION_ID", ""))
        return token, location_id
    token = (os.environ.get("GHL_API_TOKEN", "") or os.environ.get("GHL_ACCESS_TOKEN", "")).strip()
    location_id = os.environ.get("GHL_LOCATION_ID", "").strip()
    return token, location_id


def save_ghl_credentials(db_path: str, token: str, location_id: str) -> None:
    _ensure_tokens_table(db_path)
    with connect(db_path) as conn:
        conn.execute(
            """INSERT OR REPLACE INTO platform_tokens
               (platform, access_token, refresh_token, advertiser_id, expires_at, updated_at)
               VALUES ('ghl', ?, '', ?, '', ?)""",
            (token.strip(), location_id.strip(), _now()),
        )
        conn.commit()


def _headers(token: str, version: str = GHL_API_VERSION) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Version": version,
        "Accept": "application/json",
    }


# ── Field extraction helpers ───────────────────────────────────────────────────

def _contact_email(contact: dict) -> str:
    email = contact.get("email") or ""
    return str(email).strip().lower()


def _contact_name(contact: dict) -> str:
    name = contact.get("name") or contact.get("contactName") or ""
    if not name:
        first = contact.get("firstName") or ""
        last = contact.get("lastName") or ""
        name = f"{first} {last}".strip()
    return str(name)


def _contact_ts(contact: dict, fallback: str) -> str:
    """Normalise GHL dateAdded (ISO8601 with ms) to our compact UTC format."""
    raw = str(contact.get("dateAdded") or contact.get("createdAt") or contact.get("dateUpdated") or "")
    if not raw:
        return fallback
    # Tolerant parse (handles Hyros-style "…Z UTC-06:00" and naive strings) so a
    # non-standard timestamp no longer silently collapses every lead onto the
    # sync day.
    dt = try_parse_iso_ts(raw)
    if dt is None:
        logger.warning("GHL contact ts unparseable, using fallback: %r", raw)
        return fallback
    return _iso_ts(dt)


def _src_info_from_block(contact: dict, attr: dict | None) -> dict:
    """Map one LeadConnector attribution record to a Vigil source record."""
    if not isinstance(attr, dict):
        attr = {}

    utm_source = str(
        attr.get("utmSource")
        or attr.get("utmSessionSource")
        or attr.get("sessionSource")
        or contact.get("source")
        or ""
    )
    utm_medium = str(attr.get("utmMedium") or attr.get("medium") or "")
    page_url = str(attr.get("pageUrl") or attr.get("url") or "")
    return {
        "utm_source": utm_source,
        "utm_medium": utm_medium,
        "utm_campaign": str(attr.get("campaign") or attr.get("utmCampaign") or ""),
        "utm_content": str(attr.get("utmContent") or ""),
        "utm_term": str(attr.get("utmTerm") or ""),
        "gclid": str(attr.get("gclid") or attr.get("utmGclid") or ""),
        "wbraid": str(attr.get("wbraid") or ""),
        "gbraid": str(attr.get("gbraid") or ""),
        "fbclid": str(attr.get("fbclid") or attr.get("utmFbclid") or ""),
        "ttclid": str(attr.get("ttclid") or attr.get("utmTtclid") or ""),
        "source": str(contact.get("source") or utm_source or ""),
        "referrer": str(attr.get("referrer") or ""),
        "url": page_url,
        "landing_page": page_url,
        "campaign_id": str(attr.get("campaignId") or ""),
        "adset_id": str(attr.get("adSetId") or attr.get("adGroupId") or ""),
        "ad_id": str(attr.get("adId") or ""),
        "creative_id": str(attr.get("creativeId") or ""),
        "fbc_id": str(attr.get("fbc") or ""),
        "ttc_id": str(attr.get("ttc") or ""),
        # gaClientId identifies the browser, not a Google Ads click. Do not map
        # it to gc_id because that would falsely classify organic traffic.
        "gc_id": "",
        "h_ad_id": "",
        "g_special_campaign": str(attr.get("gbraid") or ""),
        "detected_platform": "",
    }


def _attribution_blocks(contact: dict) -> list[dict]:
    """Return GHL attribution history in first-to-last order."""
    blocks = [item for item in (contact.get("attributions") or []) if isinstance(item, dict)]
    for key in ("attributionSource", "lastAttributionSource"):
        item = contact.get(key)
        if isinstance(item, dict) and item:
            blocks.append(item)

    def rank(item: dict) -> int:
        if item.get("isFirst") is True:
            return 0
        if item.get("isLast") is True:
            return 2
        return 1

    return sorted(blocks, key=rank)


def _src_infos_from_attribution(contact: dict) -> list[dict]:
    blocks = _attribution_blocks(contact)
    infos = [_src_info_from_block(contact, block) for block in blocks]
    if not infos:
        infos = [_src_info_from_block(contact, None)]

    deduped: list[dict] = []
    seen: set[tuple[str, ...]] = set()
    keys = (
        "utm_source", "utm_medium", "utm_campaign", "utm_content", "gclid",
        "wbraid", "gbraid", "fbclid", "ttclid", "fbc_id", "campaign_id",
        "adset_id", "ad_id", "url",
    )
    for info in infos:
        signature = tuple(str(info.get(key) or "") for key in keys)
        if signature in seen:
            continue
        seen.add(signature)
        deduped.append(info)
    return deduped


def _src_info_from_attribution(contact: dict) -> dict:
    """Return the latest GHL attribution for the contact/session summary."""
    return _src_infos_from_attribution(contact)[-1]


def _has_attribution(src_info: dict) -> bool:
    return bool(
        src_info.get("utm_source") or src_info.get("utm_medium")
        or src_info.get("gclid")
        or src_info.get("wbraid") or src_info.get("gbraid")
        or src_info.get("fbclid") or src_info.get("ttclid")
        or src_info.get("fbc_id") or src_info.get("ttc_id")
    )


def _ordered_attribution_ts(contact_ts: str, index: int, total: int) -> str:
    """Preserve GHL first/last ordering when attribution rows lack timestamps."""
    dt = try_parse_iso_ts(contact_ts)
    if dt is None:
        return contact_ts
    seconds_before_contact = max(total - index - 1, 0)
    return _iso_ts(dt - timedelta(seconds=seconds_before_contact))


def _within(ts: str, start_date: str, end_date: str) -> bool:
    day = utc_ts_to_local_date(ts) if ts else ""
    if start_date and day < start_date:
        return False
    if end_date and day > end_date:
        return False
    return True


def _submission_ts(submission: dict, fallback: str) -> str:
    raw = str(submission.get("createdAt") or submission.get("dateAdded") or "")
    dt = try_parse_iso_ts(raw) if raw else None
    return _iso_ts(dt) if dt is not None else fallback


def _submission_source_info(submission: dict) -> dict[str, str]:
    others = submission.get("others") or {}
    if not isinstance(others, dict):
        others = {}
    event = others.get("eventData") or submission.get("eventData") or {}
    if not isinstance(event, dict):
        event = {}
    source = str(event.get("source") or event.get("adSource") or "")
    medium = str(event.get("medium") or "form")
    return {
        "utm_source": source,
        "utm_medium": medium,
        "utm_campaign": str(event.get("campaign") or ""),
        "utm_content": str(event.get("utmContent") or ""),
        "gclid": str(event.get("gclid") or ""),
        "fbclid": str(event.get("fbclid") or ""),
        "ttclid": str(event.get("ttclid") or ""),
        "source": source,
        "referrer": str(event.get("referrer") or ""),
        "url": str((event.get("page") or {}).get("url") if isinstance(event.get("page"), dict) else ""),
        "fbc_id": str(event.get("fbc") or ""),
        "ttc_id": "",
        "gc_id": "",
        "h_ad_id": "",
        "g_special_campaign": "",
        "detected_platform": "",
    }


def _write_session(db_path: str, session_id: str, ts: str, src_info: dict,
                   landing: str, customer_key: str) -> None:
    with connect(db_path) as conn:
        conn.execute(
            """INSERT OR IGNORE INTO sessions
               (session_id, ts, utm_source, utm_medium, utm_campaign, utm_content, utm_term,
                referrer, landing_page, device, gclid, fbclid, ttclid, customer_key)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                session_id, ts,
                src_info.get("utm_source", "") or "ghl",
                src_info.get("utm_medium", "") or "crm",
                src_info.get("utm_campaign", ""), src_info.get("utm_content", ""), "",
                src_info.get("referrer", ""), landing or "GHL", "",
                src_info.get("gclid", ""), src_info.get("fbclid", ""),
                src_info.get("ttclid", ""), customer_key,
            ),
        )
        conn.commit()


# ── GHL API fetchers ───────────────────────────────────────────────────────────

async def _fetch_contacts(
    client: httpx.AsyncClient,
    token: str,
    location_id: str,
    limit: int,
    start_date: str = "",
    end_date: str = "",
) -> list[dict]:
    """Fetch contacts in the requested window, following newest-first pages.

    ``limit`` caps matching contacts, not the number of recent contacts scanned.
    This matters for historical windows: filtering the first N recent contacts
    after the fetch silently misses older leads.
    """
    contacts: list[dict] = []
    params: dict[str, Any] = {"locationId": location_id, "limit": min(limit, 100)}
    page_url = f"{GHL_API_BASE}/contacts/"
    guard = 0
    while page_url and len(contacts) < limit and guard < 250:
        guard += 1
        resp = await client.get(page_url, headers=_headers(token), params=params)
        resp.raise_for_status()
        data = resp.json()
        batch = list(data.get("contacts") or [])

        batch_days: list[str] = []
        for contact in batch:
            ts = _contact_ts(contact, "")
            day = utc_ts_to_local_date(ts) if ts else ""
            if day:
                batch_days.append(day)
            if _within(ts, start_date, end_date):
                contacts.append(contact)
                if len(contacts) >= limit:
                    break

        meta = data.get("meta") or {}
        next_url = meta.get("nextPageUrl")
        if not batch or not next_url:
            break
        # The contacts endpoint is newest-first. Once an entire page reaches
        # before the requested window, subsequent pages cannot contain matches.
        if start_date and batch_days and max(batch_days) < start_date:
            break
        page_url, params = next_url, {}
    return contacts[:limit]


async def _fetch_form_submissions(
    client: httpx.AsyncClient,
    token: str,
    location_id: str,
    limit: int,
    start_date: str,
    end_date: str,
) -> list[dict]:
    """Fetch every form submission in the requested local-date window.

    HighLevel exposes submissions separately from contacts. A contact is a
    person; a submission is an opt-in event, and the same person may submit
    more than once. Keeping both is required for Hyros-compatible Leads,
    New Leads and Leads Opt-ins metrics.
    """
    submissions: list[dict] = []
    page = 1
    while len(submissions) < limit and page <= 250:
        params = {
            "locationId": location_id,
            "page": page,
            "limit": min(100, limit),
        }
        resp = await client.get(
            f"{GHL_API_BASE}/forms/submissions",
            headers=_headers(token),
            params=params,
        )
        # The current docs identify this as the v3 API. Older private tokens
        # still expect the dated version header, so retry only on a version-
        # shaped validation response instead of making the whole sync brittle.
        if resp.status_code in {400, 422}:
            retry = await client.get(
                f"{GHL_API_BASE}/forms/submissions",
                headers=_headers(token, "v3"),
                params=params,
            )
            if retry.status_code < 400:
                resp = retry
        resp.raise_for_status()
        data = resp.json()
        batch = list(data.get("submissions") or [])
        batch_days: list[str] = []
        for submission in batch:
            ts = _submission_ts(submission, "")
            if not ts:
                continue
            day = utc_ts_to_local_date(ts)
            batch_days.append(day)
            if _within(ts, start_date, end_date):
                submissions.append(submission)
                if len(submissions) >= limit:
                    break

        meta = data.get("meta") or {}
        next_page = meta.get("nextPage")
        # The endpoint is newest-first. Its startAt/endAt parameters are not
        # consistently honored, so filter locally and keep paging until the
        # requested window has been crossed. Newer rows must not consume the
        # requested match limit.
        entirely_before_window = bool(
            start_date and batch_days and max(batch_days) < start_date
        )
        if not batch or entirely_before_window or next_page in (None, "", False):
            break
        try:
            page = int(next_page)
        except (TypeError, ValueError):
            page += 1
    return submissions[:limit]


async def _fetch_opportunities(client: httpx.AsyncClient, token: str, location_id: str,
                               limit: int) -> list[dict]:
    """Fetch up to ``limit`` opportunities, following pagination (was first-page only)."""
    opps: list[dict] = []
    params: dict[str, Any] = {"location_id": location_id, "limit": min(limit, 100)}
    page_url: str | None = f"{GHL_API_BASE}/opportunities/search"
    guard = 0
    while page_url and len(opps) < limit and guard < 50:
        guard += 1
        resp = await client.get(page_url, headers=_headers(token), params=params)
        resp.raise_for_status()
        data = resp.json()
        batch = list(data.get("opportunities") or [])
        opps.extend(batch)
        meta = data.get("meta") or {}
        next_url = meta.get("nextPageUrl")
        if not batch or not next_url:
            break
        page_url, params = next_url, {}
    return opps[:limit]


# ── Writers ────────────────────────────────────────────────────────────────────

def _write_contacts(db_path: str, contacts: list[dict], start_date: str,
                    end_date: str) -> dict[str, int]:
    leads = 0
    skipped = 0
    now = _now()
    for c in contacts:
        email = _contact_email(c)
        # Phone-only contacts are a real lead segment; skip only when the
        # contact carries NEITHER identity.
        phone_key = normalized_phone_key(str(c.get("phone") or ""))
        if not email and not phone_key:
            skipped += 1
            continue
        ts = _contact_ts(c, now)
        if not _within(ts, start_date, end_date):
            skipped += 1
            continue

        customer_key = _sha256(email) if email else phone_key
        contact_id = str(c.get("id") or customer_key)
        src_infos = _src_infos_from_attribution(c)
        src_info = src_infos[-1]

        # Same canonical key as the webhook path so a lead captured by both the
        # webhook and this sync is counted once, not twice. Keyed per local day
        # so a later-day re-engagement is a new Lead event; rows written under
        # the older contact-only key are rekeyed in place first.
        _migrate_legacy_lead_conversion(db_path, contact_id)
        conv_id = _lead_conversion_id(contact_id, ts)
        _insert_conversion(db_path, conv_id, ts, LEAD_TYPE, 0.0, conv_id, customer_key)

        session_id = _sha256(f"ghl_api_session|{contact_id}")
        _write_session(db_path, session_id, ts, src_info,
                       src_info.get("url", "") or "GHL Form", customer_key)
        # This session id belongs exclusively to this importer. Replacing its
        # touchpoints keeps recurring syncs idempotent while allowing GHL's
        # attribution history to be refreshed.
        with connect(db_path) as conn:
            conn.execute("DELETE FROM touchpoints WHERE session_id = ?", (session_id,))
            conn.commit()
        for index, info in enumerate(src_infos):
            if not _has_attribution(info):
                continue
            info_platform, info_channel = _resolve_platform(
                info["utm_source"], info["utm_medium"], info["source"], src_info=info,
            )
            attr_ts = _ordered_attribution_ts(ts, index, len(src_infos))
            _insert_touchpoint(
                db_path,
                attr_ts,
                info_channel or "crm",
                info_platform,
                info,
                customer_key,
                session_id,
            )
        leads += 1
    return {"leads": leads, "skipped": skipped}


def _write_form_submissions(
    db_path: str,
    submissions: list[dict],
    start_date: str,
    end_date: str,
    contacts_by_id: dict[str, dict] | None = None,
) -> dict[str, int]:
    optins = 0
    skipped = 0
    now = _now()
    contact_map = contacts_by_id or {}
    for submission in submissions:
        ts = _submission_ts(submission, now)
        if not _within(ts, start_date, end_date):
            skipped += 1
            continue

        contact_id = str(submission.get("contactId") or "").strip()
        contact = contact_map.get(contact_id) or {}
        email = str(submission.get("email") or _contact_email(contact) or "").strip().lower()
        customer_key = _sha256(email) if email else ""
        submission_id = str(submission.get("id") or "").strip()
        if not submission_id:
            skipped += 1
            continue

        conv_id = _sha256(f"ghl_submission|{submission_id}")
        _insert_conversion(db_path, conv_id, ts, "OptIn", 0.0, conv_id, customer_key)

        src_info = _submission_source_info(submission)
        platform, channel = _resolve_platform(
            src_info["utm_source"], src_info["utm_medium"], src_info["source"], src_info=src_info,
        )
        session_id = _sha256(f"ghl_submission_session|{submission_id}")
        form_name = str(submission.get("name") or submission.get("formId") or "GHL Form")
        _write_session(
            db_path,
            session_id,
            ts,
            src_info,
            src_info.get("url", "") or form_name,
            customer_key,
        )
        if _has_attribution(src_info):
            _insert_touchpoint(
                db_path,
                ts,
                channel or "crm",
                platform,
                src_info,
                customer_key,
                session_id,
            )
        optins += 1
    return {"optins": optins, "skipped": skipped}


def _write_opportunities(db_path: str, opportunities: list[dict], start_date: str,
                         end_date: str) -> dict[str, int]:
    orders = 0
    open_opps = 0
    skipped = 0
    now = _now()
    won_stages = {"won", "closed won", "closedwon", "paid", "sale", "customer", "purchased"}
    for opp in opportunities:
        ts_raw = str(opp.get("createdAt") or opp.get("dateAdded") or "")
        _dt = try_parse_iso_ts(ts_raw) if ts_raw else None
        ts = _iso_ts(_dt) if _dt is not None else now
        if not _within(ts, start_date, end_date):
            skipped += 1
            continue

        contact = opp.get("contact") or {}
        email = str(contact.get("email") or "").strip().lower()
        customer_key = _sha256(email) if email else ""
        opp_id = str(opp.get("id") or "")
        status = str(opp.get("status") or "").lower()
        try:
            value = float(opp.get("monetaryValue") or 0)
        except (ValueError, TypeError):
            value = 0.0

        if status in won_stages and value > 0:
            # Canonical keys shared with the webhook path (ghl.py) so the same won
            # opportunity isn't double-counted when both paths run.
            order_id = _sha256(f"ghl_opp|{opp_id}")
            with connect(db_path) as conn:
                conn.execute(
                    """INSERT OR IGNORE INTO orders
                       (order_id, ts, gross, net, refunds, chargebacks, cogs, fees, customer_key, subscription_id)
                       VALUES (?, ?, ?, ?, '0', '0', '0', ?, ?, '')""",
                    (order_id, ts, str(round(value, 2)), str(round(value, 2)),
                     str(round(value * 0.029 + 0.30, 2)), customer_key),
                )
                conn.commit()
            _insert_conversion(db_path, _sha256(f"ghl_oppconv|{opp_id}"),
                               ts, PURCHASE_TYPE, value, order_id, customer_key)
            orders += 1
        else:
            _insert_conversion(db_path, _sha256(f"ghl_oppopen|{opp_id}"),
                               ts, "Opportunity", value, _sha256(f"ghl_oppopen|{opp_id}"), customer_key)
            open_opps += 1
    return {"orders": orders, "open_opportunities": open_opps, "skipped": skipped}


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.post("/connect")
async def ghl_connect(request: Request):
    """Save and validate a GHL API token + Location ID."""
    body: dict[str, Any] = {}
    try:
        body = await request.json()
    except Exception:
        body = {}

    token = str(body.get("api_token") or body.get("token") or "").strip()
    location_id = str(body.get("location_id") or body.get("locationId") or "").strip()
    if not token or not location_id:
        return {"connected": False, "error": "Both api_token and location_id are required."}

    # Validate against the GHL API before persisting.
    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            resp = await client.get(
                f"{GHL_API_BASE}/locations/{location_id}",
                headers=_headers(token),
            )
        if resp.status_code in (401, 403):
            return {"connected": False, "error": "Token rejected by GHL (invalid or missing scope)."}
        if resp.status_code == 404:
            return {"connected": False, "error": "Location not found for this token."}
        if resp.status_code >= 400:
            return {"connected": False, "error": f"GHL API error {resp.status_code}: {resp.text[:200]}"}
    except Exception as exc:
        return {"connected": False, "error": f"Could not reach GHL API: {exc}"}

    save_ghl_credentials(_db(), token, location_id)
    return {"connected": True, "location_id": location_id}


@router.get("/status")
async def ghl_status():
    """Return GHL connection status + lead counts already in the warehouse."""
    db_path = _db()
    token, location_id = get_ghl_credentials(db_path)
    if not token or not location_id:
        return {"connected": False, "configured": False,
                "error": "GHL API token / location id not set."}

    leads_in_db = 0
    last_lead_ts = None
    try:
        rows = db_query(
            db_path,
            """SELECT COUNT(*) AS cnt, MAX(ts) AS last_ts FROM conversions
               WHERE LOWER(type) IN ('lead','formsubmission','form_submission','signup','booking')""",
        )
        if rows:
            leads_in_db = int(rows[0].get("cnt") or 0)
            last_lead_ts = rows[0].get("last_ts")
    except Exception:
        pass

    connected = False
    detail = ""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{GHL_API_BASE}/locations/{location_id}", headers=_headers(token)
            )
        connected = resp.status_code == 200
        if not connected:
            detail = f"GHL API returned {resp.status_code}"
    except Exception as exc:
        detail = f"Could not reach GHL API: {exc}"

    return {
        "connected": connected,
        "configured": True,
        "location_id": location_id,
        "leads_in_db": leads_in_db,
        "last_lead_ts": last_lead_ts,
        "token_prefix": token[:8] + "..." if token else "",
        "detail": detail,
    }


@router.post("/sync")
async def ghl_sync(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    limit: int = Query(default=500, ge=1, le=2000),
    include_forms: bool = Query(default=True),
    include_opportunities: bool = Query(default=True),
):
    """Pull GHL contacts (leads) + opportunities for a date range into the warehouse."""
    db_path = _db()
    token, location_id = get_ghl_credentials(db_path)
    if not token or not location_id:
        return {"synced": 0, "leads": 0, "orders": 0,
                "error": "GHL API token / location id not set. Connect GHL first."}

    if not end_date:
        end_date = datetime.now(UTC).strftime("%Y-%m-%d")
    if not start_date:
        start_date = (datetime.now(UTC) - timedelta(days=90)).strftime("%Y-%m-%d")

    errors: list[str] = []
    lead_stats = {"leads": 0, "skipped": 0}
    form_stats = {"optins": 0, "skipped": 0}
    opp_stats = {"orders": 0, "open_opportunities": 0, "skipped": 0}

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            async def _empty_result() -> list[dict]:
                return []

            contacts_result, submissions_result, opportunities_result = await asyncio.gather(
                _fetch_contacts(
                    client, token, location_id, limit, start_date=start_date, end_date=end_date
                ),
                _fetch_form_submissions(
                    client, token, location_id, limit, start_date, end_date
                ) if include_forms else _empty_result(),
                _fetch_opportunities(client, token, location_id, limit)
                if include_opportunities else _empty_result(),
                return_exceptions=True,
            )

            contacts: list[dict] = []
            if isinstance(contacts_result, httpx.HTTPStatusError):
                errors.append(
                    f"contacts: HTTP {contacts_result.response.status_code} "
                    f"{contacts_result.response.text[:160]}"
                )
            elif isinstance(contacts_result, Exception):
                errors.append(f"contacts: {contacts_result}")
            else:
                contacts = contacts_result
                lead_stats = _write_contacts(db_path, contacts, start_date, end_date)

            if isinstance(submissions_result, httpx.HTTPStatusError):
                errors.append(
                    f"forms: HTTP {submissions_result.response.status_code} "
                    f"{submissions_result.response.text[:160]}"
                )
            elif isinstance(submissions_result, Exception):
                errors.append(f"forms: {submissions_result}")
            else:
                contacts_by_id = {
                    str(contact.get("id") or ""): contact
                    for contact in contacts
                    if contact.get("id")
                }
                form_stats = _write_form_submissions(
                    db_path, submissions_result, start_date, end_date, contacts_by_id
                )

            if isinstance(opportunities_result, httpx.HTTPStatusError):
                errors.append(
                    f"opportunities: HTTP {opportunities_result.response.status_code} "
                    f"{opportunities_result.response.text[:160]}"
                )
            elif isinstance(opportunities_result, Exception):
                errors.append(f"opportunities: {opportunities_result}")
            else:
                opp_stats = _write_opportunities(
                    db_path, opportunities_result, start_date, end_date
                )
    except Exception as exc:
        return {"synced": 0, "leads": 0, "orders": 0, "error": str(exc)}

    return {
        "synced": lead_stats["leads"] + form_stats["optins"] + opp_stats["orders"],
        "leads": lead_stats["leads"],
        "optins": form_stats["optins"],
        "orders": opp_stats["orders"],
        "open_opportunities": opp_stats["open_opportunities"],
        "skipped": lead_stats["skipped"] + form_stats["skipped"] + opp_stats["skipped"],
        "start_date": start_date,
        "end_date": end_date,
        "include_forms": include_forms,
        "include_opportunities": include_opportunities,
        "errors": errors,
    }
