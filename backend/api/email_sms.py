"""Email/SMS Sequence Attribution — track which email or SMS led to a purchase.

Endpoints:
  POST /api/email-sms/track      — record an email/SMS touchpoint (open, click, etc.)
  GET  /api/email-sms/attribution — attribution by email/SMS sequence
  GET  /api/email-sms/summary     — overall email/SMS performance
"""

from __future__ import annotations

import hashlib
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Request, Query

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
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def _ensure_email_sms_table(db_path: str) -> None:
    with connect(db_path) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS email_sms_events (
            id TEXT PRIMARY KEY,
            ts TEXT,
            customer_key TEXT,
            channel TEXT,
            event_type TEXT,
            sequence_name TEXT,
            step_number TEXT,
            subject_line TEXT,
            link_url TEXT,
            source TEXT
        )""")
        conn.commit()


@router.post("/track")
async def track_email_sms(request: Request):
    """Record an email or SMS touchpoint.
    
    Body: {
        email: "user@example.com",    // or customer_key
        channel: "email" | "sms",
        event_type: "sent" | "opened" | "clicked" | "replied",
        sequence_name: "Welcome Sequence",
        step_number: 3,
        subject_line: "Your offer expires...",
        link_url: "https://...",
        source: "ghl" | "mailchimp" | "klaviyo" | ...
    }
    """
    payload = await request.json()
    db_path = _db()
    now = _iso_ts(datetime.now(UTC))

    email = str(payload.get("email", "")).strip().lower()
    customer_key = payload.get("customer_key", "")
    if email and "@" in email:
        customer_key = _sha256(email)
    elif not customer_key:
        return {"ok": False, "error": "email or customer_key required"}

    channel = str(payload.get("channel", "email")).lower()
    event_type = str(payload.get("event_type", "clicked")).lower()
    sequence_name = str(payload.get("sequence_name", ""))
    step_number = str(payload.get("step_number", ""))
    subject_line = str(payload.get("subject_line", ""))
    link_url = str(payload.get("link_url", ""))
    source = str(payload.get("source", ""))

    event_id = _sha256(f"es|{customer_key}|{channel}|{event_type}|{sequence_name}|{step_number}|{now}")

    _ensure_email_sms_table(db_path)

    with connect(db_path) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO email_sms_events (id, ts, customer_key, channel, event_type, sequence_name, step_number, subject_line, link_url, source) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (event_id, now, customer_key, channel, event_type, sequence_name, step_number, subject_line, link_url, source),
        )

        # If it's a click, also insert as a touchpoint for attribution
        if event_type in ("clicked", "click"):
            session_id = _sha256(f"es_session|{customer_key}|{now}")
            conn.execute(
                """INSERT INTO touchpoints (ts, channel, platform, campaign_id, adset_id, ad_id, creative_id, gclid, fbclid, ttclid, customer_key, session_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (now, channel, "", sequence_name, "", "", "", "", "", "", customer_key, session_id),
            )
            conn.execute(
                """INSERT INTO sessions (session_id, ts, utm_source, utm_medium, utm_campaign, utm_content, utm_term, referrer, landing_page, device, gclid, fbclid, ttclid, customer_key)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (session_id, now, source or "email", channel, sequence_name, subject_line, "", "", link_url, "", "", "", "", customer_key),
            )

        conn.commit()

    # Broadcast
    from main import manager
    import asyncio
    asyncio.create_task(manager.broadcast({
        "type": "email_sms_event",
        "channel": channel,
        "event_type": event_type,
        "sequence_name": sequence_name,
        "customer_key": customer_key,
        "ts": now,
    }))

    return {"ok": True, "event_id": event_id}


@router.get("/attribution")
async def email_sms_attribution(
    channel: str = Query(default="", description="email, sms, or empty for both"),
):
    """Attribution by email/SMS sequence — which sequences drove the most revenue."""
    db_path = _db()
    _ensure_email_sms_table(db_path)

    where = ""
    params = []
    if channel:
        where = " AND e.channel = ?"
        params.append(channel)

    try:
        rows = db_query(db_path, f"""
            SELECT e.sequence_name,
                   e.channel,
                   COUNT(DISTINCT CASE WHEN e.event_type = 'sent' THEN e.id END) as sent,
                   COUNT(DISTINCT CASE WHEN e.event_type = 'opened' THEN e.id END) as opened,
                   COUNT(DISTINCT CASE WHEN e.event_type IN ('clicked', 'click') THEN e.id END) as clicked,
                   COUNT(DISTINCT e.customer_key) as unique_contacts
            FROM email_sms_events e
            WHERE e.sequence_name != '' {where}
            GROUP BY e.sequence_name, e.channel
            ORDER BY clicked DESC
        """, params)

        # Enrich with revenue data
        for row in rows:
            seq = row["sequence_name"]
            # Find customers who clicked this sequence and then purchased
            clickers = db_query(db_path, """
                SELECT DISTINCT e.customer_key
                FROM email_sms_events e
                WHERE e.sequence_name = ? AND e.event_type IN ('clicked', 'click')
            """, [seq])

            ck_set = {c["customer_key"] for c in clickers}
            if ck_set:
                ck_list = list(ck_set)
                placeholders = ",".join(["?"] * len(ck_list))
                rev = db_query(db_path, f"""
                    SELECT COUNT(DISTINCT order_id) as orders,
                           SUM(CAST(gross AS REAL)) as revenue
                    FROM orders WHERE customer_key IN ({placeholders})
                """, ck_list)
                r = rev[0] if rev else {}
                row["attributed_orders"] = int(r.get("orders", 0) or 0)
                row["attributed_revenue"] = round(float(r.get("revenue", 0) or 0), 2)
            else:
                row["attributed_orders"] = 0
                row["attributed_revenue"] = 0

            # Rates
            sent = int(row.get("sent", 0) or 0)
            row["open_rate"] = round(int(row.get("opened", 0) or 0) / max(sent, 1) * 100, 1)
            row["click_rate"] = round(int(row.get("clicked", 0) or 0) / max(sent, 1) * 100, 1)

        return {"rows": rows}
    except Exception:
        return {"rows": []}


@router.get("/summary")
async def email_sms_summary():
    """Overall email/SMS performance."""
    db_path = _db()
    _ensure_email_sms_table(db_path)

    try:
        stats = db_query(db_path, """
            SELECT channel,
                   COUNT(*) as total_events,
                   COUNT(DISTINCT customer_key) as unique_contacts,
                   COUNT(DISTINCT sequence_name) as sequences,
                   COUNT(DISTINCT CASE WHEN event_type = 'sent' THEN id END) as sent,
                   COUNT(DISTINCT CASE WHEN event_type = 'opened' THEN id END) as opened,
                   COUNT(DISTINCT CASE WHEN event_type IN ('clicked', 'click') THEN id END) as clicked
            FROM email_sms_events
            GROUP BY channel
        """)

        return {"channels": stats}
    except Exception:
        return {"channels": []}
