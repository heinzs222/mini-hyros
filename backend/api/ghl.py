"""GoHighLevel (GHL) webhook integration.

Handles all GHL webhook events:
  - ContactCreate / ContactUpdate  → identify visitor
  - OpportunityCreate / OpportunityStageUpdate → track pipeline movement
  - PaymentReceived / InvoicePaid  → track revenue (orders)
  - FormSubmission                 → track leads
  - AppointmentBooked              → track bookings
  - NoteCreate, TaskCreate         → logged but not attributed

Webhook URL:  POST /api/webhooks/ghl
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Request, HTTPException

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from attributionops.config import default_db_path
from attributionops.db import connect, sql_rows as db_query

router = APIRouter()
UTC = timezone.utc


def _ensure_webhook_log_table(db_path: str) -> None:
    with connect(db_path) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS webhook_log (
            id TEXT PRIMARY KEY,
            ts TEXT,
            source TEXT,
            event_type TEXT,
            payload TEXT,
            result TEXT
        )""")
        conn.commit()


def _log_webhook(db_path: str, log_id: str, ts: str, source: str,
                 event_type: str, payload: str, result: str) -> None:
    _ensure_webhook_log_table(db_path)
    with connect(db_path) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO webhook_log (id, ts, source, event_type, payload, result) VALUES (?,?,?,?,?,?)",
            (log_id, ts, source, event_type, payload[:2000], result[:500])
        )
        conn.commit()


def _db() -> str:
    return os.environ.get("ATTRIBUTIONOPS_DB_PATH", default_db_path())


def _iso_ts(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def _extract_email(payload: dict) -> str:
    """Try multiple GHL payload paths to find the contact email."""
    # Direct fields
    email = payload.get("email", "")
    if email:
        return str(email).strip().lower()

    # Nested in contact object
    contact = payload.get("contact", {}) or {}
    email = contact.get("email", "")
    if email:
        return str(email).strip().lower()

    # In custom fields or additional info
    for key in ("contact_email", "customerEmail", "customer_email"):
        if payload.get(key):
            return str(payload[key]).strip().lower()

    return ""


def _extract_name(payload: dict) -> str:
    """Extract contact name from GHL payload."""
    name = payload.get("full_name") or payload.get("name") or ""
    if not name:
        contact = payload.get("contact", {}) or {}
        first = contact.get("firstName", contact.get("first_name", ""))
        last = contact.get("lastName", contact.get("last_name", ""))
        name = f"{first} {last}".strip()
    if not name:
        first = payload.get("firstName", payload.get("first_name", ""))
        last = payload.get("lastName", payload.get("last_name", ""))
        name = f"{first} {last}".strip()
    return name


def _extract_phone(payload: dict) -> str:
    phone = payload.get("phone", "")
    if not phone:
        contact = payload.get("contact", {}) or {}
        phone = contact.get("phone", "")
    return str(phone)


def _extract_value(payload: dict) -> float:
    """Extract monetary value from GHL payment/opportunity payload."""
    for key in ("amount", "value", "monetary_value", "monetaryValue",
                "total_amount", "totalAmount", "price"):
        val = payload.get(key)
        if val is not None:
            try:
                return float(val)
            except (ValueError, TypeError):
                continue
    # Check nested
    for obj_key in ("payment", "invoice", "opportunity"):
        obj = payload.get(obj_key, {}) or {}
        for key in ("amount", "value", "monetary_value", "monetaryValue"):
            val = obj.get(key)
            if val is not None:
                try:
                    return float(val)
                except (ValueError, TypeError):
                    continue
    return 0.0


def _extract_source_info(payload: dict) -> dict:
    """Extract UTM / source info from GHL contact or custom fields."""
    info = {
        "utm_source": "",
        "utm_medium": "",
        "utm_campaign": "",
        "utm_content": "",
        "gclid": "",
        "fbclid": "",
        "ttclid": "",
        "source": "",
        "fbc_id": "",
        "ttc_id": "",
        "gc_id": "",
        "h_ad_id": "",
        "g_special_campaign": "",
        "detected_platform": "",
    }

    # GHL sometimes stores attribution in tags, custom fields, or source field
    info["source"] = str(payload.get("source", "") or "")

    # Check direct UTM fields (some GHL setups pass these)
    for key in ("utm_source", "utm_medium", "utm_campaign", "utm_content",
                "gclid", "fbclid", "ttclid",
                "fbc_id", "ttc_id", "gc_id", "h_ad_id", "g_special_campaign", "detected_platform"):
        val = payload.get(key, "")
        if not val:
            # Check in contact object
            contact = payload.get("contact", {}) or {}
            val = contact.get(key, "")
        if not val:
            # Check in customField array (GHL stores custom fields as array)
            for cf in payload.get("customFields", payload.get("custom_fields", [])) or []:
                if isinstance(cf, dict):
                    cf_key = cf.get("key", cf.get("id", "")).lower().replace(" ", "_")
                    if cf_key == key:
                        val = cf.get("value", cf.get("field_value", ""))
                        break
        info[key] = str(val) if val else ""

    return info


def _resolve_platform(utm_source: str, utm_medium: str, source: str,
                      src_info: dict | None = None) -> tuple[str, str]:
    """Return (platform, channel) from UTM/source info."""
    src = (utm_source or source or "").lower()
    med = (utm_medium or "").lower()

    # Check custom params first
    if src_info:
        dp = (src_info.get("detected_platform") or "").lower()
        fbc_id = src_info.get("fbc_id", "")
        ttc_id = src_info.get("ttc_id", "")
        gc_id = src_info.get("gc_id", "")
        g_special = src_info.get("g_special_campaign", "")
        if dp == "meta" or fbc_id:
            return "meta", "paid_social"
        if dp == "tiktok" or ttc_id:
            return "tiktok", "paid_social"
        if dp == "google" or gc_id or g_special:
            return "google", "paid_search"

    if src in ("facebook", "fb", "meta", "ig", "instagram"):
        return "meta", "paid_social"
    elif src in ("google", "gads", "adwords"):
        return "google", "paid_search"
    elif src in ("tiktok", "tt"):
        return "tiktok", "paid_social"
    elif src in ("youtube", "yt"):
        return "google", "paid_video"
    elif med == "email" or src == "email":
        return "", "email"
    elif src in ("ghl", "highlevel", "go_high_level"):
        return "", "crm"
    elif src:
        return "", "referral"
    return "", "organic"


def _insert_conversion(db_path: str, conv_id: str, ts: str, conv_type: str,
                       value: float, order_id: str, customer_key: str) -> None:
    """Insert a conversion record."""
    with connect(db_path) as conn:
        conn.execute(
            """INSERT OR IGNORE INTO conversions (conversion_id, ts, type, value, order_id, customer_key)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (conv_id, ts, conv_type, str(value), order_id, customer_key),
        )
        conn.commit()


def _insert_order(db_path: str, order: dict[str, Any]) -> None:
    """Insert an order record."""
    with connect(db_path) as conn:
        conn.execute(
            """INSERT OR IGNORE INTO orders (order_id, ts, gross, net, refunds, chargebacks, cogs, fees, customer_key, subscription_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                str(order["order_id"]), str(order["ts"]),
                str(order["gross"]), str(order["net"]),
                str(order.get("refunds", 0)), str(order.get("chargebacks", 0)),
                str(order.get("cogs", 0)), str(order.get("fees", 0)),
                str(order.get("customer_key", "")),
                str(order.get("subscription_id", "")),
            ),
        )
        conn.commit()


def _insert_touchpoint(db_path: str, ts: str, channel: str, platform: str,
                       src_info: dict, customer_key: str, session_id: str) -> None:
    """Insert a touchpoint for attribution."""
    with connect(db_path) as conn:
        conn.execute(
            """INSERT INTO touchpoints (ts, channel, platform, campaign_id, adset_id, ad_id, creative_id, gclid, fbclid, ttclid, customer_key, session_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                ts, channel, platform,
                src_info.get("utm_campaign", ""),
                "", "",  # adset_id, ad_id not typically in GHL webhooks
                src_info.get("utm_content", ""),
                src_info.get("gclid", ""),
                src_info.get("fbclid", ""),
                src_info.get("ttclid", ""),
                customer_key, session_id,
            ),
        )
        conn.commit()


def _broadcast(data: dict) -> None:
    """Broadcast event via WebSocket."""
    from main import manager
    import asyncio
    asyncio.create_task(manager.broadcast(data))


# ── GHL event type mapping ────────────────────────────────────────────────────
# GHL sends a "type" or "event" field. Common ones:
GHL_CONTACT_EVENTS = {"ContactCreate", "ContactUpdate", "contact.create", "contact.update"}
GHL_FORM_EVENTS = {"FormSubmission", "form.submission", "form_submission"}
GHL_BOOKING_EVENTS = {"AppointmentBooked", "appointment.booked", "appointment_booked",
                      "CalendarBooking", "calendar.booking"}
GHL_OPPORTUNITY_EVENTS = {"OpportunityCreate", "OpportunityStageUpdate",
                          "opportunity.create", "opportunity.status_change",
                          "opportunity_stage_update", "opportunity_created"}
GHL_PAYMENT_EVENTS = {"PaymentReceived", "payment.received", "payment_received",
                      "InvoicePaid", "invoice.paid", "invoice_paid",
                      "OrderSubmitted", "order.submitted"}


def _infer_event_type(payload: dict) -> str:
    """Infer the event type from payload content when GHL doesn't send an explicit type.
    GHL workflow webhooks often just send contact data without a type field."""
    # Check for payment/invoice indicators
    if any(k in payload for k in ("amount", "payment_id", "paymentId", "invoice_id", "invoiceId")):
        if _extract_value(payload) > 0:
            return "PaymentReceived"

    # Check for appointment/booking indicators
    if any(k in payload for k in ("startTime", "start_time", "appointmentTime",
                                   "calendarId", "calendar_id", "selectedTimezone")):
        return "AppointmentBooked"

    # Check for opportunity indicators
    if any(k in payload for k in ("pipelineId", "pipeline_id", "pipelineStage",
                                   "pipeline_stage", "opportunity")):
        return "OpportunityCreate"

    # Check for form submission indicators
    if any(k in payload for k in ("form_name", "formName", "form_id", "formId",
                                   "page_name", "pageName", "survey")):
        return "FormSubmission"

    # Check workflow_trigger or trigger_type fields
    trigger = str(payload.get("workflow_trigger", payload.get("triggerType", ""))).lower()
    if "form" in trigger or "survey" in trigger or "submit" in trigger:
        return "FormSubmission"
    if "appointment" in trigger or "booking" in trigger or "calendar" in trigger:
        return "AppointmentBooked"
    if "payment" in trigger or "invoice" in trigger or "order" in trigger:
        return "PaymentReceived"
    if "opportunity" in trigger or "pipeline" in trigger:
        return "OpportunityCreate"

    # If there's an email, treat as contact/lead (most common GHL workflow webhook)
    email = _extract_email(payload)
    if email:
        return "FormSubmission"

    return "unknown"


@router.post("/ghl")
async def ghl_webhook(request: Request):
    """Master GHL webhook endpoint. Handles all event types from GoHighLevel."""
    body = await request.body()
    payload = json.loads(body) if body else {}

    # GHL sends event type in different fields depending on version
    event_type = (
        payload.get("type") or
        payload.get("event") or
        payload.get("eventType") or
        payload.get("webhook_event") or
        ""
    )

    # If no explicit event type, infer from payload content
    if not event_type or event_type == "unknown":
        event_type = _infer_event_type(payload)

    email = _extract_email(payload)
    customer_key = _sha256(email) if email else ""
    name = _extract_name(payload)
    phone = _extract_phone(payload)
    now = _iso_ts(datetime.now(UTC))
    db_path = _db()

    contact_id = str(
        payload.get("contact_id") or
        payload.get("contactId") or
        payload.get("id") or
        (payload.get("contact", {}) or {}).get("id", "") or
        ""
    )

    src_info = _extract_source_info(payload)
    platform, channel = _resolve_platform(
        src_info["utm_source"], src_info["utm_medium"], src_info["source"],
        src_info=src_info
    )

    result = {
        "ok": True,
        "event_type": event_type,
        "contact_id": contact_id,
        "customer_key": customer_key,
    }

    # ── Contact events (identity stitching) ──────────────────────────────
    if event_type in GHL_CONTACT_EVENTS:
        if email:
            # Update any anonymous sessions with this customer_key
            with connect(db_path) as conn:
                conn.execute(
                    "UPDATE sessions SET customer_key = ? WHERE customer_key = '' AND session_id IN "
                    "(SELECT session_id FROM touchpoints WHERE customer_key = '')",
                    (customer_key,),
                )
                conn.execute(
                    "UPDATE touchpoints SET customer_key = ? WHERE customer_key = ''",
                    (customer_key,),
                )
                conn.commit()

            _broadcast({
                "type": "identify",
                "customer_key": customer_key,
                "name": name,
                "source": "ghl",
                "ts": now,
            })

        result["action"] = "contact_identified"
        return result

    # ── Form submission (Lead conversion) ────────────────────────────────
    if event_type in GHL_FORM_EVENTS:
        form_name = str(
            payload.get("form_name") or
            payload.get("formName") or
            payload.get("page_name") or
            "GHL Form"
        )
        conv_id = _sha256(f"ghl_form|{contact_id}|{now}")
        session_id = _sha256(f"ghl_session|{contact_id}|{now}")

        _insert_conversion(db_path, conv_id, now, "Lead", 0, conv_id, customer_key)

        # Always insert touchpoint + session so data shows in dashboard
        _insert_touchpoint(db_path, now, channel or "crm", platform, src_info,
                           customer_key, session_id)
        with connect(db_path) as conn:
            conn.execute(
                """INSERT OR IGNORE INTO sessions (session_id, ts, utm_source, utm_medium, utm_campaign, utm_content, utm_term, referrer, landing_page, device, gclid, fbclid, ttclid, customer_key)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (session_id, now, src_info.get("utm_source", "") or "ghl", src_info.get("utm_medium", "") or "webhook",
                 src_info.get("utm_campaign", ""), src_info.get("utm_content", ""), "",
                 "ghl-webhook", form_name, "",
                 src_info.get("gclid", ""), src_info.get("fbclid", ""),
                 src_info.get("ttclid", ""), customer_key),
            )
            conn.commit()

        _broadcast({
            "type": "new_lead",
            "form_name": form_name,
            "customer_key": customer_key,
            "name": name,
            "source": "ghl",
            "ts": now,
        })

        result["action"] = "lead_tracked"
        result["form_name"] = form_name
        try:
            _log_webhook(db_path, _sha256(f"whlog|{now}|{conv_id}"), now, "ghl",
                         event_type, json.dumps(payload), json.dumps(result))
        except Exception:
            pass
        return result

    # ── Appointment / booking (Booking conversion) ───────────────────────
    if event_type in GHL_BOOKING_EVENTS:
        calendar_name = str(
            payload.get("calendar_name") or
            payload.get("calendarName") or
            payload.get("calendar", {}).get("name", "") or
            "GHL Booking"
        )
        appointment_time = str(
            payload.get("startTime") or
            payload.get("start_time") or
            payload.get("appointmentTime") or
            now
        )
        conv_id = _sha256(f"ghl_booking|{contact_id}|{appointment_time}")
        session_id = _sha256(f"ghl_bsession|{contact_id}|{now}")

        _insert_conversion(db_path, conv_id, now, "Booking", 0, conv_id, customer_key)

        # Always insert touchpoint + session so data shows in dashboard
        _insert_touchpoint(db_path, now, channel or "crm", platform, src_info,
                           customer_key, session_id)
        with connect(db_path) as conn:
            conn.execute(
                """INSERT OR IGNORE INTO sessions (session_id, ts, utm_source, utm_medium, utm_campaign, utm_content, utm_term, referrer, landing_page, device, gclid, fbclid, ttclid, customer_key)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (session_id, now, src_info.get("utm_source", "") or "ghl", src_info.get("utm_medium", "") or "webhook",
                 src_info.get("utm_campaign", ""), src_info.get("utm_content", ""), "",
                 "ghl-webhook", calendar_name, "",
                 src_info.get("gclid", ""), src_info.get("fbclid", ""),
                 src_info.get("ttclid", ""), customer_key),
            )
            conn.commit()

        _broadcast({
            "type": "new_booking",
            "calendar": calendar_name,
            "customer_key": customer_key,
            "name": name,
            "source": "ghl",
            "ts": now,
        })

        result["action"] = "booking_tracked"
        result["calendar"] = calendar_name
        return result

    # ── Opportunity events (pipeline tracking) ───────────────────────────
    if event_type in GHL_OPPORTUNITY_EVENTS:
        opp = payload.get("opportunity", {}) or payload
        stage = str(opp.get("stage", opp.get("pipelineStage", opp.get("status", "unknown"))))
        pipeline = str(opp.get("pipeline", opp.get("pipelineName", "")))
        value = _extract_value(payload)
        opp_id = str(opp.get("id", contact_id))

        conv_type = "Opportunity"
        # If stage indicates a won deal, treat as purchase
        won_stages = {"won", "closed won", "closedwon", "paid", "sale", "customer", "purchased"}
        if stage.lower() in won_stages and value > 0:
            conv_type = "Purchase"
            order_id = _sha256(f"ghl_opp|{opp_id}")
            session_id = _sha256(f"ghl_osession|{contact_id}|{now}")

            _insert_order(db_path, {
                "order_id": order_id,
                "ts": now,
                "gross": round(value, 2),
                "net": round(value, 2),
                "fees": round(value * 0.029 + 0.30, 2),
                "customer_key": customer_key,
            })
            _insert_conversion(db_path, _sha256(f"ghl_oppconv|{opp_id}"),
                               now, "Purchase", value, order_id, customer_key)

            if src_info.get("utm_source") or src_info.get("gclid") or src_info.get("fbclid"):
                _insert_touchpoint(db_path, now, channel, platform, src_info,
                                   customer_key, session_id)

            _broadcast({
                "type": "new_order",
                "order_id": order_id,
                "gross": value,
                "customer_key": customer_key,
                "stage": stage,
                "pipeline": pipeline,
                "source": "ghl",
                "ts": now,
            })
        else:
            conv_id = _sha256(f"ghl_opp_stage|{opp_id}|{stage}")
            _insert_conversion(db_path, conv_id, now, conv_type, value, conv_id, customer_key)

            _broadcast({
                "type": "opportunity_update",
                "stage": stage,
                "pipeline": pipeline,
                "value": value,
                "customer_key": customer_key,
                "source": "ghl",
                "ts": now,
            })

        result["action"] = "opportunity_tracked"
        result["stage"] = stage
        result["value"] = value
        return result

    # ── Payment events (revenue) ─────────────────────────────────────────
    if event_type in GHL_PAYMENT_EVENTS:
        value = _extract_value(payload)
        if value <= 0:
            return {"ok": True, "skipped": True, "reason": "zero_value"}

        payment_id = str(
            payload.get("payment_id") or
            payload.get("paymentId") or
            payload.get("invoice_id") or
            payload.get("invoiceId") or
            payload.get("id") or
            _sha256(f"ghl_pay|{contact_id}|{now}")
        )
        order_id = _sha256(f"ghl_pay|{payment_id}")
        session_id = _sha256(f"ghl_psession|{contact_id}|{now}")

        _insert_order(db_path, {
            "order_id": order_id,
            "ts": now,
            "gross": round(value, 2),
            "net": round(value, 2),
            "fees": round(value * 0.029 + 0.30, 2),
            "customer_key": customer_key,
        })
        _insert_conversion(db_path, _sha256(f"ghl_payconv|{payment_id}"),
                           now, "Purchase", value, order_id, customer_key)

        if src_info.get("utm_source") or src_info.get("gclid") or src_info.get("fbclid"):
            _insert_touchpoint(db_path, now, channel, platform, src_info,
                               customer_key, session_id)

        _broadcast({
            "type": "new_order",
            "order_id": order_id,
            "gross": value,
            "customer_key": customer_key,
            "source": "ghl",
            "ts": now,
        })

        result["action"] = "payment_tracked"
        result["order_id"] = order_id
        result["value"] = value
        return result

    # ── Unknown / other events → still try to record if we have email ─────
    if email and customer_key:
        # Even for unknown events, record as a touchpoint so data isn't lost
        session_id = _sha256(f"ghl_unknown|{contact_id}|{now}")
        conv_id = _sha256(f"ghl_event|{contact_id}|{now}")
        _insert_conversion(db_path, conv_id, now, "Lead", 0, conv_id, customer_key)
        _insert_touchpoint(db_path, now, channel or "crm", platform, src_info,
                           customer_key, session_id)

        # Also insert a session so it appears in the dashboard
        with connect(db_path) as conn:
            conn.execute(
                """INSERT OR IGNORE INTO sessions (session_id, ts, utm_source, utm_medium, utm_campaign, utm_content, utm_term, referrer, landing_page, device, gclid, fbclid, ttclid, customer_key)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (session_id, now, src_info.get("utm_source", "ghl"), src_info.get("utm_medium", "webhook"),
                 src_info.get("utm_campaign", ""), src_info.get("utm_content", ""), "",
                 "ghl-webhook", "", "",
                 src_info.get("gclid", ""), src_info.get("fbclid", ""),
                 src_info.get("ttclid", ""), customer_key),
            )
            conn.commit()

        _broadcast({
            "type": "new_lead",
            "customer_key": customer_key,
            "name": name,
            "source": "ghl",
            "ts": now,
        })

        result["action"] = "lead_tracked_fallback"
    else:
        result["action"] = "logged"
        result["note"] = f"Event type '{event_type}' received, no email found"

    # Log all webhooks for debugging
    try:
        log_id = _sha256(f"whlog|{now}|{contact_id}")
        _log_webhook(db_path, log_id, now, "ghl", event_type,
                     json.dumps(payload), json.dumps(result))
    except Exception:
        pass

    return result


@router.get("/ghl-debug")
async def ghl_debug():
    """View recent GHL webhook payloads for debugging."""
    db_path = _db()
    _ensure_webhook_log_table(db_path)
    try:
        rows = db_query(db_path, "SELECT * FROM webhook_log WHERE source = 'ghl' ORDER BY ts DESC LIMIT 20")
        return {"rows": rows, "count": len(rows)}
    except Exception as e:
        return {"rows": [], "error": str(e)}
