"""Customer Journey — visualize the full path each customer took before converting.

Endpoints:
  GET /api/journey/customer     — full journey for a specific customer
  GET /api/journey/common-paths — most common conversion paths
  GET /api/journey/stats        — journey stats (avg touchpoints, time to convert, etc.)
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from collections import Counter
from itertools import groupby
from typing import Any

from fastapi import APIRouter, Query, HTTPException

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from attributionops.config import default_db_path
from attributionops.db import connect, sql_rows as db_query

router = APIRouter()
UTC = timezone.utc


def _db() -> str:
    return os.environ.get("ATTRIBUTIONOPS_DB_PATH", default_db_path())


def _table_columns(db_path: str, table: str) -> set[str]:
    with connect(db_path) as conn:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _parse_ts(ts: str) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _duration_label(start_ts: str, end_ts: str) -> str:
    start = _parse_ts(start_ts)
    end = _parse_ts(end_ts)
    if not start or not end:
        return ""
    seconds = max((end - start).total_seconds(), 0)
    minutes = seconds / 60
    hours = seconds / 3600
    if minutes < 1:
        return "<1 min"
    if hours < 1:
        return f"{int(minutes)} min"
    if hours < 24:
        return f"{hours:.1f} hours"
    return f"{hours / 24:.1f} days"


def _ad_name_map(db_path: str) -> dict[tuple[str, str, str], str]:
    try:
        rows = db_query(db_path, """
            SELECT platform, entity_type, entity_id, name
            FROM ad_names
            WHERE COALESCE(entity_id, '') != ''
        """)
    except Exception:
        return {}
    return {
        (str(r.get("platform") or ""), str(r.get("entity_type") or ""), str(r.get("entity_id") or "")): str(r.get("name") or "")
        for r in rows
    }


def _entity_name(names: dict[tuple[str, str, str], str], platform: str, entity_type: str, entity_id: str) -> str:
    if not entity_id:
        return ""
    return names.get((platform, entity_type, entity_id), "")


def _event_label(event: dict[str, Any]) -> str:
    details = event.get("details") or {}
    event_type = event.get("type")
    if event_type == "touchpoint":
        platform = details.get("platform") or details.get("channel") or "direct"
        campaign = details.get("campaign_name") or details.get("campaign_id") or ""
        return f"{platform}: {campaign}" if campaign else str(platform)
    if event_type == "session":
        event_name = details.get("event_name") or ""
        if event_name and event_name.lower() not in ("pageview", "page_view"):
            return str(event_name)
        return str(details.get("page_title") or details.get("landing_page") or "Session")
    if event_type == "order":
        return "Purchase"
    if event_type == "conversion":
        return str(details.get("conversion_type") or "Conversion")
    return str(event_type or "event")


def _compact_path(timeline: list[dict[str, Any]], max_steps: int = 8) -> list[str]:
    parts: list[str] = []
    for event in timeline:
        label = _event_label(event).strip()
        if not label:
            continue
        if not parts or parts[-1] != label:
            parts.append(label)
    if len(parts) <= max_steps:
        return parts
    return parts[: max_steps - 1] + [f"+{len(parts) - max_steps + 1} more"]


def _conversion_timeline(
    db_path: str,
    customer_key: str,
    conversion_ts: str,
    conversion: dict[str, Any],
    names: dict[tuple[str, str, str], str],
) -> list[dict[str, Any]]:
    timeline: list[dict[str, Any]] = []

    session_cols = _table_columns(db_path, "sessions")
    session_select = [
        "session_id", "ts", "utm_source", "utm_medium", "utm_campaign", "utm_content",
        "landing_page", "device", "referrer", "gclid", "fbclid", "ttclid",
    ]
    for optional_col in ("visitor_id", "event_name", "page_title", "custom_data_json"):
        if optional_col in session_cols:
            session_select.append(optional_col)

    sessions = db_query(db_path, f"""
        SELECT {", ".join(session_select)}
        FROM sessions
        WHERE customer_key = ? AND ts <= ?
        ORDER BY ts
    """, [customer_key, conversion_ts])

    for s in sessions:
        details = {
            "session_id": s.get("session_id", ""),
            "utm_source": s.get("utm_source", ""),
            "utm_medium": s.get("utm_medium", ""),
            "utm_campaign": s.get("utm_campaign", ""),
            "utm_content": s.get("utm_content", ""),
            "landing_page": s.get("landing_page", ""),
            "device": s.get("device", ""),
            "referrer": s.get("referrer", ""),
            "has_gclid": bool(s.get("gclid")),
            "has_fbclid": bool(s.get("fbclid")),
            "has_ttclid": bool(s.get("ttclid")),
        }
        for optional_col in ("visitor_id", "event_name", "page_title"):
            if optional_col in s:
                details[optional_col] = s.get(optional_col, "")
        if s.get("custom_data_json"):
            try:
                details["custom_data"] = json.loads(s.get("custom_data_json") or "{}")
            except json.JSONDecodeError:
                details["custom_data"] = {}
        timeline.append({"type": "session", "ts": s.get("ts", ""), "details": details})

    touchpoints = db_query(db_path, """
        SELECT ts, channel, platform, campaign_id, adset_id, ad_id,
               creative_id, gclid, fbclid, ttclid, session_id
        FROM touchpoints
        WHERE customer_key = ? AND ts <= ?
        ORDER BY ts
    """, [customer_key, conversion_ts])

    for t in touchpoints:
        platform = str(t.get("platform") or "")
        campaign_id = str(t.get("campaign_id") or "")
        adset_id = str(t.get("adset_id") or "")
        ad_id = str(t.get("ad_id") or "")
        timeline.append({
            "type": "touchpoint",
            "ts": t.get("ts", ""),
            "details": {
                "channel": t.get("channel", ""),
                "platform": platform,
                "campaign_id": campaign_id,
                "campaign_name": _entity_name(names, platform, "campaign", campaign_id),
                "adset_id": adset_id,
                "adset_name": _entity_name(names, platform, "adset", adset_id),
                "ad_id": ad_id,
                "ad_name": _entity_name(names, platform, "ad", ad_id),
                "creative_id": t.get("creative_id", ""),
                "session_id": t.get("session_id", ""),
                "has_gclid": bool(t.get("gclid")),
                "has_fbclid": bool(t.get("fbclid")),
                "has_ttclid": bool(t.get("ttclid")),
            },
        })

    conv_type = conversion.get("type") or "Conversion"
    timeline.append({
        "type": "conversion",
        "ts": conversion_ts,
        "details": {
            "conversion_id": conversion.get("conversion_id", ""),
            "conversion_type": conv_type,
            "value": float(conversion.get("value", 0) or 0),
            "order_id": conversion.get("order_id", ""),
        },
    })

    if str(conv_type).strip().lower() in ("purchase", "payment"):
        timeline.append({
            "type": "order",
            "ts": conversion_ts,
            "details": {
                "order_id": conversion.get("order_id", ""),
                "gross": float(conversion.get("gross", conversion.get("value", 0)) or 0),
                "net": float(conversion.get("net", conversion.get("value", 0)) or 0),
            },
        })

    timeline.sort(key=lambda event: str(event.get("ts") or ""))
    return timeline


@router.get("/customer")
async def customer_journey(
    customer_key: str = Query(..., description="Customer key (SHA256 hash)"),
):
    """Full journey timeline for a specific customer."""
    db_path = _db()

    if not customer_key:
        raise HTTPException(400, "customer_key is required")

    # Get all sessions
    session_cols = _table_columns(db_path, "sessions")
    session_select = [
        "session_id", "ts", "utm_source", "utm_medium", "utm_campaign", "utm_content",
        "landing_page", "device", "referrer", "gclid", "fbclid", "ttclid",
    ]
    for optional_col in ("visitor_id", "event_name", "page_title", "custom_data_json"):
        if optional_col in session_cols:
            session_select.append(optional_col)
    sessions = db_query(db_path, f"""
        SELECT {", ".join(session_select)}
        FROM sessions WHERE customer_key = ?
        ORDER BY ts
    """, [customer_key])

    # Get all touchpoints
    touchpoints = db_query(db_path, """
        SELECT ts, channel, platform, campaign_id, adset_id, ad_id,
               gclid, fbclid, ttclid, session_id
        FROM touchpoints WHERE customer_key = ?
        ORDER BY ts
    """, [customer_key])

    # Get all orders
    orders = db_query(db_path, """
        SELECT order_id, ts, gross, net, refunds, fees
        FROM orders WHERE customer_key = ?
        ORDER BY ts
    """, [customer_key])

    # Get all conversions
    conversions = db_query(db_path, """
        SELECT conversion_id, ts, type, value, order_id
        FROM conversions WHERE customer_key = ?
        ORDER BY ts
    """, [customer_key])

    # Build unified timeline
    timeline = []

    for s in sessions:
        details = {
            "session_id": s["session_id"],
            "utm_source": s.get("utm_source", ""),
            "utm_medium": s.get("utm_medium", ""),
            "utm_campaign": s.get("utm_campaign", ""),
            "landing_page": s.get("landing_page", ""),
            "device": s.get("device", ""),
            "referrer": s.get("referrer", ""),
            "has_gclid": bool(s.get("gclid")),
            "has_fbclid": bool(s.get("fbclid")),
            "has_ttclid": bool(s.get("ttclid")),
        }
        if s.get("visitor_id"):
            details["visitor_id"] = s.get("visitor_id", "")
        if s.get("event_name"):
            details["event_name"] = s.get("event_name", "")
        if s.get("page_title"):
            details["page_title"] = s.get("page_title", "")
        if s.get("custom_data_json"):
            try:
                details["custom_data"] = json.loads(s.get("custom_data_json") or "{}")
            except json.JSONDecodeError:
                details["custom_data"] = {}
        timeline.append({
            "type": "session",
            "ts": s["ts"],
            "details": details
        })

    for t in touchpoints:
        timeline.append({
            "type": "touchpoint",
            "ts": t["ts"],
            "details": {
                "channel": t.get("channel", ""),
                "platform": t.get("platform", ""),
                "campaign_id": t.get("campaign_id", ""),
                "adset_id": t.get("adset_id", ""),
                "ad_id": t.get("ad_id", ""),
                "session_id": t.get("session_id", ""),
                "has_gclid": bool(t.get("gclid")),
                "has_fbclid": bool(t.get("fbclid")),
                "has_ttclid": bool(t.get("ttclid")),
            }
        })

    for c in conversions:
        conv_type = c.get("type", "unknown")
        timeline.append({
            "type": "conversion",
            "ts": c["ts"],
            "details": {
                "conversion_type": conv_type,
                "value": float(c.get("value", 0) or 0),
                "order_id": c.get("order_id", ""),
            }
        })

    for o in orders:
        timeline.append({
            "type": "order",
            "ts": o["ts"],
            "details": {
                "order_id": o["order_id"],
                "gross": float(o.get("gross", 0) or 0),
                "net": float(o.get("net", 0) or 0),
                "refunds": float(o.get("refunds", 0) or 0),
            }
        })

    timeline.sort(key=lambda x: x["ts"])

    # Calculate summary
    total_revenue = sum(float(o.get("gross", 0) or 0) for o in orders)
    first_touch_ts = sessions[0]["ts"] if sessions else ""
    first_order_ts = orders[0]["ts"] if orders else ""
    time_to_convert = ""
    if first_touch_ts and first_order_ts:
        try:
            ft = datetime.fromisoformat(first_touch_ts.replace("Z", "+00:00"))
            fo = datetime.fromisoformat(first_order_ts.replace("Z", "+00:00"))
            delta = fo - ft
            hours = delta.total_seconds() / 3600
            if hours < 1:
                time_to_convert = f"{int(delta.total_seconds() / 60)} min"
            elif hours < 24:
                time_to_convert = f"{hours:.1f} hours"
            else:
                time_to_convert = f"{delta.days} days"
        except (ValueError, TypeError):
            pass

    return {
        "customer_key": customer_key,
        "summary": {
            "total_sessions": len(sessions),
            "total_touchpoints": len(touchpoints),
            "total_orders": len(orders),
            "total_conversions": len(conversions),
            "total_revenue": round(total_revenue, 2),
            "first_touch": first_touch_ts,
            "first_order": first_order_ts,
            "time_to_convert": time_to_convert,
        },
        "timeline": timeline,
    }


@router.get("/leads")
async def lead_paths(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    limit: int = Query(default=50, ge=1, le=200),
    include_purchases: bool = Query(default=True),
):
    """Recent lead and purchase paths with the timeline for each customer."""
    db_path = _db()

    lead_types = [
        "lead",
        "formsubmission",
        "form_submission",
        "signup",
        "booking",
        "appointment",
        "appointmentbooked",
        "appointment_booked",
    ]
    purchase_types = ["purchase", "payment"] if include_purchases else []
    wanted_types = lead_types + purchase_types
    placeholders = ", ".join("?" for _ in wanted_types)
    where = [
        "COALESCE(c.customer_key, '') != ''",
        f"LOWER(COALESCE(c.type, '')) IN ({placeholders})",
    ]
    params: list[Any] = list(wanted_types)

    if start_date:
        where.append("date(substr(c.ts, 1, 10)) >= date(?)")
        params.append(start_date)
    if end_date:
        where.append("date(substr(c.ts, 1, 10)) <= date(?)")
        params.append(end_date)

    params.append(limit)
    conversions = db_query(db_path, f"""
        SELECT c.conversion_id, c.ts, c.type, c.value, c.order_id, c.customer_key,
               o.gross, o.net
        FROM conversions c
        LEFT JOIN orders o ON o.order_id = c.order_id
        WHERE {" AND ".join(where)}
        ORDER BY c.ts DESC
        LIMIT ?
    """, params)

    names = _ad_name_map(db_path)
    rows: list[dict[str, Any]] = []
    for conv in conversions:
        customer_key = str(conv.get("customer_key") or "")
        conversion_ts = str(conv.get("ts") or "")
        timeline = _conversion_timeline(db_path, customer_key, conversion_ts, conv, names)
        pre_conversion = [event for event in timeline if event.get("type") not in ("conversion", "order")]
        first_touch = pre_conversion[0] if pre_conversion else None
        last_touch = pre_conversion[-1] if pre_conversion else None

        rows.append({
            "customer_key": customer_key,
            "customer_key_short": f"{customer_key[:10]}..." if len(customer_key) > 13 else customer_key,
            "conversion_id": conv.get("conversion_id", ""),
            "conversion_type": conv.get("type", ""),
            "conversion_ts": conversion_ts,
            "order_id": conv.get("order_id", ""),
            "value": round(float(conv.get("value", 0) or 0), 2),
            "gross": round(float(conv.get("gross", conv.get("value", 0)) or 0), 2),
            "first_touch": first_touch,
            "last_touch": last_touch,
            "first_touch_ts": first_touch.get("ts", "") if first_touch else "",
            "last_touch_ts": last_touch.get("ts", "") if last_touch else "",
            "time_to_convert": _duration_label(first_touch.get("ts", ""), conversion_ts) if first_touch else "",
            "touchpoint_count": len(pre_conversion),
            "path": _compact_path(timeline),
            "timeline": timeline,
        })

    return {
        "rows": rows,
        "count": len(rows),
        "include_purchases": include_purchases,
        "date_range": {"start": start_date, "end": end_date},
    }


@router.get("/common-paths")
async def common_paths(
    limit: int = Query(default=20),
    min_conversions: int = Query(default=2),
):
    """Most common conversion paths (channel sequences leading to purchase)."""
    db_path = _db()

    # Revenue per customer in one grouped query (was one query per customer).
    rev_rows = db_query(db_path, """
        SELECT customer_key, SUM(CAST(gross AS REAL)) AS rev
        FROM orders WHERE customer_key != '' GROUP BY customer_key
    """)
    revenue_by_customer = {r["customer_key"]: float(r["rev"] or 0) for r in rev_rows}

    # All touchpoints for customers-with-orders in one ordered query (was one
    # query per customer). Rows arrive grouped by customer_key, then by ts.
    touch_rows = db_query(db_path, """
        SELECT t.customer_key, t.channel, t.platform
        FROM touchpoints t
        WHERE t.customer_key != '' AND EXISTS (
            SELECT 1 FROM orders o WHERE o.customer_key = t.customer_key AND o.customer_key != ''
        )
        ORDER BY t.customer_key, t.ts
    """)

    path_counter = Counter()
    path_revenue = {}

    for ck, group in groupby(touch_rows, key=lambda r: r["customer_key"]):
        # Build path string (collapse consecutive identical labels).
        path_parts = []
        prev = ""
        for t in group:
            label = t.get("platform") or t.get("channel") or "direct"
            if label != prev:
                path_parts.append(label)
                prev = label

        if not path_parts:
            continue

        path_str = " → ".join(path_parts)
        path_counter[path_str] += 1
        path_revenue[path_str] = path_revenue.get(path_str, 0) + revenue_by_customer.get(ck, 0)

    # Filter and sort
    rows = []
    for path, count in path_counter.most_common(limit * 2):
        if count >= min_conversions:
            rows.append({
                "path": path,
                "conversions": count,
                "total_revenue": round(path_revenue.get(path, 0), 2),
                "avg_revenue": round(path_revenue.get(path, 0) / max(count, 1), 2),
                "touchpoints": len(path.split(" → ")),
            })
        if len(rows) >= limit:
            break

    return {"rows": rows, "count": len(rows)}


@router.get("/stats")
async def journey_stats():
    """Overall journey statistics."""
    db_path = _db()

    try:
        # Avg touchpoints before conversion
        customers = db_query(db_path, """
            SELECT o.customer_key,
                   COUNT(DISTINCT t.session_id) as touch_count,
                   MIN(t.ts) as first_touch,
                   MIN(o.ts) as first_order
            FROM orders o
            JOIN touchpoints t ON o.customer_key = t.customer_key
            WHERE o.customer_key != '' AND t.ts <= o.ts
            GROUP BY o.customer_key
        """)

        if not customers:
            return {
                "avg_touchpoints_before_conversion": 0,
                "avg_time_to_convert_hours": 0,
                "single_touch_pct": 0,
                "multi_touch_pct": 0,
                "total_journeys": 0,
            }

        touch_counts = [int(c.get("touch_count", 0)) for c in customers]
        single_touch = sum(1 for tc in touch_counts if tc <= 1)

        # Calculate avg time to convert
        time_deltas = []
        for c in customers:
            try:
                ft = datetime.fromisoformat(c["first_touch"].replace("Z", "+00:00"))
                fo = datetime.fromisoformat(c["first_order"].replace("Z", "+00:00"))
                hours = (fo - ft).total_seconds() / 3600
                if hours >= 0:
                    time_deltas.append(hours)
            except (ValueError, TypeError, KeyError):
                pass

        total = len(customers)
        return {
            "avg_touchpoints_before_conversion": round(sum(touch_counts) / max(total, 1), 1),
            "avg_time_to_convert_hours": round(sum(time_deltas) / max(len(time_deltas), 1), 1),
            "single_touch_pct": round(single_touch / max(total, 1) * 100, 1),
            "multi_touch_pct": round((total - single_touch) / max(total, 1) * 100, 1),
            "total_journeys": total,
        }
    except Exception:
        return {
            "avg_touchpoints_before_conversion": 0,
            "avg_time_to_convert_hours": 0,
            "single_touch_pct": 0,
            "multi_touch_pct": 0,
            "total_journeys": 0,
        }
