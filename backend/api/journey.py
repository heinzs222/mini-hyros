"""Customer Journey — visualize the full path each customer took before converting.

Endpoints:
  GET /api/journey/customer     — full journey for a specific customer
  GET /api/journey/common-paths — most common conversion paths
  GET /api/journey/stats        — journey stats (avg touchpoints, time to convert, etc.)
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from collections import Counter

from fastapi import APIRouter, Query, HTTPException

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from attributionops.config import default_db_path
from attributionops.db import sql_rows as db_query

router = APIRouter()
UTC = timezone.utc


def _db() -> str:
    return os.environ.get("ATTRIBUTIONOPS_DB_PATH", default_db_path())


@router.get("/customer")
async def customer_journey(
    customer_key: str = Query(..., description="Customer key (SHA256 hash)"),
):
    """Full journey timeline for a specific customer."""
    db_path = _db()

    if not customer_key:
        raise HTTPException(400, "customer_key is required")

    # Get all sessions
    sessions = db_query(db_path, """
        SELECT session_id, ts, utm_source, utm_medium, utm_campaign, utm_content,
               landing_page, device, referrer, gclid, fbclid, ttclid
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
        timeline.append({
            "type": "session",
            "ts": s["ts"],
            "details": {
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


@router.get("/common-paths")
async def common_paths(
    limit: int = Query(default=20),
    min_conversions: int = Query(default=2),
):
    """Most common conversion paths (channel sequences leading to purchase)."""
    db_path = _db()

    # Get customers who have orders
    customers_with_orders = db_query(db_path, """
        SELECT DISTINCT customer_key FROM orders WHERE customer_key != ''
    """)

    path_counter = Counter()
    path_revenue = {}

    for cust in customers_with_orders:
        ck = cust["customer_key"]

        # Get touchpoint channels in order for this customer
        touches = db_query(db_path, """
            SELECT channel, platform FROM touchpoints
            WHERE customer_key = ?
            ORDER BY ts
        """, [ck])

        if not touches:
            continue

        # Build path string
        path_parts = []
        prev = ""
        for t in touches:
            label = t.get("platform") or t.get("channel") or "direct"
            if label != prev:
                path_parts.append(label)
                prev = label

        path_str = " → ".join(path_parts)
        path_counter[path_str] += 1

        # Get revenue for this customer
        rev = db_query(db_path, """
            SELECT SUM(CAST(gross AS REAL)) as rev FROM orders WHERE customer_key = ?
        """, [ck])
        revenue = float(rev[0]["rev"] or 0) if rev else 0

        if path_str not in path_revenue:
            path_revenue[path_str] = 0
        path_revenue[path_str] += revenue

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
