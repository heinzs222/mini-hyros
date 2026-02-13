"""LTV (Lifetime Value) Tracking — track customer value over 30/60/90/365 days.

Endpoints:
  GET /api/ltv/by-source      — LTV broken down by traffic source / campaign / ad
  GET /api/ltv/by-customer     — individual customer LTV
  GET /api/ltv/summary         — overall LTV metrics
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import APIRouter, Query

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from attributionops.config import default_db_path
from attributionops.db import sql_rows as db_query

router = APIRouter()
UTC = timezone.utc


def _db() -> str:
    return os.environ.get("ATTRIBUTIONOPS_DB_PATH", default_db_path())


@router.get("/by-source")
async def ltv_by_source(
    breakdown: str = Query(default="platform", description="platform, campaign_id, ad_id"),
    windows: str = Query(default="30,60,90,365", description="Comma-separated day windows"),
):
    """LTV per traffic source / campaign / ad over multiple time windows."""
    db_path = _db()
    day_windows = [int(w.strip()) for w in windows.split(",") if w.strip().isdigit()]

    # Get first touchpoint per customer (acquisition source)
    first_touch = db_query(db_path, """
        SELECT customer_key, platform, campaign_id, ad_id, MIN(ts) as first_ts
        FROM touchpoints
        WHERE customer_key != ''
        GROUP BY customer_key
    """)

    if not first_touch:
        return {"rows": [], "windows": day_windows}

    # Build customer → acquisition source map
    acq_map = {}
    for t in first_touch:
        acq_map[t["customer_key"]] = {
            "platform": t.get("platform", ""),
            "campaign_id": t.get("campaign_id", ""),
            "ad_id": t.get("ad_id", ""),
            "first_ts": t["first_ts"],
        }

    # Get all orders
    orders = db_query(db_path, """
        SELECT customer_key, ts, gross, net, refunds, fees
        FROM orders WHERE customer_key != ''
        ORDER BY ts
    """)

    # Calculate LTV per source per window
    results = {}
    for order in orders:
        ck = order["customer_key"]
        if ck not in acq_map:
            continue

        acq = acq_map[ck]
        dim_key = acq.get(breakdown, "unknown") or "unknown"
        first_ts = acq["first_ts"]

        try:
            first_dt = datetime.fromisoformat(first_ts.replace("Z", "+00:00"))
            order_dt = datetime.fromisoformat(order["ts"].replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue

        days_since = (order_dt - first_dt).days

        if dim_key not in results:
            results[dim_key] = {
                "dimension": dim_key,
                "customers": set(),
                "total_revenue": 0,
                "total_orders": 0,
            }
            for w in day_windows:
                results[dim_key][f"ltv_{w}d"] = 0
                results[dim_key][f"orders_{w}d"] = 0
                results[dim_key][f"customers_{w}d"] = set()

        gross = float(order.get("gross", 0) or 0)
        net = float(order.get("net", 0) or 0)
        revenue = net if net else gross

        results[dim_key]["customers"].add(ck)
        results[dim_key]["total_revenue"] += revenue
        results[dim_key]["total_orders"] += 1

        for w in day_windows:
            if days_since <= w:
                results[dim_key][f"ltv_{w}d"] += revenue
                results[dim_key][f"orders_{w}d"] += 1
                results[dim_key][f"customers_{w}d"].add(ck)

    # Convert sets to counts and calculate averages
    rows = []
    for dim_key, data in results.items():
        row = {
            "dimension": data["dimension"],
            "customers": len(data["customers"]),
            "total_revenue": round(data["total_revenue"], 2),
            "total_orders": data["total_orders"],
            "avg_ltv": round(data["total_revenue"] / max(len(data["customers"]), 1), 2),
        }
        for w in day_windows:
            cust_count = len(data[f"customers_{w}d"])
            row[f"ltv_{w}d"] = round(data[f"ltv_{w}d"], 2)
            row[f"avg_ltv_{w}d"] = round(data[f"ltv_{w}d"] / max(cust_count, 1), 2)
            row[f"orders_{w}d"] = data[f"orders_{w}d"]
            row[f"customers_{w}d"] = cust_count
        rows.append(row)

    rows.sort(key=lambda r: r["total_revenue"], reverse=True)
    return {"rows": rows, "windows": day_windows, "breakdown": breakdown}


@router.get("/by-customer")
async def ltv_by_customer(
    limit: int = Query(default=50),
    sort: str = Query(default="total_revenue", description="Sort by: total_revenue, orders, first_ts"),
):
    """Individual customer LTV with full purchase history."""
    db_path = _db()

    customers = db_query(db_path, """
        SELECT o.customer_key,
               COUNT(*) as order_count,
               SUM(CAST(o.gross AS REAL)) as total_gross,
               SUM(CAST(o.net AS REAL)) as total_net,
               SUM(CAST(o.refunds AS REAL)) as total_refunds,
               MIN(o.ts) as first_order,
               MAX(o.ts) as last_order
        FROM orders o
        WHERE o.customer_key != ''
        GROUP BY o.customer_key
        ORDER BY total_gross DESC
        LIMIT ?
    """, [limit])

    # Enrich with acquisition source
    rows = []
    for c in customers:
        ck = c["customer_key"]
        touch = db_query(db_path, """
            SELECT platform, campaign_id, channel, MIN(ts) as first_ts
            FROM touchpoints WHERE customer_key = ?
            ORDER BY ts LIMIT 1
        """, [ck])

        source = touch[0] if touch else {}
        rows.append({
            "customer_key": ck,
            "order_count": int(c.get("order_count", 0)),
            "total_revenue": round(float(c.get("total_gross", 0) or 0), 2),
            "total_net": round(float(c.get("total_net", 0) or 0), 2),
            "total_refunds": round(float(c.get("total_refunds", 0) or 0), 2),
            "first_order": c.get("first_order", ""),
            "last_order": c.get("last_order", ""),
            "acquisition_platform": source.get("platform", ""),
            "acquisition_campaign": source.get("campaign_id", ""),
            "acquisition_channel": source.get("channel", ""),
        })

    return {"rows": rows, "count": len(rows)}


@router.get("/summary")
async def ltv_summary():
    """Overall LTV metrics."""
    db_path = _db()

    try:
        totals = db_query(db_path, """
            SELECT COUNT(DISTINCT customer_key) as customers,
                   COUNT(*) as orders,
                   SUM(CAST(gross AS REAL)) as revenue,
                   SUM(CAST(net AS REAL)) as net_revenue,
                   SUM(CAST(refunds AS REAL)) as refunds,
                   AVG(CAST(gross AS REAL)) as avg_order_value
            FROM orders WHERE customer_key != ''
        """)

        total = totals[0] if totals else {}
        customers = int(total.get("customers", 0) or 0)
        revenue = float(total.get("revenue", 0) or 0)
        net = float(total.get("net_revenue", 0) or 0)
        orders = int(total.get("orders", 0) or 0)
        aov = float(total.get("avg_order_value", 0) or 0)

        # Repeat purchase rate
        repeat = db_query(db_path, """
            SELECT COUNT(*) as cnt FROM (
                SELECT customer_key FROM orders
                WHERE customer_key != ''
                GROUP BY customer_key HAVING COUNT(*) > 1
            )
        """)
        repeat_count = int(repeat[0]["cnt"]) if repeat else 0

        return {
            "total_customers": customers,
            "total_orders": orders,
            "total_revenue": round(revenue, 2),
            "total_net_revenue": round(net, 2),
            "avg_ltv": round(revenue / max(customers, 1), 2),
            "avg_order_value": round(aov, 2),
            "avg_orders_per_customer": round(orders / max(customers, 1), 2),
            "repeat_purchase_rate": round(repeat_count / max(customers, 1) * 100, 1),
            "repeat_customers": repeat_count,
        }
    except Exception:
        return {
            "total_customers": 0, "total_orders": 0, "total_revenue": 0,
            "total_net_revenue": 0, "avg_ltv": 0, "avg_order_value": 0,
            "avg_orders_per_customer": 0, "repeat_purchase_rate": 0, "repeat_customers": 0,
        }
