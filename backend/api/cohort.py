"""Cohort Analysis — group customers by acquisition period and compare LTV curves.

Endpoints:
  GET /api/cohort/analysis      — cohort LTV matrix (rows = cohorts, cols = months)
  GET /api/cohort/retention      — retention rates per cohort
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

from fastapi import APIRouter, Query

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from attributionops.config import default_db_path
from attributionops.db import sql_rows as db_query

router = APIRouter()
UTC = timezone.utc


def _db() -> str:
    return os.environ.get("ATTRIBUTIONOPS_DB_PATH", default_db_path())


def _month_key(ts_str: str) -> str:
    """Extract YYYY-MM from ISO timestamp."""
    try:
        return ts_str[:7]
    except (TypeError, IndexError):
        return "unknown"


@router.get("/analysis")
async def cohort_analysis(
    granularity: str = Query(default="month", description="month or week"),
    breakdown: str = Query(default="", description="Optional: platform, campaign_id"),
):
    """Cohort LTV matrix. Rows are acquisition cohorts, columns are periods since acquisition."""
    db_path = _db()

    # Get first touchpoint per customer (acquisition date)
    first_touch = db_query(db_path, """
        SELECT customer_key, MIN(ts) as first_ts, platform, campaign_id
        FROM touchpoints WHERE customer_key != ''
        GROUP BY customer_key
    """)

    if not first_touch:
        return {"cohorts": [], "periods": []}

    # Map customer → cohort
    customer_cohort = {}
    cohort_meta = defaultdict(lambda: {"customers": set(), "platform": "", "campaign": ""})

    for t in first_touch:
        ck = t["customer_key"]
        if granularity == "week":
            try:
                dt = datetime.fromisoformat(t["first_ts"].replace("Z", "+00:00"))
                cohort_key = dt.strftime("%Y-W%W")
            except (ValueError, TypeError):
                cohort_key = "unknown"
        else:
            cohort_key = _month_key(t["first_ts"])

        # Optional breakdown filter
        if breakdown == "platform" and t.get("platform"):
            cohort_key = f"{cohort_key} ({t['platform']})"
        elif breakdown == "campaign_id" and t.get("campaign_id"):
            cohort_key = f"{cohort_key} ({t['campaign_id']})"

        customer_cohort[ck] = cohort_key
        cohort_meta[cohort_key]["customers"].add(ck)
        cohort_meta[cohort_key]["platform"] = t.get("platform", "")
        cohort_meta[cohort_key]["campaign"] = t.get("campaign_id", "")

    # Get all orders
    orders = db_query(db_path, """
        SELECT customer_key, ts, gross, net FROM orders WHERE customer_key != ''
    """)

    # Build cohort matrix: cohort → {period_0: revenue, period_1: revenue, ...}
    cohort_data = defaultdict(lambda: defaultdict(float))
    cohort_order_counts = defaultdict(lambda: defaultdict(int))

    for order in orders:
        ck = order["customer_key"]
        if ck not in customer_cohort:
            continue

        cohort_key = customer_cohort[ck]
        revenue = float(order.get("gross", 0) or 0)

        # Calculate period offset
        first_ts = None
        for t in first_touch:
            if t["customer_key"] == ck:
                first_ts = t["first_ts"]
                break

        if not first_ts:
            continue

        try:
            first_dt = datetime.fromisoformat(first_ts.replace("Z", "+00:00"))
            order_dt = datetime.fromisoformat(order["ts"].replace("Z", "+00:00"))
            if granularity == "week":
                period = (order_dt - first_dt).days // 7
            else:
                period = (order_dt.year - first_dt.year) * 12 + (order_dt.month - first_dt.month)
        except (ValueError, TypeError):
            period = 0

        cohort_data[cohort_key][period] += revenue
        cohort_order_counts[cohort_key][period] += 1

    # Find max period
    all_periods = set()
    for periods in cohort_data.values():
        all_periods.update(periods.keys())
    max_period = max(all_periods) if all_periods else 0
    period_labels = list(range(max_period + 1))

    # Build output
    cohorts = []
    for cohort_key in sorted(cohort_meta.keys()):
        meta = cohort_meta[cohort_key]
        customer_count = len(meta["customers"])
        periods_data = []
        cumulative = 0

        for p in period_labels:
            rev = cohort_data[cohort_key].get(p, 0)
            cumulative += rev
            periods_data.append({
                "period": p,
                "revenue": round(rev, 2),
                "cumulative_revenue": round(cumulative, 2),
                "orders": cohort_order_counts[cohort_key].get(p, 0),
                "ltv_per_customer": round(cumulative / max(customer_count, 1), 2),
            })

        cohorts.append({
            "cohort": cohort_key,
            "customers": customer_count,
            "total_revenue": round(cumulative, 2),
            "avg_ltv": round(cumulative / max(customer_count, 1), 2),
            "periods": periods_data,
        })

    return {
        "cohorts": cohorts,
        "periods": period_labels,
        "granularity": granularity,
    }


@router.get("/retention")
async def cohort_retention(
    granularity: str = Query(default="month"),
):
    """Retention rates per cohort — what % of customers from each cohort made repeat purchases."""
    db_path = _db()

    # First touchpoint per customer
    first_touch = db_query(db_path, """
        SELECT customer_key, MIN(ts) as first_ts
        FROM touchpoints WHERE customer_key != ''
        GROUP BY customer_key
    """)

    if not first_touch:
        return {"cohorts": []}

    customer_cohort = {}
    cohort_customers = defaultdict(set)

    for t in first_touch:
        ck = t["customer_key"]
        cohort_key = _month_key(t["first_ts"])
        customer_cohort[ck] = cohort_key
        cohort_customers[cohort_key].add(ck)

    # Get orders grouped by customer and period
    orders = db_query(db_path, """
        SELECT customer_key, ts FROM orders WHERE customer_key != ''
    """)

    # Track which customers purchased in each period
    cohort_period_active = defaultdict(lambda: defaultdict(set))

    for order in orders:
        ck = order["customer_key"]
        if ck not in customer_cohort:
            continue

        cohort_key = customer_cohort[ck]
        first_ts_str = None
        for t in first_touch:
            if t["customer_key"] == ck:
                first_ts_str = t["first_ts"]
                break

        if not first_ts_str:
            continue

        try:
            first_dt = datetime.fromisoformat(first_ts_str.replace("Z", "+00:00"))
            order_dt = datetime.fromisoformat(order["ts"].replace("Z", "+00:00"))
            period = (order_dt.year - first_dt.year) * 12 + (order_dt.month - first_dt.month)
        except (ValueError, TypeError):
            period = 0

        cohort_period_active[cohort_key][period].add(ck)

    # Build retention table
    max_period = 0
    for periods in cohort_period_active.values():
        if periods:
            max_period = max(max_period, max(periods.keys()))

    cohorts = []
    for cohort_key in sorted(cohort_customers.keys()):
        total = len(cohort_customers[cohort_key])
        retention = []
        for p in range(max_period + 1):
            active = len(cohort_period_active[cohort_key].get(p, set()))
            retention.append({
                "period": p,
                "active_customers": active,
                "retention_rate": round(active / max(total, 1) * 100, 1),
            })

        cohorts.append({
            "cohort": cohort_key,
            "customers": total,
            "retention": retention,
        })

    return {"cohorts": cohorts, "max_period": max_period, "granularity": granularity}
