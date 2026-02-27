"""Funnel Step Reporting — conversion rates at each stage of the sales funnel.

Endpoints:
  GET /api/funnel/report        — funnel breakdown with conversion rates per step
  GET /api/funnel/by-source     — funnel by traffic source / campaign
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from fastapi import APIRouter, Query

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from attributionops.config import default_db_path
from attributionops.db import sql_rows as db_query

router = APIRouter()


def _db() -> str:
    return os.environ.get("ATTRIBUTIONOPS_DB_PATH", default_db_path())


FUNNEL_STAGES = [
    {"key": "visit", "label": "Page Visit", "query_type": "sessions"},
    {"key": "lead", "label": "Lead / Opt-in", "query_type": "conversion", "types": ("Lead", "lead", "FormSubmission", "form_submission")},
    {"key": "booking", "label": "Booking", "query_type": "conversion", "types": ("Booking", "booking", "AppointmentBooked", "appointment_booked")},
    {"key": "opportunity", "label": "Opportunity", "query_type": "conversion", "types": ("Opportunity", "opportunity")},
    {"key": "purchase", "label": "Purchase", "query_type": "conversion", "types": ("Purchase", "purchase", "Payment")},
]


def _build_funnel(db_path: str, where_clause: str = "", params: list = None) -> list[dict]:
    """Build funnel data for given filter."""
    params = params or []
    stages = []

    for stage in FUNNEL_STAGES:
        if stage["query_type"] == "sessions":
            sql = f"SELECT COUNT(DISTINCT session_id) as cnt FROM sessions WHERE 1=1 {where_clause}"
            rows = db_query(db_path, sql, params)
        else:
            type_list = stage["types"]
            placeholders = ",".join(["?"] * len(type_list))
            sql = f"SELECT COUNT(DISTINCT customer_key) as cnt FROM conversions WHERE type IN ({placeholders}) {where_clause}"
            rows = db_query(db_path, sql, list(type_list) + params)

        count = int(rows[0]["cnt"]) if rows else 0
        stages.append({
            "key": stage["key"],
            "label": stage["label"],
            "count": count,
        })

    # Calculate conversion rates between steps and from top
    top_count = stages[0]["count"] if stages else 0
    for i, stage in enumerate(stages):
        prev_count = stages[i - 1]["count"] if i > 0 else stage["count"]
        stage["step_rate"] = round(stage["count"] / max(prev_count, 1) * 100, 1) if i > 0 else 100.0
        stage["overall_rate"] = round(stage["count"] / max(top_count, 1) * 100, 1)
        stage["drop_off"] = (prev_count - stage["count"]) if i > 0 else 0

    return stages


@router.get("/report")
async def funnel_report(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
):
    """Overall funnel with conversion rates at each step."""
    db_path = _db()

    where = ""
    params = []
    if start_date:
        where += " AND ts >= ?"
        params.append(start_date)
    if end_date:
        where += " AND ts <= ?"
        params.append(end_date + "T23:59:59Z")

    stages = _build_funnel(db_path, where, params)

    return {
        "stages": stages,
        "top_of_funnel": stages[0]["count"] if stages else 0,
        "bottom_of_funnel": stages[-1]["count"] if stages else 0,
        "overall_conversion_rate": stages[-1]["overall_rate"] if stages else 0,
    }


@router.get("/by-source")
async def funnel_by_source(
    breakdown: str = Query(default="platform", description="platform, utm_source, campaign_id"),
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
):
    """Funnel broken down by traffic source."""
    db_path = _db()

    breakdown = breakdown or "platform"

    if breakdown == "utm_source":
        source_table = "sessions"
        source_col = "utm_source"
    elif breakdown in ("platform", "campaign_id"):
        source_table = "touchpoints"
        source_col = breakdown
    else:
        source_table = "touchpoints"
        source_col = "platform"
        breakdown = "platform"

    date_filter = ""
    params = []
    if start_date:
        date_filter += " AND ts >= ?"
        params.append(start_date)
    if end_date:
        date_filter += " AND ts <= ?"
        params.append(end_date + "T23:59:59Z")

    sources = db_query(
        db_path,
        f"SELECT DISTINCT {source_col} as source FROM {source_table} WHERE 1=1 {date_filter}",
        params,
    )

    results = []
    for src in sources:
        raw_source = str(src.get("source") or "")
        source_name = raw_source or "direct"

        if source_table == "sessions":
            sess = db_query(
                db_path,
                f"SELECT COUNT(DISTINCT session_id) as cnt FROM sessions WHERE {source_col} = ? {date_filter}",
                [raw_source] + params,
            )
            visit_count = int(sess[0]["cnt"]) if sess else 0

            cust_keys = db_query(
                db_path,
                f"SELECT DISTINCT customer_key FROM sessions WHERE {source_col} = ? AND customer_key != '' {date_filter}",
                [raw_source] + params,
            )
        else:
            sess = db_query(
                db_path,
                f"SELECT COUNT(DISTINCT session_id) as cnt FROM touchpoints WHERE {source_col} = ? {date_filter}",
                [raw_source] + params,
            )
            visit_count = int(sess[0]["cnt"]) if sess else 0

            cust_keys = db_query(
                db_path,
                f"SELECT DISTINCT customer_key FROM touchpoints WHERE {source_col} = ? AND customer_key != '' {date_filter}",
                [raw_source] + params,
            )

        ck_set = {c["customer_key"] for c in cust_keys if c.get("customer_key")}

        if not ck_set:
            results.append({
                "source": source_name,
                "visits": visit_count,
                "leads": 0, "bookings": 0, "opportunities": 0, "purchases": 0,
                "lead_rate": 0, "booking_rate": 0, "purchase_rate": 0,
                "revenue": 0,
            })
            continue

        ck_placeholders = ",".join(["?"] * len(ck_set))
        ck_list = list(ck_set)

        # Count conversions by type for these customers
        convs = db_query(db_path,
            f"SELECT type, COUNT(DISTINCT customer_key) as cnt FROM conversions WHERE customer_key IN ({ck_placeholders}) GROUP BY type",
            ck_list)

        conv_map = {}
        for c in convs:
            conv_map[c["type"]] = int(c["cnt"])

        leads = conv_map.get("Lead", 0) + conv_map.get("lead", 0)
        bookings = conv_map.get("Booking", 0) + conv_map.get("booking", 0)
        opps = conv_map.get("Opportunity", 0) + conv_map.get("opportunity", 0)
        purchases = conv_map.get("Purchase", 0) + conv_map.get("purchase", 0)

        # Revenue
        rev = db_query(db_path,
            f"SELECT SUM(CAST(gross AS REAL)) as rev FROM orders WHERE customer_key IN ({ck_placeholders})",
            ck_list)
        revenue = float(rev[0]["rev"] or 0) if rev else 0

        results.append({
            "source": source_name,
            "visits": visit_count,
            "leads": leads,
            "bookings": bookings,
            "opportunities": opps,
            "purchases": purchases,
            "lead_rate": round(leads / max(visit_count, 1) * 100, 1),
            "booking_rate": round(bookings / max(leads, 1) * 100, 1),
            "purchase_rate": round(purchases / max(visit_count, 1) * 100, 1),
            "revenue": round(revenue, 2),
        })

    results.sort(key=lambda r: r["visits"], reverse=True)
    return {"rows": results, "breakdown": breakdown}
