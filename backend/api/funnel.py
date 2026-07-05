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
from attributionops.util import local_day_bounds_utc

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
        if i == 0:
            stage["step_rate"] = 100.0
        elif prev_count > 0:
            stage["step_rate"] = round(stage["count"] / prev_count * 100, 1)
        else:
            # No base population at the previous stage → a step rate is undefined.
            stage["step_rate"] = None
        stage["overall_rate"] = round(stage["count"] / max(top_count, 1) * 100, 1)
        stage["drop_off"] = (prev_count - stage["count"]) if i > 0 else 0

    return stages


@router.get("/report")
def funnel_report(
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
def funnel_by_source(
    breakdown: str = Query(default="platform", description="platform, utm_source, campaign_id"),
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
):
    """Funnel broken down by traffic source.

    Set-based: three grouped queries total (visits, conversions, revenue) rather
    than the previous N+1-per-source pattern with unbounded ``customer_key IN(...)``
    lists (which 500'd past SQLite's variable limit). The same timezone-aware date
    range is applied to conversions and orders, not just to visits.
    """
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

    # Half-open UTC instant bounds from the inclusive local-day range. Either
    # side may be absent → open-ended on that side (preserves all-time default).
    start_utc = end_excl_utc = None
    if start_date:
        start_utc, _ = local_day_bounds_utc(start_date, start_date)
    if end_date:
        _, end_excl_utc = local_day_bounds_utc(end_date, end_date)

    def _range(col: str) -> tuple[str, list]:
        clause = ""
        vals: list = []
        if start_utc:
            clause += f" AND {col} >= ?"
            vals.append(start_utc)
        if end_excl_utc:
            clause += f" AND {col} < ?"
            vals.append(end_excl_utc)
        return clause, vals

    src_clause, src_params = _range("ts")

    # 1) Visits per source (all sessions/touchpoints, not just identified ones).
    visit_rows = db_query(
        db_path,
        f"SELECT COALESCE({source_col}, '') AS source, COUNT(DISTINCT session_id) AS visits "
        f"FROM {source_table} WHERE 1=1 {src_clause} "
        f"GROUP BY COALESCE({source_col}, '')",
        src_params,
    )
    visits_by_source = {str(r.get("source") or ""): int(r.get("visits") or 0) for r in visit_rows}

    # Distinct (source, customer_key) map, reused by the conversion + revenue joins.
    src_cust_cte = (
        f"WITH src_cust AS ("
        f" SELECT DISTINCT COALESCE({source_col}, '') AS source, customer_key"
        f" FROM {source_table}"
        f" WHERE COALESCE(customer_key, '') != '' {src_clause}"
        f")"
    )

    # 2) Distinct converting customers per (source, conversion type), date-filtered.
    conv_clause, conv_params = _range("cv.ts")
    conv_rows = db_query(
        db_path,
        f"""
        {src_cust_cte}
        SELECT sc.source AS source, cv.type AS type, COUNT(DISTINCT cv.customer_key) AS cnt
        FROM src_cust sc
        JOIN conversions cv ON cv.customer_key = sc.customer_key
        WHERE 1=1 {conv_clause}
        GROUP BY sc.source, cv.type
        """,
        src_params + conv_params,
    )
    conv_map: dict[str, dict[str, int]] = {}
    for r in conv_rows:
        conv_map.setdefault(str(r.get("source") or ""), {})[str(r.get("type") or "")] = int(r.get("cnt") or 0)

    # 3) Revenue per source (orders of that source's customers), date-filtered.
    rev_clause, rev_params = _range("o.ts")
    rev_rows = db_query(
        db_path,
        f"""
        {src_cust_cte}
        SELECT sc.source AS source, SUM(CAST(o.gross AS REAL)) AS rev
        FROM src_cust sc
        JOIN orders o ON o.customer_key = sc.customer_key
        WHERE 1=1 {rev_clause}
        GROUP BY sc.source
        """,
        src_params + rev_params,
    )
    rev_by_source = {str(r.get("source") or ""): float(r.get("rev") or 0) for r in rev_rows}

    results = []
    for source_key, visit_count in visits_by_source.items():
        source_name = source_key or "direct"
        types = conv_map.get(source_key, {})
        leads = types.get("Lead", 0) + types.get("lead", 0)
        bookings = types.get("Booking", 0) + types.get("booking", 0)
        opps = types.get("Opportunity", 0) + types.get("opportunity", 0)
        purchases = types.get("Purchase", 0) + types.get("purchase", 0)
        revenue = rev_by_source.get(source_key, 0)

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
