"""AI Ad Recommendations — suggest which ads to scale, pause, or test.

Endpoints:
  GET /api/ai/recommendations    — get ad recommendations based on attribution data
  GET /api/ai/insights           — key insights about performance trends
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


def _safe_float(val, default=0.0) -> float:
    try:
        return float(val or default)
    except (ValueError, TypeError):
        return default


@router.get("/recommendations")
async def get_recommendations(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
):
    """Generate ad recommendations based on attribution data."""
    db_path = _db()

    if not start_date:
        start_date = (datetime.now(UTC) - timedelta(days=30)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.now(UTC).strftime("%Y-%m-%d")

    recommendations = []

    # Get spend + attribution per campaign
    try:
        campaigns = db_query(db_path, """
            SELECT s.platform, s.campaign_id,
                   SUM(CAST(s.cost AS REAL)) as spend,
                   SUM(CAST(s.clicks AS REAL)) as clicks
            FROM spend s
            WHERE s.date >= ? AND s.date <= ?
            GROUP BY s.platform, s.campaign_id
        """, [start_date, end_date])
    except Exception:
        campaigns = []

    try:
        # Revenue per campaign from attribution
        revenue_data = db_query(db_path, """
            SELECT t.platform, t.campaign_id,
                   COUNT(DISTINCT o.order_id) as orders,
                   SUM(CAST(o.gross AS REAL)) as revenue
            FROM touchpoints t
            JOIN orders o ON t.customer_key = o.customer_key
            WHERE t.ts >= ? AND o.ts >= ?
            GROUP BY t.platform, t.campaign_id
        """, [start_date, start_date])
    except Exception:
        revenue_data = []

    # Build campaign map
    rev_map = {}
    for r in revenue_data:
        key = f"{r.get('platform','')}|{r.get('campaign_id','')}"
        rev_map[key] = {
            "orders": int(r.get("orders", 0) or 0),
            "revenue": _safe_float(r.get("revenue")),
        }

    # Analyze each campaign
    for camp in campaigns:
        platform = camp.get("platform", "")
        campaign_id = camp.get("campaign_id", "")
        spend = _safe_float(camp.get("spend"))
        clicks = _safe_float(camp.get("clicks"))

        key = f"{platform}|{campaign_id}"
        rev_info = rev_map.get(key, {"orders": 0, "revenue": 0})
        revenue = rev_info["revenue"]
        orders = rev_info["orders"]

        roas = revenue / max(spend, 1)
        cpa = spend / max(orders, 1) if orders else 0
        profit = revenue - spend

        rec = {
            "platform": platform,
            "campaign_id": campaign_id,
            "spend": round(spend, 2),
            "revenue": round(revenue, 2),
            "profit": round(profit, 2),
            "roas": round(roas, 2),
            "orders": orders,
            "cpa": round(cpa, 2),
            "clicks": int(clicks),
        }

        # Generate recommendation
        if spend == 0:
            rec["action"] = "monitor"
            rec["priority"] = "low"
            rec["reason"] = "No spend recorded — verify tracking is set up correctly."
        elif roas >= 3:
            rec["action"] = "scale"
            rec["priority"] = "high"
            rec["reason"] = f"Strong ROAS of {roas:.1f}x. Consider increasing budget by 20-30%. This campaign is highly profitable."
            rec["suggested_budget_change"] = "+25%"
        elif roas >= 2:
            rec["action"] = "scale"
            rec["priority"] = "medium"
            rec["reason"] = f"Solid ROAS of {roas:.1f}x. Profitable — try scaling budget by 10-15% and monitor."
            rec["suggested_budget_change"] = "+15%"
        elif roas >= 1:
            rec["action"] = "optimize"
            rec["priority"] = "medium"
            rec["reason"] = f"Breaking even at {roas:.1f}x ROAS. Optimize targeting, creatives, or landing page to improve."
            rec["suggestions"] = [
                "Test new ad creatives",
                "Refine audience targeting",
                "Improve landing page conversion rate",
                "Check if LTV makes this campaign profitable long-term",
            ]
        elif orders > 0:
            rec["action"] = "optimize_or_pause"
            rec["priority"] = "high"
            rec["reason"] = f"Losing money at {roas:.1f}x ROAS (${abs(profit):.0f} loss). Needs urgent optimization or consider pausing."
            rec["suggestions"] = [
                "Pause worst-performing ad sets",
                "Tighten targeting",
                "Test completely new creatives",
                "Review landing page for issues",
            ]
        else:
            rec["action"] = "pause"
            rec["priority"] = "high"
            rec["reason"] = f"Spent ${spend:.0f} with zero attributed orders. Pause and investigate — check tracking, audience, or offer."
            rec["suggestions"] = [
                "Verify tracking pixel is firing on this campaign's landing page",
                "Check if UTM params are set correctly in ad links",
                "Review if the audience matches your offer",
            ]

        recommendations.append(rec)

    # Sort: high priority first, then by profit (worst first for urgent attention)
    priority_order = {"high": 0, "medium": 1, "low": 2}
    recommendations.sort(key=lambda r: (priority_order.get(r["priority"], 3), r["profit"]))

    return {"recommendations": recommendations, "period": {"start": start_date, "end": end_date}}


@router.get("/insights")
async def get_insights():
    """Key performance insights and trends."""
    db_path = _db()
    insights = []

    try:
        # Top performing platform
        platform_perf = db_query(db_path, """
            SELECT t.platform,
                   COUNT(DISTINCT o.order_id) as orders,
                   SUM(CAST(o.gross AS REAL)) as revenue
            FROM touchpoints t
            JOIN orders o ON t.customer_key = o.customer_key
            WHERE t.platform != ''
            GROUP BY t.platform
            ORDER BY revenue DESC
        """)

        if platform_perf:
            top = platform_perf[0]
            insights.append({
                "type": "top_platform",
                "title": f"{top['platform'].title()} is your top performer",
                "detail": f"{int(top.get('orders',0))} orders, ${_safe_float(top.get('revenue')):.0f} revenue",
                "priority": "info",
            })

        # Tracking coverage
        total_sessions = db_query(db_path, "SELECT COUNT(*) as cnt FROM sessions")[0]["cnt"]
        tracked_sessions = db_query(db_path, "SELECT COUNT(*) as cnt FROM sessions WHERE customer_key != ''")[0]["cnt"]
        total_s = int(total_sessions or 0)
        tracked_s = int(tracked_sessions or 0)

        if total_s > 0:
            pct = tracked_s / total_s * 100
            if pct < 50:
                insights.append({
                    "type": "tracking_gap",
                    "title": f"Only {pct:.0f}% of sessions have identity",
                    "detail": "Most visitors are anonymous. Add hyros.identify() on more pages (forms, login, checkout) to improve attribution accuracy.",
                    "priority": "warning",
                })

        # Refund rate check
        try:
            refund_stats = db_query(db_path, """
                SELECT SUM(CAST(refunds AS REAL)) as total_refunds,
                       SUM(CAST(gross AS REAL)) as total_revenue
                FROM orders
            """)
            if refund_stats:
                total_ref = _safe_float(refund_stats[0].get("total_refunds"))
                total_rev = _safe_float(refund_stats[0].get("total_revenue"))
                if total_rev > 0:
                    refund_rate = total_ref / total_rev * 100
                    if refund_rate > 10:
                        insights.append({
                            "type": "high_refunds",
                            "title": f"Refund rate is {refund_rate:.1f}%",
                            "detail": "This is above the healthy 5% threshold. Investigate product quality, targeting, or expectation-setting in ads.",
                            "priority": "warning",
                        })
        except Exception:
            pass

        # Single vs multi-touch
        try:
            multi = db_query(db_path, """
                SELECT customer_key, COUNT(DISTINCT session_id) as touches
                FROM touchpoints WHERE customer_key != ''
                GROUP BY customer_key
            """)
            if multi:
                avg_touches = sum(int(m.get("touches", 0)) for m in multi) / len(multi)
                multi_count = sum(1 for m in multi if int(m.get("touches", 0)) > 1)
                if avg_touches > 1.5:
                    insights.append({
                        "type": "multi_touch",
                        "title": f"Average {avg_touches:.1f} touchpoints per customer",
                        "detail": f"{multi_count} customers ({multi_count/max(len(multi),1)*100:.0f}%) needed multiple interactions before buying. Consider using multi-touch attribution models.",
                        "priority": "info",
                    })
        except Exception:
            pass

        # LTV opportunity
        try:
            repeat = db_query(db_path, """
                SELECT COUNT(*) as cnt FROM (
                    SELECT customer_key FROM orders WHERE customer_key != ''
                    GROUP BY customer_key HAVING COUNT(*) > 1
                )
            """)
            total_cust = db_query(db_path, "SELECT COUNT(DISTINCT customer_key) as cnt FROM orders WHERE customer_key != ''")
            repeat_count = int(repeat[0]["cnt"]) if repeat else 0
            total_count = int(total_cust[0]["cnt"]) if total_cust else 0
            if total_count > 0:
                repeat_rate = repeat_count / total_count * 100
                if repeat_rate > 20:
                    insights.append({
                        "type": "repeat_customers",
                        "title": f"{repeat_rate:.0f}% repeat purchase rate",
                        "detail": "Strong repeat buying. Factor LTV into your ad decisions — campaigns that look break-even on first purchase may be highly profitable long-term.",
                        "priority": "info",
                    })
                elif repeat_rate < 5 and total_count > 10:
                    insights.append({
                        "type": "low_repeat",
                        "title": f"Only {repeat_rate:.0f}% repeat purchase rate",
                        "detail": "Consider adding email/SMS follow-up sequences, loyalty offers, or upsells to increase customer LTV.",
                        "priority": "warning",
                    })
        except Exception:
            pass

    except Exception:
        pass

    return {"insights": insights}
