from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Query

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from attributionops.config import default_db_path
from attributionops.db import connect, query

router = APIRouter()


def _db() -> str:
    return os.environ.get("ATTRIBUTIONOPS_DB_PATH", default_db_path())


def _ensure_video_table(db_path: str) -> None:
    with connect(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS video_metrics (
                platform TEXT,
                date TEXT,
                account_id TEXT,
                campaign_id TEXT,
                adset_id TEXT,
                ad_id TEXT,
                video_id TEXT,
                video_name TEXT,
                views INTEGER DEFAULT 0,
                views_3s INTEGER DEFAULT 0,
                views_25pct INTEGER DEFAULT 0,
                views_50pct INTEGER DEFAULT 0,
                views_75pct INTEGER DEFAULT 0,
                views_100pct INTEGER DEFAULT 0,
                avg_watch_time_sec REAL DEFAULT 0,
                video_length_sec REAL DEFAULT 0,
                thumb_stop_rate REAL DEFAULT 0,
                hook_rate REAL DEFAULT 0,
                hold_rate REAL DEFAULT 0,
                click_through_rate REAL DEFAULT 0,
                cost_per_view REAL DEFAULT 0,
                impressions INTEGER DEFAULT 0,
                clicks INTEGER DEFAULT 0,
                cost REAL DEFAULT 0
            );
        """)
        conn.commit()


@router.get("/metrics")
async def get_video_metrics(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    platform: str = Query(default="all"),
    breakdown: str = Query(default="ad"),
):
    db_path = _db()
    _ensure_video_table(db_path)

    from datetime import date, timedelta
    if not start_date:
        start_date = (date.today() - timedelta(days=30)).isoformat()
    if not end_date:
        end_date = date.today().isoformat()

    platform_filter = ""
    params: dict[str, Any] = {"start_date": start_date, "end_date": end_date}
    if platform and platform.lower() != "all":
        platform_filter = "AND platform = :platform"
        params["platform"] = platform

    if breakdown == "ad":
        group_cols = "platform, account_id, campaign_id, adset_id, ad_id, video_id, video_name"
    elif breakdown == "campaign":
        group_cols = "platform, account_id, campaign_id"
    elif breakdown == "platform":
        group_cols = "platform"
    else:
        group_cols = "platform, account_id, campaign_id, adset_id, ad_id, video_id, video_name"

    rows = query(
        db_path,
        f"""
        SELECT
            {group_cols},
            SUM(views) as views,
            SUM(views_3s) as views_3s,
            SUM(views_25pct) as views_25pct,
            SUM(views_50pct) as views_50pct,
            SUM(views_75pct) as views_75pct,
            SUM(views_100pct) as views_100pct,
            AVG(avg_watch_time_sec) as avg_watch_time_sec,
            AVG(video_length_sec) as video_length_sec,
            AVG(thumb_stop_rate) as thumb_stop_rate,
            AVG(hook_rate) as hook_rate,
            AVG(hold_rate) as hold_rate,
            AVG(click_through_rate) as click_through_rate,
            SUM(cost) as cost,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            CASE WHEN SUM(views) > 0 THEN ROUND(SUM(cost) * 1.0 / SUM(views), 4) ELSE 0 END as cost_per_view
        FROM video_metrics
        WHERE date BETWEEN :start_date AND :end_date
        {platform_filter}
        GROUP BY {group_cols}
        ORDER BY SUM(views) DESC;
        """,
        params,
    ).rows

    # Compute VTR (view-through rate) and completion rate per row
    for r in rows:
        views = int(r.get("views") or 0)
        impressions = int(r.get("impressions") or 0)
        views_100 = int(r.get("views_100pct") or 0)
        r["vtr"] = round(views / impressions, 4) if impressions > 0 else 0
        r["completion_rate"] = round(views_100 / views, 4) if views > 0 else 0

    return {
        "start_date": start_date,
        "end_date": end_date,
        "platform": platform,
        "breakdown": breakdown,
        "rows": rows,
    }


@router.get("/summary")
async def video_summary(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
):
    """Platform-level video performance summary for the dashboard."""
    db_path = _db()
    _ensure_video_table(db_path)

    from datetime import date, timedelta
    if not start_date:
        start_date = (date.today() - timedelta(days=30)).isoformat()
    if not end_date:
        end_date = date.today().isoformat()

    rows = query(
        db_path,
        """
        SELECT
            platform,
            SUM(views) as total_views,
            SUM(views_3s) as total_3s_views,
            SUM(views_100pct) as total_completions,
            AVG(avg_watch_time_sec) as avg_watch_time,
            AVG(hook_rate) as avg_hook_rate,
            AVG(hold_rate) as avg_hold_rate,
            AVG(thumb_stop_rate) as avg_thumb_stop,
            SUM(cost) as total_cost,
            SUM(clicks) as total_clicks,
            SUM(impressions) as total_impressions,
            CASE WHEN SUM(views) > 0 THEN ROUND(SUM(cost) * 1.0 / SUM(views), 4) ELSE 0 END as cpv
        FROM video_metrics
        WHERE date BETWEEN :start_date AND :end_date
        GROUP BY platform
        ORDER BY SUM(views) DESC;
        """,
        {"start_date": start_date, "end_date": end_date},
    ).rows

    return {"start_date": start_date, "end_date": end_date, "platforms": rows}
