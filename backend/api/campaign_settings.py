"""Campaign tracking settings API.

Lets the operator mark individual campaigns as tracked / not-tracked. A campaign
flagged ``tracked = 0`` is filtered out at read time everywhere (reports, drill-
down children, AI recommendations) via ``attributionops.tools.campaign_filter``.
Absence of a ``campaign_settings`` row means the campaign IS tracked.

Endpoints:
  GET  /api/campaigns                 — list every known campaign + tracked flag
  PUT  /api/campaigns/tracking        — toggle a single campaign
  PUT  /api/campaigns/tracking/batch  — toggle many campaigns in one transaction
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from attributionops.config import default_db_path
from attributionops.db import connect, sql_rows
from attributionops.tools import campaign_filter

router = APIRouter()
UTC = timezone.utc


def _db() -> str:
    return os.environ.get("ATTRIBUTIONOPS_DB_PATH", default_db_path())


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── Models ───────────────────────────────────────────────────────────────────

class TrackingUpdate(BaseModel):
    platform: str
    campaign_id: str
    tracked: bool


class TrackingBatchItem(BaseModel):
    platform: str
    campaign_id: str
    tracked: bool


class TrackingBatch(BaseModel):
    items: list[TrackingBatchItem]


# ── Endpoints ────────────────────────────────────────────────────────────────

@router.get("")
def list_campaigns():
    """List every distinct campaign seen in spend or touchpoints, with its
    display name, tracked flag, lifetime spend, and last-seen date."""
    db_path = _db()
    campaign_filter.ensure_campaign_settings_table(db_path)

    rows = sql_rows(db_path, """
        SELECT c.platform AS platform,
               c.campaign_id AS campaign_id,
               COALESCE(an.name, '') AS name,
               cs.tracked AS tracked,
               COALESCE(sp.lifetime_spend, 0.0) AS lifetime_spend,
               COALESCE(sp.last_seen, '') AS last_seen
        FROM (
            SELECT DISTINCT platform, campaign_id FROM spend WHERE campaign_id != ''
            UNION
            SELECT DISTINCT platform, campaign_id FROM touchpoints WHERE campaign_id != ''
        ) c
        LEFT JOIN (
            SELECT platform, campaign_id,
                   SUM(CAST(cost AS REAL)) AS lifetime_spend,
                   MAX(date) AS last_seen
            FROM spend
            WHERE campaign_id != ''
            GROUP BY platform, campaign_id
        ) sp
            ON sp.platform = c.platform AND sp.campaign_id = c.campaign_id
        LEFT JOIN ad_names an
            ON an.entity_type = 'campaign'
           AND LOWER(an.platform) = LOWER(c.platform)
           AND an.entity_id = c.campaign_id
        LEFT JOIN campaign_settings cs
            ON LOWER(cs.platform) = LOWER(c.platform)
           AND cs.campaign_id = c.campaign_id
        ORDER BY lifetime_spend DESC
    """)

    campaigns = []
    for r in rows:
        raw_tracked = r.get("tracked")
        # Absence of a settings row (NULL) == tracked.
        tracked = True if raw_tracked is None else bool(int(raw_tracked))
        campaigns.append({
            "platform": str(r.get("platform") or ""),
            "campaign_id": str(r.get("campaign_id") or ""),
            "name": str(r.get("name") or ""),
            "tracked": tracked,
            "lifetime_spend": round(float(r.get("lifetime_spend") or 0.0), 2),
            "last_seen": str(r.get("last_seen") or ""),
        })

    return {"campaigns": campaigns}


@router.put("/tracking")
def update_tracking(body: TrackingUpdate):
    """Set the tracked flag for a single campaign."""
    db_path = _db()
    campaign_filter.ensure_campaign_settings_table(db_path)

    with connect(db_path) as conn:
        conn.execute(
            """INSERT INTO campaign_settings (platform, campaign_id, tracked, updated_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(platform, campaign_id) DO UPDATE SET
                   tracked = excluded.tracked,
                   updated_at = excluded.updated_at""",
            (body.platform, body.campaign_id, int(bool(body.tracked)), _now()),
        )
        conn.commit()

    return {"ok": True}


@router.put("/tracking/batch")
def update_tracking_batch(body: TrackingBatch):
    """Set the tracked flag for many campaigns in a single transaction."""
    db_path = _db()
    campaign_filter.ensure_campaign_settings_table(db_path)

    now = _now()
    with connect(db_path) as conn:
        for item in body.items:
            conn.execute(
                """INSERT INTO campaign_settings (platform, campaign_id, tracked, updated_at)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(platform, campaign_id) DO UPDATE SET
                       tracked = excluded.tracked,
                       updated_at = excluded.updated_at""",
                (item.platform, item.campaign_id, int(bool(item.tracked)), now),
            )
        conn.commit()

    return {"ok": True, "updated": len(body.items)}
