"""Ad Spend Sync API — fetch ad spend from platform APIs into local spend table."""

from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from attributionops.config import default_db_path
from attributionops.db import connect

router = APIRouter()
UTC = timezone.utc


def _db() -> str:
    return os.environ.get("ATTRIBUTIONOPS_DB_PATH", default_db_path())


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ensure_spend_table(db_path: str) -> None:
    with connect(db_path) as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS spend (
                platform TEXT, date TEXT, account_id TEXT, campaign_id TEXT,
                adset_id TEXT, ad_id TEXT, creative_id TEXT,
                clicks TEXT, cost TEXT, impressions TEXT, metadata TEXT
            )"""
        )
        conn.commit()


def _default_dates() -> tuple[str, str]:
    end = date.today()
    start = end - timedelta(days=30)
    return start.isoformat(), end.isoformat()


def _normalize_date_range(start_date: str, end_date: str) -> tuple[str, str]:
    default_start, default_end = _default_dates()
    s = (start_date or default_start).strip()
    e = (end_date or default_end).strip()

    try:
        s_d = date.fromisoformat(s)
        e_d = date.fromisoformat(e)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.") from exc

    if s_d <= e_d:
        return s_d.isoformat(), e_d.isoformat()
    return e_d.isoformat(), s_d.isoformat()


async def _fetch_meta_insights(access_token: str, ad_account_id: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
    account_id = ad_account_id.replace("act_", "").strip()
    fields = ",".join(
        [
            "date_start",
            "account_id",
            "campaign_id",
            "adset_id",
            "ad_id",
            "clicks",
            "spend",
            "impressions",
            "campaign_name",
            "adset_name",
            "ad_name",
        ]
    )

    url = f"https://graph.facebook.com/v18.0/act_{account_id}/insights"
    params = {
        "access_token": access_token,
        "level": "ad",
        "fields": fields,
        "time_increment": "1",
        "time_range": json.dumps({"since": start_date, "until": end_date}, separators=(",", ":")),
        "limit": "500",
    }

    rows: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=45) as client:
        next_url: str | None = url
        next_params: dict[str, Any] | None = params
        while next_url:
            resp = await client.get(next_url, params=next_params)
            resp.raise_for_status()
            payload = resp.json()
            data = payload.get("data") or []
            rows.extend(data)
            next_url = (payload.get("paging") or {}).get("next")
            next_params = None

    return rows


def _write_meta_spend_rows(db_path: str, ad_account_id: str, start_date: str, end_date: str, rows: list[dict[str, Any]]) -> dict[str, int]:
    account_id = ad_account_id.replace("act_", "").strip()
    synced_at = _now()
    inserted = 0

    with connect(db_path) as conn:
        deleted = conn.execute(
            """
            DELETE FROM spend
            WHERE platform = 'meta'
              AND date BETWEEN ? AND ?
              AND (account_id = ? OR account_id = ?)
            """,
            (start_date, end_date, account_id, f"act_{account_id}"),
        ).rowcount

        for r in rows:
            day = str(r.get("date_start") or "").strip()
            if not day:
                continue

            account = str(r.get("account_id") or account_id).replace("act_", "").strip()
            campaign_id = str(r.get("campaign_id") or "").strip()
            adset_id = str(r.get("adset_id") or "").strip()
            ad_id = str(r.get("ad_id") or "").strip()
            clicks = str(r.get("clicks") or "0").strip()
            spend = str(r.get("spend") or "0").strip()
            impressions = str(r.get("impressions") or "0").strip()

            metadata = json.dumps(
                {
                    "campaign_name": str(r.get("campaign_name") or "").strip(),
                    "adset_name": str(r.get("adset_name") or "").strip(),
                    "ad_name": str(r.get("ad_name") or "").strip(),
                    "source": "meta_insights_api",
                    "synced_at": synced_at,
                },
                separators=(",", ":"),
            )

            conn.execute(
                """
                INSERT INTO spend (
                    platform, date, account_id, campaign_id,
                    adset_id, ad_id, creative_id,
                    clicks, cost, impressions, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "meta",
                    day,
                    account,
                    campaign_id,
                    adset_id,
                    ad_id,
                    "",
                    clicks,
                    spend,
                    impressions,
                    metadata,
                ),
            )
            inserted += 1

        conn.commit()

    return {"deleted": int(deleted if deleted is not None and deleted > 0 else 0), "inserted": inserted}


async def _sync_meta_spend(start_date: str, end_date: str) -> dict[str, Any]:
    access_token = os.environ.get("META_ACCESS_TOKEN", "").strip()
    ad_account_id = os.environ.get("META_AD_ACCOUNT_ID", "").strip()

    if not access_token or not ad_account_id:
        return {
            "synced": 0,
            "error": "META_ACCESS_TOKEN and META_AD_ACCOUNT_ID are required",
        }

    db_path = _db()
    _ensure_spend_table(db_path)

    try:
        api_rows = await _fetch_meta_insights(access_token, ad_account_id, start_date, end_date)
        write_stats = _write_meta_spend_rows(db_path, ad_account_id, start_date, end_date, api_rows)
        return {
            "synced": write_stats["inserted"],
            "fetched": len(api_rows),
            "deleted": write_stats["deleted"],
            "date_range": {"start": start_date, "end": end_date},
            "account_id": ad_account_id.replace("act_", ""),
        }
    except httpx.HTTPStatusError as exc:
        details = exc.response.text[:400] if exc.response is not None else str(exc)
        return {"synced": 0, "error": f"Meta Insights API error: {details}"}
    except Exception as exc:
        return {"synced": 0, "error": str(exc)}


@router.post("/sync")
async def sync_spend(
    platform: str = Query(default="meta"),
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
):
    """Sync ad spend into local spend table.

    Current support:
    - meta (Insights API)

    Query params:
    - platform: meta | all
    - start_date: YYYY-MM-DD (optional, default: last 30 days)
    - end_date: YYYY-MM-DD (optional, default: today)
    """
    p = (platform or "meta").strip().lower()
    if p not in {"meta", "all", "google", "tiktok"}:
        raise HTTPException(status_code=400, detail="platform must be one of: meta, google, tiktok, all")

    s, e = _normalize_date_range(start_date, end_date)

    results: dict[str, Any] = {
        "synced": 0,
        "errors": [],
        "platforms": {},
        "date_range": {"start": s, "end": e},
    }

    if p in {"meta", "all"}:
        meta_result = await _sync_meta_spend(s, e)
        results["platforms"]["meta"] = meta_result
        results["synced"] += int(meta_result.get("synced", 0) or 0)
        if meta_result.get("error"):
            results["errors"].append(f"meta: {meta_result['error']}")

    if p in {"google", "all"}:
        msg = "Google spend sync is not implemented yet in this build."
        results["platforms"]["google"] = {"synced": 0, "error": msg}
        if p == "google":
            results["errors"].append(f"google: {msg}")

    if p in {"tiktok", "all"}:
        msg = "TikTok spend sync is not implemented yet in this build."
        results["platforms"]["tiktok"] = {"synced": 0, "error": msg}
        if p == "tiktok":
            results["errors"].append(f"tiktok: {msg}")

    return results
