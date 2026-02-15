"""Ad Names API — manual mapping + auto-sync from ad platform APIs.

Endpoints:
  GET  /api/ad-names              — list all name mappings
  POST /api/ad-names              — upsert a name mapping (manual)
  DELETE /api/ad-names             — delete a mapping
  POST /api/ad-names/sync         — auto-fetch names from Meta/Google/TikTok APIs
  GET  /api/ad-names/resolve      — resolve a list of IDs to names
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from attributionops.config import default_db_path
from attributionops.db import connect, sql_rows

router = APIRouter()
UTC = timezone.utc


def _db() -> str:
    return os.environ.get("ATTRIBUTIONOPS_DB_PATH", default_db_path())


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ensure_table(db_path: str) -> None:
    with connect(db_path) as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS ad_names (
            platform TEXT,
            entity_type TEXT,
            entity_id TEXT,
            name TEXT,
            parent_id TEXT,
            source TEXT DEFAULT 'manual',
            updated_at TEXT,
            PRIMARY KEY (platform, entity_type, entity_id)
        )""")
        conn.commit()


# ── Models ───────────────────────────────────────────────────────────────────

class NameMapping(BaseModel):
    platform: str
    entity_type: str  # "campaign", "adset", "ad"
    entity_id: str
    name: str
    parent_id: str = ""


class DeleteMapping(BaseModel):
    platform: str
    entity_type: str
    entity_id: str


# ── CRUD Endpoints ───────────────────────────────────────────────────────────

@router.get("")
async def list_names(
    platform: str = Query(default=""),
    entity_type: str = Query(default=""),
):
    """List all name mappings, optionally filtered."""
    db_path = _db()
    _ensure_table(db_path)

    sql = "SELECT * FROM ad_names WHERE 1=1"
    params = []
    if platform:
        sql += " AND platform = ?"
        params.append(platform)
    if entity_type:
        sql += " AND entity_type = ?"
        params.append(entity_type)
    sql += " ORDER BY platform, entity_type, name"

    rows = sql_rows(db_path, sql, params)
    return {"rows": rows, "count": len(rows)}


@router.post("")
async def upsert_name(mapping: NameMapping):
    """Create or update a name mapping."""
    db_path = _db()
    _ensure_table(db_path)

    with connect(db_path) as conn:
        conn.execute(
            """INSERT OR REPLACE INTO ad_names (platform, entity_type, entity_id, name, parent_id, source, updated_at)
               VALUES (?, ?, ?, ?, ?, 'manual', ?)""",
            (mapping.platform, mapping.entity_type, mapping.entity_id,
             mapping.name, mapping.parent_id, _now()),
        )
        conn.commit()

    return {"ok": True, "mapping": mapping.dict()}


@router.post("/batch")
async def batch_upsert(mappings: list[NameMapping]):
    """Batch upsert multiple name mappings."""
    db_path = _db()
    _ensure_table(db_path)

    with connect(db_path) as conn:
        for m in mappings:
            conn.execute(
                """INSERT OR REPLACE INTO ad_names (platform, entity_type, entity_id, name, parent_id, source, updated_at)
                   VALUES (?, ?, ?, ?, ?, 'manual', ?)""",
                (m.platform, m.entity_type, m.entity_id, m.name, m.parent_id, _now()),
            )
        conn.commit()

    return {"ok": True, "count": len(mappings)}


@router.delete("")
async def delete_name(mapping: DeleteMapping):
    """Delete a name mapping."""
    db_path = _db()
    _ensure_table(db_path)

    with connect(db_path) as conn:
        conn.execute(
            "DELETE FROM ad_names WHERE platform = ? AND entity_type = ? AND entity_id = ?",
            (mapping.platform, mapping.entity_type, mapping.entity_id),
        )
        conn.commit()

    return {"ok": True}


@router.get("/resolve")
async def resolve_names(
    ids: str = Query(default=""),
    platform: str = Query(default=""),
):
    """Resolve a comma-separated list of entity IDs to names."""
    db_path = _db()
    _ensure_table(db_path)

    if not ids:
        return {"names": {}}

    id_list = [i.strip() for i in ids.split(",") if i.strip()]
    placeholders = ",".join("?" * len(id_list))

    sql = f"SELECT entity_id, name, entity_type FROM ad_names WHERE entity_id IN ({placeholders})"
    params = list(id_list)
    if platform:
        sql = f"SELECT entity_id, name, entity_type FROM ad_names WHERE entity_id IN ({placeholders}) AND platform = ?"
        params.append(platform)

    rows = sql_rows(db_path, sql, params)
    names = {r["entity_id"]: r["name"] for r in rows}
    return {"names": names}


# ── Name resolution helper (used by report endpoints) ───────────────────────

def get_name_map(db_path: str, entity_type: str = "") -> dict[str, str]:
    """Return a name map with both platform-aware and raw entity_id keys.

    Keys included per row:
    - "{platform}|{entity_id}" (preferred, avoids collisions)
    - "{entity_id}" (fallback for legacy callers)
    """
    try:
        with connect(db_path) as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS ad_names (
                platform TEXT, entity_type TEXT, entity_id TEXT,
                name TEXT, parent_id TEXT, source TEXT DEFAULT 'manual',
                updated_at TEXT, PRIMARY KEY (platform, entity_type, entity_id)
            )""")
            conn.commit()

        if entity_type:
            rows = sql_rows(
                db_path,
                "SELECT platform, entity_id, name FROM ad_names WHERE entity_type = ?",
                [entity_type],
            )
        else:
            rows = sql_rows(db_path, "SELECT platform, entity_id, name FROM ad_names")

        out: dict[str, str] = {}
        for r in rows:
            platform = str(r.get("platform") or "").strip().lower()
            entity_id = str(r.get("entity_id") or "").strip()
            name = str(r.get("name") or "").strip()
            if not entity_id or not name:
                continue
            if platform:
                out[f"{platform}|{entity_id}"] = name
            if entity_id not in out:
                out[entity_id] = name

        return out
    except Exception:
        return {}


# ── API Sync ─────────────────────────────────────────────────────────────────

@router.post("/sync")
async def sync_names(platform: str = Query(default="all")):
    """Fetch campaign/adset/ad names from ad platform APIs.
    Requires API credentials in environment variables.
    """
    results = {"synced": 0, "errors": [], "platforms": {}}

    if platform in ("all", "meta"):
        r = await _sync_meta()
        results["platforms"]["meta"] = r
        results["synced"] += r.get("synced", 0)
        if r.get("error"):
            results["errors"].append(f"meta: {r['error']}")

    if platform in ("all", "google"):
        r = await _sync_google()
        results["platforms"]["google"] = r
        results["synced"] += r.get("synced", 0)
        if r.get("error"):
            results["errors"].append(f"google: {r['error']}")

    if platform in ("all", "tiktok"):
        r = await _sync_tiktok()
        results["platforms"]["tiktok"] = r
        results["synced"] += r.get("synced", 0)
        if r.get("error"):
            results["errors"].append(f"tiktok: {r['error']}")

    return results


def _meta_api_version() -> str:
    raw = str(os.environ.get("META_API_VERSION", "v18.0") or "").strip()
    return raw or "v18.0"


def _meta_url(path: str) -> str:
    return f"https://graph.facebook.com/{_meta_api_version()}/{path.lstrip('/')}"


def _meta_error_message(payload: dict[str, Any], status_code: int) -> str:
    err = payload.get("error") or {}
    message = str(err.get("message") or "Meta API request failed").strip()
    code = err.get("code")
    error_type = err.get("type")
    fbtrace = err.get("fbtrace_id")

    parts = [message]
    if error_type:
        parts.append(f"type={error_type}")
    if code is not None:
        parts.append(f"code={code}")
    if fbtrace:
        parts.append(f"fbtrace_id={fbtrace}")
    parts.append(f"status={status_code}")
    return " | ".join(parts)


async def _meta_fetch_all(
    client: httpx.AsyncClient,
    account_id: str,
    access_token: str,
    endpoint: str,
    fields: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    next_url: str | None = _meta_url(f"act_{account_id}/{endpoint}")
    params: dict[str, Any] | None = {
        "access_token": access_token,
        "fields": fields,
        "limit": 500,
    }

    while next_url:
        resp = await client.get(next_url, params=params)
        try:
            payload = resp.json()
        except Exception:
            payload = {}

        if resp.status_code >= 400:
            raise RuntimeError(_meta_error_message(payload, resp.status_code))

        if payload.get("error"):
            raise RuntimeError(_meta_error_message(payload, resp.status_code))

        data = payload.get("data")
        if isinstance(data, list):
            rows.extend(data)

        next_url = (payload.get("paging") or {}).get("next")
        params = None

    return rows


async def _sync_meta() -> dict:
    """Fetch campaign/adset/ad names from Meta Marketing API."""
    access_token = os.environ.get("META_ACCESS_TOKEN", "")
    ad_account_id = os.environ.get("META_AD_ACCOUNT_ID", "")
    account_id = str(ad_account_id or "").replace("act_", "").strip()

    if not access_token or not account_id:
        return {"synced": 0, "error": "META_ACCESS_TOKEN and META_AD_ACCOUNT_ID required"}

    db_path = _db()
    _ensure_table(db_path)
    synced = 0
    now = _now()
    campaigns: list[dict[str, Any]] = []
    adsets: list[dict[str, Any]] = []
    ads: list[dict[str, Any]] = []

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            # Fetch campaigns
            campaigns = await _meta_fetch_all(
                client=client,
                account_id=account_id,
                access_token=access_token,
                endpoint="campaigns",
                fields="id,name,status",
            )
            with connect(db_path) as conn:
                for c in campaigns:
                    conn.execute(
                        """INSERT OR REPLACE INTO ad_names (platform, entity_type, entity_id, name, parent_id, source, updated_at)
                           VALUES ('meta', 'campaign', ?, ?, '', 'api', ?)""",
                        (str(c["id"]), str(c["name"]), now),
                    )
                    synced += 1
                conn.commit()

            # Fetch adsets
            adsets = await _meta_fetch_all(
                client=client,
                account_id=account_id,
                access_token=access_token,
                endpoint="adsets",
                fields="id,name,campaign_id,status",
            )
            with connect(db_path) as conn:
                for a in adsets:
                    conn.execute(
                        """INSERT OR REPLACE INTO ad_names (platform, entity_type, entity_id, name, parent_id, source, updated_at)
                           VALUES ('meta', 'adset', ?, ?, ?, 'api', ?)""",
                        (str(a["id"]), str(a["name"]), str(a.get("campaign_id", "")), now),
                    )
                    synced += 1
                conn.commit()

            # Fetch ads
            ads = await _meta_fetch_all(
                client=client,
                account_id=account_id,
                access_token=access_token,
                endpoint="ads",
                fields="id,name,adset_id,campaign_id,status",
            )
            with connect(db_path) as conn:
                for a in ads:
                    conn.execute(
                        """INSERT OR REPLACE INTO ad_names (platform, entity_type, entity_id, name, parent_id, source, updated_at)
                           VALUES ('meta', 'ad', ?, ?, ?, 'api', ?)""",
                        (str(a["id"]), str(a["name"]), str(a.get("adset_id", "")), now),
                    )
                    synced += 1
                conn.commit()

    except Exception as e:
        return {"synced": synced, "error": str(e)}

    return {
        "synced": synced,
        "campaigns": len(campaigns),
        "adsets": len(adsets),
        "ads": len(ads),
        "account_id": account_id,
        "api_version": _meta_api_version(),
    }


async def _sync_google() -> dict:
    """Placeholder for Google Ads API sync. Requires google-ads library."""
    developer_token = os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN", "")
    customer_id = os.environ.get("GOOGLE_ADS_CUSTOMER_ID", "")

    if not developer_token or not customer_id:
        return {"synced": 0, "error": "GOOGLE_ADS_DEVELOPER_TOKEN and GOOGLE_ADS_CUSTOMER_ID required"}

    # Google Ads API requires the google-ads Python library and OAuth2.
    # For now, return a helpful message.
    return {"synced": 0, "error": "Google Ads API sync requires google-ads library. Use manual mapping or install google-ads package."}


async def _sync_tiktok() -> dict:
    """Fetch campaign/adgroup/ad names from TikTok Marketing API."""
    access_token = os.environ.get("TIKTOK_ACCESS_TOKEN", "")
    advertiser_id = os.environ.get("TIKTOK_ADVERTISER_ID", "")

    if not access_token or not advertiser_id:
        return {"synced": 0, "error": "TIKTOK_ACCESS_TOKEN and TIKTOK_ADVERTISER_ID required"}

    db_path = _db()
    _ensure_table(db_path)
    synced = 0
    now = _now()

    try:
        headers = {"Access-Token": access_token, "Content-Type": "application/json"}
        async with httpx.AsyncClient(timeout=30) as client:
            # Fetch campaigns
            resp = await client.get(
                "https://business-api.tiktok.com/open_api/v1.3/campaign/get/",
                params={"advertiser_id": advertiser_id, "page_size": 200},
                headers=headers,
            )
            data = resp.json().get("data", {})
            campaigns = data.get("list", [])
            with connect(db_path) as conn:
                for c in campaigns:
                    conn.execute(
                        """INSERT OR REPLACE INTO ad_names (platform, entity_type, entity_id, name, parent_id, source, updated_at)
                           VALUES ('tiktok', 'campaign', ?, ?, '', 'api', ?)""",
                        (str(c.get("campaign_id", "")), str(c.get("campaign_name", "")), now),
                    )
                    synced += 1
                conn.commit()

            # Fetch adgroups (adsets)
            resp = await client.get(
                "https://business-api.tiktok.com/open_api/v1.3/adgroup/get/",
                params={"advertiser_id": advertiser_id, "page_size": 200},
                headers=headers,
            )
            data = resp.json().get("data", {})
            adgroups = data.get("list", [])
            with connect(db_path) as conn:
                for a in adgroups:
                    conn.execute(
                        """INSERT OR REPLACE INTO ad_names (platform, entity_type, entity_id, name, parent_id, source, updated_at)
                           VALUES ('tiktok', 'adset', ?, ?, ?, 'api', ?)""",
                        (str(a.get("adgroup_id", "")), str(a.get("adgroup_name", "")),
                         str(a.get("campaign_id", "")), now),
                    )
                    synced += 1
                conn.commit()

            # Fetch ads
            resp = await client.get(
                "https://business-api.tiktok.com/open_api/v1.3/ad/get/",
                params={"advertiser_id": advertiser_id, "page_size": 200},
                headers=headers,
            )
            data = resp.json().get("data", {})
            ads = data.get("list", [])
            with connect(db_path) as conn:
                for a in ads:
                    conn.execute(
                        """INSERT OR REPLACE INTO ad_names (platform, entity_type, entity_id, name, parent_id, source, updated_at)
                           VALUES ('tiktok', 'ad', ?, ?, ?, 'api', ?)""",
                        (str(a.get("ad_id", "")), str(a.get("ad_name", "")),
                         str(a.get("adgroup_id", "")), now),
                    )
                    synced += 1
                conn.commit()

    except Exception as e:
        return {"synced": synced, "error": str(e)}

    return {"synced": synced, "campaigns": len(campaigns), "adgroups": len(adgroups), "ads": len(ads)}
