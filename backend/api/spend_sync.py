"""Ad Spend Sync API — fetch ad spend from platform APIs into local spend table."""

from __future__ import annotations

import csv
import hmac
import io
import json
import os
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from attributionops.config import default_db_path
from attributionops.db import connect
from api.platform_auth import get_or_refresh_tiktok_token, get_tiktok_advertiser_id

router = APIRouter()
UTC = timezone.utc


class SpendCsvImportPayload(BaseModel):
    platform: str = "google"
    account_id: str = ""
    csv_text: str
    replace: bool = True


class GoogleAdsScriptSpendRow(BaseModel):
    date: str
    account_id: str = ""
    campaign_id: str = ""
    campaign_name: str = ""
    adset_id: str = ""
    adset_name: str = ""
    ad_id: str = ""
    ad_name: str = ""
    clicks: float = 0
    impressions: float = 0
    cost: float = 0


class GoogleAdsScriptSpendPayload(BaseModel):
    token: str = ""
    account_id: str = ""
    start_date: str = ""
    end_date: str = ""
    replace: bool = True
    rows: list[GoogleAdsScriptSpendRow]


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


def _ensure_ad_names_table(db_path: str) -> None:
    with connect(db_path) as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS ad_names (
                platform TEXT,
                entity_type TEXT,
                entity_id TEXT,
                name TEXT,
                parent_id TEXT,
                source TEXT DEFAULT 'manual',
                updated_at TEXT,
                PRIMARY KEY (platform, entity_type, entity_id)
            )"""
        )
        conn.commit()


def _spend_push_tokens() -> list[str]:
    return [
        str(token).strip()
        for token in (
            os.environ.get("GOOGLE_ADS_SCRIPT_TOKEN", ""),
            os.environ.get("SPEND_WEBHOOK_TOKEN", ""),
            os.environ.get("SITE_TOKEN", ""),
        )
        if str(token).strip()
    ]


def _authorize_spend_push(request: Request, payload_token: str = "") -> None:
    provided = (
        str(request.headers.get("x-mini-hyros-token") or "").strip()
        or str(request.query_params.get("token") or "").strip()
        or str(payload_token or "").strip()
    )
    allowed = _spend_push_tokens()
    if not allowed:
        raise HTTPException(status_code=500, detail="Set GOOGLE_ADS_SCRIPT_TOKEN or SITE_TOKEN before using spend push.")
    if not provided or not any(hmac.compare_digest(provided, token) for token in allowed):
        raise HTTPException(status_code=401, detail="Invalid spend push token.")


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


def _num(value: Any, default: float = 0.0) -> float:
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return default


def _fmt_decimal(value: float) -> str:
    if value == 0:
        return "0"
    return f"{value:.6f}".rstrip("0").rstrip(".")


def _normalize_header(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").strip().lstrip("\ufeff").lower())


def _normalized_aliases(values: list[str]) -> set[str]:
    return {_normalize_header(v) for v in values}


DATE_FIELDS = _normalized_aliases([
    "date",
    "day",
    "date_start",
    "date start",
    "segments.date",
    "stat_time_day",
    "stat time day",
])
ACCOUNT_FIELDS = _normalized_aliases([
    "account_id",
    "account id",
    "customer_id",
    "customer id",
    "customer",
    "ad_account_id",
    "advertiser_id",
    "advertiser id",
])
CAMPAIGN_ID_FIELDS = _normalized_aliases([
    "campaign_id",
    "campaign id",
    "campaign.id",
    "campaignid",
])
CAMPAIGN_NAME_FIELDS = _normalized_aliases([
    "campaign",
    "campaign_name",
    "campaign name",
    "campaign.name",
])
ADSET_ID_FIELDS = _normalized_aliases([
    "adset_id",
    "adset id",
    "ad set id",
    "ad_group_id",
    "ad group id",
    "adgroup_id",
    "adgroup id",
    "ad_group.id",
])
ADSET_NAME_FIELDS = _normalized_aliases([
    "adset",
    "ad set",
    "adset_name",
    "ad set name",
    "ad_group",
    "ad group",
    "adgroup",
    "ad_group.name",
])
AD_ID_FIELDS = _normalized_aliases([
    "ad_id",
    "ad id",
    "ad.id",
    "adgroupad.ad.id",
])
AD_NAME_FIELDS = _normalized_aliases([
    "ad",
    "ad_name",
    "ad name",
    "ad.name",
    "adgroupad.ad.name",
])
CREATIVE_ID_FIELDS = _normalized_aliases([
    "creative_id",
    "creative id",
    "creative.id",
])
CLICKS_FIELDS = _normalized_aliases(["clicks", "metrics.clicks"])
IMPRESSIONS_FIELDS = _normalized_aliases([
    "impressions",
    "impr",
    "impr.",
    "metrics.impressions",
])
COST_FIELDS = _normalized_aliases([
    "cost",
    "spend",
    "amount spent",
    "amount_spent",
    "cost (cad)",
    "cost cad",
    "cost (usd)",
    "cost usd",
    "metrics.cost",
])
COST_MICROS_FIELDS = _normalized_aliases([
    "cost_micros",
    "cost micros",
    "metrics.cost_micros",
    "metrics.costMicros",
])

FRENCH_MONTHS = {
    "janvier": "January",
    "fevrier": "February",
    "f\u00e9vrier": "February",
    "mars": "March",
    "avril": "April",
    "mai": "May",
    "juin": "June",
    "juillet": "July",
    "aout": "August",
    "ao\u00fbt": "August",
    "septembre": "September",
    "octobre": "October",
    "novembre": "November",
    "decembre": "December",
    "d\u00e9cembre": "December",
}


def _row_pick(row: dict[str, Any], aliases: set[str]) -> str:
    for key, value in row.items():
        if key in aliases and value not in (None, ""):
            return str(value).strip()
    return ""


def _parse_import_date(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    raw = raw.replace("\xa0", " ").strip()
    raw = raw.split("T", 1)[0].strip()
    if raw.endswith(" 00:00:00"):
        raw = raw[:-9].strip()

    for fr, en in FRENCH_MONTHS.items():
        raw = re.sub(fr, en, raw, flags=re.IGNORECASE)

    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError:
        pass

    for fmt in (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%b %d, %Y",
        "%B %d, %Y",
        "%d %b %Y",
        "%d %B %Y",
    ):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return ""


def _parse_number(value: Any, default: float = 0.0) -> float:
    s = str(value or "").strip()
    if not s or s in {"-", "--", "\u2014"}:
        return default

    negative = s.startswith("(") and s.endswith(")")
    s = s.strip("()").replace("\xa0", "").replace(" ", "")
    if "," in s and "." not in s:
        parts = s.split(",")
        if len(parts) == 2 and len(parts[-1]) in {1, 2}:
            s = ".".join(parts)
        elif len(parts) == 2:
            s = "".join(parts)
        elif len(parts[-1]) in {1, 2}:
            s = "".join(parts[:-1]) + "." + parts[-1]
        else:
            s = "".join(parts)
    else:
        s = s.replace(",", "")

    s = re.sub(r"[^0-9.\-]", "", s)
    if not s or s in {"-", ".", "-."}:
        return default
    try:
        value_f = float(s)
    except ValueError:
        return default
    return -value_f if negative else value_f


def _clean_id(value: Any) -> str:
    return str(value or "").strip().replace(",", "")


def _metadata_for_import(row: dict[str, str], imported_at: str) -> str:
    payload = {
        "campaign_name": _row_pick(row, CAMPAIGN_NAME_FIELDS),
        "adset_name": _row_pick(row, ADSET_NAME_FIELDS),
        "ad_name": _row_pick(row, AD_NAME_FIELDS),
        "source": "csv_import",
        "imported_at": imported_at,
    }
    return json.dumps(payload, separators=(",", ":"))


def _csv_reader(csv_text: str) -> csv.DictReader:
    cleaned = (csv_text or "").lstrip("\ufeff").strip()
    if not cleaned:
        raise HTTPException(status_code=400, detail="csv_text is required")
    sample = cleaned[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
    except csv.Error:
        dialect = csv.excel
    return csv.DictReader(io.StringIO(cleaned), dialect=dialect)


def _normalize_csv_row(row: dict[str, Any]) -> dict[str, str]:
    return {_normalize_header(k): str(v or "").strip() for k, v in row.items() if k is not None}


def _import_spend_csv_rows(payload: SpendCsvImportPayload) -> dict[str, Any]:
    platform = (payload.platform or "google").strip().lower()
    if platform not in {"google", "meta", "tiktok"}:
        raise HTTPException(status_code=400, detail="platform must be one of: google, meta, tiktok")

    reader = _csv_reader(payload.csv_text)
    if not reader.fieldnames:
        raise HTTPException(status_code=400, detail="CSV header row is missing")

    default_account_id = _clean_id(payload.account_id)
    imported_at = _now()
    parsed_rows: list[dict[str, Any]] = []
    skipped = 0
    warnings: list[str] = []

    for raw_row in reader:
        row = _normalize_csv_row(raw_row)
        day = _parse_import_date(_row_pick(row, DATE_FIELDS))
        if not day:
            skipped += 1
            continue

        account_id = _clean_id(_row_pick(row, ACCOUNT_FIELDS) or default_account_id)
        if platform == "google" and account_id:
            account_id = _clean_google_customer_id(account_id)

        campaign_name = _row_pick(row, CAMPAIGN_NAME_FIELDS)
        adset_name = _row_pick(row, ADSET_NAME_FIELDS)
        ad_name = _row_pick(row, AD_NAME_FIELDS)
        campaign_id = _clean_id(_row_pick(row, CAMPAIGN_ID_FIELDS) or campaign_name)
        adset_id = _clean_id(_row_pick(row, ADSET_ID_FIELDS))
        ad_id = _clean_id(_row_pick(row, AD_ID_FIELDS))
        creative_id = _clean_id(_row_pick(row, CREATIVE_ID_FIELDS))

        cost_micros_raw = _row_pick(row, COST_MICROS_FIELDS)
        if cost_micros_raw:
            cost = _parse_number(cost_micros_raw) / 1_000_000.0
        else:
            cost = _parse_number(_row_pick(row, COST_FIELDS))

        clicks = int(round(_parse_number(_row_pick(row, CLICKS_FIELDS))))
        impressions = int(round(_parse_number(_row_pick(row, IMPRESSIONS_FIELDS))))

        if not campaign_id and not adset_id and not ad_id:
            skipped += 1
            continue

        parsed_rows.append(
            {
                "platform": platform,
                "date": day,
                "account_id": account_id,
                "campaign_id": campaign_id,
                "adset_id": adset_id,
                "ad_id": ad_id,
                "creative_id": creative_id,
                "clicks": str(max(clicks, 0)),
                "cost": _fmt_decimal(max(cost, 0.0)),
                "impressions": str(max(impressions, 0)),
                "metadata": _metadata_for_import(row, imported_at),
                "campaign_name": campaign_name,
                "adset_name": adset_name,
                "ad_name": ad_name,
            }
        )

    if not parsed_rows:
        return {
            "ok": True,
            "platform": platform,
            "inserted": 0,
            "deleted": 0,
            "skipped": skipped,
            "warnings": warnings or ["No usable spend rows were found in the CSV."],
        }

    dates = [r["date"] for r in parsed_rows]
    start_date = min(dates)
    end_date = max(dates)
    db_path = _db()
    _ensure_spend_table(db_path)
    _ensure_ad_names_table(db_path)

    inserted = 0
    names_upserted = 0
    with connect(db_path) as conn:
        if payload.replace:
            if default_account_id:
                delete_account_id = _clean_google_customer_id(default_account_id) if platform == "google" else default_account_id
                deleted = conn.execute(
                    """
                    DELETE FROM spend
                    WHERE platform = ?
                      AND date BETWEEN ? AND ?
                      AND account_id = ?
                    """,
                    (platform, start_date, end_date, delete_account_id),
                ).rowcount
            else:
                deleted = conn.execute(
                    """
                    DELETE FROM spend
                    WHERE platform = ?
                      AND date BETWEEN ? AND ?
                    """,
                    (platform, start_date, end_date),
                ).rowcount
        else:
            deleted = 0

        for r in parsed_rows:
            conn.execute(
                """
                INSERT INTO spend (
                    platform, date, account_id, campaign_id,
                    adset_id, ad_id, creative_id,
                    clicks, cost, impressions, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    r["platform"],
                    r["date"],
                    r["account_id"],
                    r["campaign_id"],
                    r["adset_id"],
                    r["ad_id"],
                    r["creative_id"],
                    r["clicks"],
                    r["cost"],
                    r["impressions"],
                    r["metadata"],
                ),
            )
            inserted += 1

            if r["campaign_id"] and r["campaign_name"] and r["campaign_name"] != r["campaign_id"]:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO ad_names (platform, entity_type, entity_id, name, parent_id, source, updated_at)
                    VALUES (?, 'campaign', ?, ?, '', 'csv_import', ?)
                    """,
                    (platform, r["campaign_id"], r["campaign_name"], imported_at),
                )
                names_upserted += 1
            if r["adset_id"] and r["adset_name"] and r["adset_name"] != r["adset_id"]:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO ad_names (platform, entity_type, entity_id, name, parent_id, source, updated_at)
                    VALUES (?, 'adset', ?, ?, ?, 'csv_import', ?)
                    """,
                    (platform, r["adset_id"], r["adset_name"], r["campaign_id"], imported_at),
                )
                names_upserted += 1
            if r["ad_id"] and r["ad_name"] and r["ad_name"] != r["ad_id"]:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO ad_names (platform, entity_type, entity_id, name, parent_id, source, updated_at)
                    VALUES (?, 'ad', ?, ?, ?, 'csv_import', ?)
                    """,
                    (platform, r["ad_id"], r["ad_name"], r["adset_id"], imported_at),
                )
                names_upserted += 1

        conn.commit()

    if not default_account_id:
        warnings.append("No account_id was supplied. Rows were imported, but account-level drilldowns may stay blank.")

    return {
        "ok": True,
        "platform": platform,
        "inserted": inserted,
        "deleted": int(deleted or 0),
        "skipped": skipped,
        "names_upserted": names_upserted,
        "date_range": {"start": start_date, "end": end_date},
        "replace": bool(payload.replace),
        "warnings": warnings,
    }


def _import_google_ads_script_rows(payload: GoogleAdsScriptSpendPayload) -> dict[str, Any]:
    account_id_default = _clean_google_customer_id(_clean_id(payload.account_id))
    imported_at = _now()
    parsed_rows: list[dict[str, Any]] = []
    skipped = 0
    warnings: list[str] = []

    for row in payload.rows:
        day = _parse_import_date(row.date)
        if not day:
            skipped += 1
            continue

        account_id = _clean_google_customer_id(_clean_id(row.account_id) or account_id_default)
        campaign_id = _clean_id(row.campaign_id)
        adset_id = _clean_id(row.adset_id)
        ad_id = _clean_id(row.ad_id)
        campaign_name = str(row.campaign_name or "").strip()
        adset_name = str(row.adset_name or "").strip()
        ad_name = str(row.ad_name or "").strip()

        if not campaign_id and not adset_id and not ad_id:
            skipped += 1
            continue

        metadata = json.dumps(
            {
                "campaign_name": campaign_name,
                "adset_name": adset_name,
                "ad_name": ad_name,
                "source": "google_ads_script",
                "imported_at": imported_at,
            },
            separators=(",", ":"),
        )
        parsed_rows.append(
            {
                "platform": "google",
                "date": day,
                "account_id": account_id,
                "campaign_id": campaign_id,
                "adset_id": adset_id,
                "ad_id": ad_id,
                "creative_id": "",
                "clicks": str(max(int(round(_num(row.clicks))), 0)),
                "cost": _fmt_decimal(max(_num(row.cost), 0.0)),
                "impressions": str(max(int(round(_num(row.impressions))), 0)),
                "metadata": metadata,
                "campaign_name": campaign_name,
                "adset_name": adset_name,
                "ad_name": ad_name,
            }
        )

    if not parsed_rows:
        start_date = _parse_import_date(payload.start_date)
        end_date = _parse_import_date(payload.end_date)
        if start_date and end_date and start_date > end_date:
            start_date, end_date = end_date, start_date
        warnings.append(
            "No usable spend rows were received from Google Ads Script; existing Google spend rows were preserved."
        )

        return {
            "ok": True,
            "platform": "google",
            "inserted": 0,
            "deleted": 0,
            "skipped": skipped,
            "date_range": {"start": start_date, "end": end_date} if start_date and end_date else {},
            "replace": bool(payload.replace),
            "warnings": warnings,
        }

    dates = [r["date"] for r in parsed_rows]
    start_date = _parse_import_date(payload.start_date) or min(dates)
    end_date = _parse_import_date(payload.end_date) or max(dates)
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    db_path = _db()
    _ensure_spend_table(db_path)
    _ensure_ad_names_table(db_path)

    inserted = 0
    names_upserted = 0
    with connect(db_path) as conn:
        if payload.replace:
            if account_id_default:
                deleted = conn.execute(
                    """
                    DELETE FROM spend
                    WHERE platform = 'google'
                      AND date BETWEEN ? AND ?
                      AND account_id = ?
                    """,
                    (start_date, end_date, account_id_default),
                ).rowcount
            else:
                deleted = conn.execute(
                    """
                    DELETE FROM spend
                    WHERE platform = 'google'
                      AND date BETWEEN ? AND ?
                    """,
                    (start_date, end_date),
                ).rowcount
        else:
            deleted = 0

        for r in parsed_rows:
            conn.execute(
                """
                INSERT INTO spend (
                    platform, date, account_id, campaign_id,
                    adset_id, ad_id, creative_id,
                    clicks, cost, impressions, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    r["platform"],
                    r["date"],
                    r["account_id"],
                    r["campaign_id"],
                    r["adset_id"],
                    r["ad_id"],
                    r["creative_id"],
                    r["clicks"],
                    r["cost"],
                    r["impressions"],
                    r["metadata"],
                ),
            )
            inserted += 1

            if r["campaign_id"] and r["campaign_name"] and r["campaign_name"] != r["campaign_id"]:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO ad_names (platform, entity_type, entity_id, name, parent_id, source, updated_at)
                    VALUES ('google', 'campaign', ?, ?, '', 'google_ads_script', ?)
                    """,
                    (r["campaign_id"], r["campaign_name"], imported_at),
                )
                names_upserted += 1
            if r["adset_id"] and r["adset_name"] and r["adset_name"] != r["adset_id"]:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO ad_names (platform, entity_type, entity_id, name, parent_id, source, updated_at)
                    VALUES ('google', 'adset', ?, ?, ?, 'google_ads_script', ?)
                    """,
                    (r["adset_id"], r["adset_name"], r["campaign_id"], imported_at),
                )
                names_upserted += 1
            if r["ad_id"] and r["ad_name"] and r["ad_name"] != r["ad_id"]:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO ad_names (platform, entity_type, entity_id, name, parent_id, source, updated_at)
                    VALUES ('google', 'ad', ?, ?, ?, 'google_ads_script', ?)
                    """,
                    (r["ad_id"], r["ad_name"], r["adset_id"], imported_at),
                )
                names_upserted += 1

        conn.commit()

    if not account_id_default:
        warnings.append("No account_id was supplied. Rows were imported, but account-level drilldowns may stay blank.")

    return {
        "ok": True,
        "platform": "google",
        "inserted": inserted,
        "deleted": int(deleted or 0),
        "skipped": skipped,
        "names_upserted": names_upserted,
        "date_range": {"start": start_date, "end": end_date},
        "replace": bool(payload.replace),
        "source": "google_ads_script",
        "warnings": warnings,
    }


def _deep_get(payload: dict[str, Any], *path: str) -> Any:
    cur: Any = payload
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _first_present(payload: dict[str, Any], *paths: tuple[str, ...]) -> Any:
    for path in paths:
        value = _deep_get(payload, *path)
        if value not in (None, ""):
            return value
    return ""


def _clean_google_customer_id(customer_id: str) -> str:
    return customer_id.replace("customers/", "").replace("-", "").strip()


def _google_ads_api_version() -> str:
    raw = str(os.environ.get("GOOGLE_ADS_API_VERSION", "v24") or "").strip()
    if not raw:
        return "v24"
    return raw if raw.startswith("v") else f"v{raw}"


def _google_ads_api_sync_disabled() -> bool:
    mode = str(os.environ.get("GOOGLE_ADS_SYNC_MODE", "api") or "api").strip().lower()
    disabled = str(os.environ.get("DISABLE_GOOGLE_ADS_API_SYNC", "") or "").strip().lower()
    return mode in {"script", "push", "manual", "disabled", "off"} or disabled in {"1", "true", "yes", "on"}


def _google_api_error_message(response: httpx.Response | None, fallback: str) -> str:
    if response is None:
        return fallback

    try:
        payload = response.json()
    except Exception:
        text = " ".join((response.text or fallback).split())
        return f"HTTP {response.status_code}: {text[:300]}"

    error = payload.get("error") if isinstance(payload, dict) else {}
    if not isinstance(error, dict):
        return f"HTTP {response.status_code}: {str(payload)[:300]}"

    status = str(error.get("status") or "").strip()
    message = str(error.get("message") or "").strip()
    google_code = ""
    detailed_message = ""

    for detail in error.get("details") or []:
        if not isinstance(detail, dict):
            continue
        for item in detail.get("errors") or []:
            if not isinstance(item, dict):
                continue
            error_code = item.get("errorCode") or {}
            if isinstance(error_code, dict) and error_code:
                google_code = str(next(iter(error_code.values())) or "").strip()
            detailed_message = str(item.get("message") or "").strip()
            break
        if google_code or detailed_message:
            break

    parts = [f"HTTP {response.status_code}"]
    if status:
        parts.append(status)
    if google_code:
        parts.append(google_code)
    if detailed_message:
        parts.append(detailed_message)
    elif message:
        parts.append(message)
    return ": ".join(parts)[:600]


def _meta_api_error_message(response: httpx.Response | None, fallback: str) -> str:
    if response is None:
        return fallback

    try:
        payload = response.json()
    except Exception:
        text = " ".join((response.text or fallback).split())
        return f"HTTP {response.status_code}: {text[:300]}"

    error = payload.get("error") if isinstance(payload, dict) else {}
    if not isinstance(error, dict):
        return f"HTTP {response.status_code}: {str(payload)[:300]}"

    message = str(error.get("message") or "Meta API request failed").strip()
    error_type = str(error.get("type") or "").strip()
    code = error.get("code")
    subcode = error.get("error_subcode")

    meta_bits = []
    if error_type:
        meta_bits.append(error_type)
    if code is not None:
        meta_bits.append(f"code={code}")
    if subcode is not None:
        meta_bits.append(f"subcode={subcode}")

    prefix = f"HTTP {response.status_code}"
    if meta_bits:
        prefix += f": {' '.join(meta_bits)}"
    if message:
        return f"{prefix}: {message}"[:600]
    return prefix[:600]


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
    name_rows: dict[tuple[str, str], tuple[str, str]] = {}

    _ensure_spend_table(db_path)
    _ensure_ad_names_table(db_path)

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
            campaign_name = str(r.get("campaign_name") or "").strip()
            adset_name = str(r.get("adset_name") or "").strip()
            ad_name = str(r.get("ad_name") or "").strip()
            clicks = str(r.get("clicks") or "0").strip()
            spend = str(r.get("spend") or "0").strip()
            impressions = str(r.get("impressions") or "0").strip()

            metadata = json.dumps(
                {
                    "campaign_name": campaign_name,
                    "adset_name": adset_name,
                    "ad_name": ad_name,
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

            if campaign_id and campaign_name:
                name_rows[("campaign", campaign_id)] = (campaign_name, "")
            if adset_id and adset_name:
                name_rows[("adset", adset_id)] = (adset_name, campaign_id)
            if ad_id and ad_name:
                name_rows[("ad", ad_id)] = (ad_name, adset_id)

        for (entity_type, entity_id), (name, parent_id) in name_rows.items():
            conn.execute(
                """
                INSERT INTO ad_names (
                    platform, entity_type, entity_id, name, parent_id, source, updated_at
                ) VALUES ('meta', ?, ?, ?, ?, 'meta_insights_api', ?)
                ON CONFLICT(platform, entity_type, entity_id) DO UPDATE SET
                    name = excluded.name,
                    parent_id = excluded.parent_id,
                    source = excluded.source,
                    updated_at = excluded.updated_at
                """,
                (entity_type, entity_id, name, parent_id, synced_at),
            )

        conn.commit()

    return {
        "deleted": int(deleted if deleted is not None and deleted > 0 else 0),
        "inserted": inserted,
        "names_upserted": len(name_rows),
    }


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
            "names_upserted": write_stats["names_upserted"],
            "date_range": {"start": start_date, "end": end_date},
            "account_id": ad_account_id.replace("act_", ""),
        }
    except httpx.HTTPStatusError as exc:
        details = _meta_api_error_message(exc.response, str(exc))
        return {"synced": 0, "error": f"Meta Insights API error: {details}"}
    except Exception as exc:
        return {"synced": 0, "error": str(exc)}


async def _google_access_token_from_refresh(client_id: str, client_secret: str, refresh_token: str) -> str:
    url = "https://oauth2.googleapis.com/token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(url, data=payload)
        resp.raise_for_status()
        token_payload = resp.json()

    token = str(token_payload.get("access_token") or "").strip()
    if not token:
        raise RuntimeError("Google OAuth token exchange returned no access_token")
    return token


async def _fetch_google_insights(
    access_token: str,
    developer_token: str,
    customer_id: str,
    start_date: str,
    end_date: str,
    login_customer_id: str = "",
) -> list[dict[str, Any]]:
    clean_customer_id = _clean_google_customer_id(customer_id)
    gaql = (
        "SELECT "
        "segments.date, "
        "campaign.id, campaign.name, "
        "ad_group.id, ad_group.name, "
        "ad_group_ad.ad.id, ad_group_ad.ad.name, "
        "metrics.clicks, metrics.impressions, metrics.cost_micros "
        "FROM ad_group_ad "
        f"WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'"
    )

    api_version = _google_ads_api_version()
    url = f"https://googleads.googleapis.com/{api_version}/customers/{clean_customer_id}/googleAds:search"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "developer-token": developer_token,
        "Content-Type": "application/json",
    }
    if login_customer_id.strip():
        headers["login-customer-id"] = _clean_google_customer_id(login_customer_id)

    rows: list[dict[str, Any]] = []
    next_page_token = ""

    async with httpx.AsyncClient(timeout=45) as client:
        while True:
            payload: dict[str, Any] = {"query": gaql, "pageSize": 10000}
            if next_page_token:
                payload["pageToken"] = next_page_token

            resp = await client.post(url, json=payload, headers=headers)
            resp.raise_for_status()
            result = resp.json()
            rows.extend(result.get("results") or [])

            next_page_token = str(result.get("nextPageToken") or "").strip()
            if not next_page_token:
                break

    return rows


def _write_google_spend_rows(
    db_path: str,
    customer_id: str,
    start_date: str,
    end_date: str,
    rows: list[dict[str, Any]],
) -> dict[str, int]:
    clean_customer_id = _clean_google_customer_id(customer_id)
    synced_at = _now()
    inserted = 0

    with connect(db_path) as conn:
        deleted = conn.execute(
            """
            DELETE FROM spend
            WHERE platform = 'google'
              AND date BETWEEN ? AND ?
              AND (account_id = ? OR account_id = ?)
            """,
            (start_date, end_date, clean_customer_id, customer_id.strip()),
        ).rowcount

        for r in rows:
            day = str(_first_present(r, ("segments", "date"), ("segments", "date_start"))).strip()
            if not day:
                continue

            campaign_id = str(_first_present(r, ("campaign", "id"), ("campaign", "resourceName"))).strip()
            adset_id = str(_first_present(r, ("adGroup", "id"), ("ad_group", "id"))).strip()
            ad_id = str(
                _first_present(
                    r,
                    ("adGroupAd", "ad", "id"),
                    ("ad_group_ad", "ad", "id"),
                )
            ).strip()
            clicks = str(int(_num(_first_present(r, ("metrics", "clicks")), 0.0)))
            impressions = str(int(_num(_first_present(r, ("metrics", "impressions")), 0.0)))
            cost_micros = _num(_first_present(r, ("metrics", "costMicros"), ("metrics", "cost_micros")), 0.0)
            cost = _fmt_decimal(cost_micros / 1_000_000.0)

            metadata = json.dumps(
                {
                    "campaign_name": str(_first_present(r, ("campaign", "name"))).strip(),
                    "adset_name": str(_first_present(r, ("adGroup", "name"), ("ad_group", "name"))).strip(),
                    "ad_name": str(
                        _first_present(
                            r,
                            ("adGroupAd", "ad", "name"),
                            ("ad_group_ad", "ad", "name"),
                        )
                    ).strip(),
                    "source": "google_ads_api",
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
                    "google",
                    day,
                    clean_customer_id,
                    campaign_id,
                    adset_id,
                    ad_id,
                    "",
                    clicks,
                    cost,
                    impressions,
                    metadata,
                ),
            )
            inserted += 1

        conn.commit()

    return {"deleted": int(deleted if deleted is not None and deleted > 0 else 0), "inserted": inserted}


async def _sync_google_spend(start_date: str, end_date: str) -> dict[str, Any]:
    if _google_ads_api_sync_disabled():
        return {
            "synced": 0,
            "skipped": True,
            "reason": "Google Ads API sync is disabled; use Google Ads Script spend push.",
            "date_range": {"start": start_date, "end": end_date},
        }

    client_id = os.environ.get("GOOGLE_ADS_CLIENT_ID", "").strip()
    client_secret = os.environ.get("GOOGLE_ADS_CLIENT_SECRET", "").strip()
    refresh_token = os.environ.get("GOOGLE_ADS_REFRESH_TOKEN", "").strip()
    developer_token = os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN", "").strip()
    customer_id = os.environ.get("GOOGLE_ADS_CUSTOMER_ID", "").strip()
    login_customer_id = os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "").strip()

    if not all([client_id, client_secret, refresh_token, developer_token, customer_id]):
        return {
            "synced": 0,
            "error": (
                "GOOGLE_ADS_CLIENT_ID, GOOGLE_ADS_CLIENT_SECRET, GOOGLE_ADS_REFRESH_TOKEN, "
                "GOOGLE_ADS_DEVELOPER_TOKEN, and GOOGLE_ADS_CUSTOMER_ID are required"
            ),
        }

    db_path = _db()
    _ensure_spend_table(db_path)

    try:
        access_token = await _google_access_token_from_refresh(client_id, client_secret, refresh_token)
        api_rows = await _fetch_google_insights(
            access_token=access_token,
            developer_token=developer_token,
            customer_id=customer_id,
            start_date=start_date,
            end_date=end_date,
            login_customer_id=login_customer_id,
        )
        write_stats = _write_google_spend_rows(db_path, customer_id, start_date, end_date, api_rows)
        return {
            "synced": write_stats["inserted"],
            "fetched": len(api_rows),
            "deleted": write_stats["deleted"],
            "date_range": {"start": start_date, "end": end_date},
            "customer_id": _clean_google_customer_id(customer_id),
            "api_version": _google_ads_api_version(),
        }
    except httpx.HTTPStatusError as exc:
        details = _google_api_error_message(exc.response, str(exc))
        if "login-customer-id" in details and not login_customer_id:
            details += " Set GOOGLE_ADS_LOGIN_CUSTOMER_ID to the manager account ID, or use a customer ID directly accessible by this OAuth user."
        return {"synced": 0, "error": f"Google Ads API error: {details}"}
    except Exception as exc:
        return {"synced": 0, "error": str(exc)}


def _tiktok_value(row: dict[str, Any], key: str) -> Any:
    direct = row.get(key)
    if direct not in (None, ""):
        return direct

    dimensions = row.get("dimensions") or {}
    metrics = row.get("metrics") or {}

    if isinstance(dimensions, dict) and dimensions.get(key) not in (None, ""):
        return dimensions.get(key)
    if isinstance(metrics, dict) and metrics.get(key) not in (None, ""):
        return metrics.get(key)
    return ""


async def _fetch_tiktok_report_level(
    client: httpx.AsyncClient,
    access_token: str,
    advertiser_id: str,
    start_date: str,
    end_date: str,
    data_level: str,
    dimensions: list[str],
) -> list[dict[str, Any]]:
    """Fetch one level of TikTok spend report (campaign, adgroup, or ad)."""
    url = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"
    headers = {"Access-Token": access_token, "Content-Type": "application/json"}
    metrics = ["spend", "impressions", "clicks"]
    page = 1
    page_size = 1000
    total_page = 1
    rows: list[dict[str, Any]] = []

    while True:
        params = {
            "advertiser_id": advertiser_id,
            "service_type": "AUCTION",
            "report_type": "BASIC",
            "data_level": data_level,
            "start_date": start_date,
            "end_date": end_date,
            "dimensions": json.dumps(dimensions, separators=(",", ":")),
            "metrics": json.dumps(metrics, separators=(",", ":")),
            "page": str(page),
            "page_size": str(page_size),
        }
        resp = await client.get(url, params=params, headers=headers)
        resp.raise_for_status()
        payload = resp.json()

        code = int(payload.get("code") or 0)
        if code != 0:
            raise RuntimeError(f"TikTok API error ({code}): {payload.get('message') or payload.get('request_id')}")

        data = payload.get("data") or {}
        batch = data.get("list") or []
        rows.extend(batch)

        page_info = data.get("page_info") or {}
        reported_page = int(page_info.get("page") or page)
        total_page = int(page_info.get("total_page") or total_page)

        if reported_page >= total_page:
            break
        page = reported_page + 1

    return rows


def _build_tiktok_ad_parent_map(db_path: str) -> dict[str, dict[str, str]]:
    """Build {ad_id: {campaign_id, adset_id}} lookup from ad_names table."""
    ad_map: dict[str, dict[str, str]] = {}
    adset_map: dict[str, str] = {}  # adset_id → campaign_id

    try:
        with connect(db_path) as conn:
            adsets = conn.execute(
                "SELECT entity_id, parent_id FROM ad_names WHERE platform='tiktok' AND entity_type='adset'"
            ).fetchall()
            for row in adsets:
                if row[0] and row[1]:
                    adset_map[str(row[0])] = str(row[1])

            ads = conn.execute(
                "SELECT entity_id, parent_id FROM ad_names WHERE platform='tiktok' AND entity_type='ad'"
            ).fetchall()
            for row in ads:
                ad_id = str(row[0])
                adset_id = str(row[1]) if row[1] else ""
                campaign_id = adset_map.get(adset_id, "")
                ad_map[ad_id] = {"adset_id": adset_id, "campaign_id": campaign_id}
    except Exception:
        pass

    return ad_map


async def _fetch_tiktok_report(
    access_token: str,
    advertiser_id: str,
    start_date: str,
    end_date: str,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch TikTok spend at both ad and campaign levels."""
    async with httpx.AsyncClient(timeout=45) as client:
        ad_rows = await _fetch_tiktok_report_level(
            client, access_token, advertiser_id, start_date, end_date,
            data_level="AUCTION_AD",
            dimensions=["ad_id", "stat_time_day"],
        )
        campaign_rows = await _fetch_tiktok_report_level(
            client, access_token, advertiser_id, start_date, end_date,
            data_level="AUCTION_CAMPAIGN",
            dimensions=["campaign_id", "stat_time_day"],
        )

    return {"ad_rows": ad_rows, "campaign_rows": campaign_rows}


def _write_tiktok_spend_rows(
    db_path: str,
    advertiser_id: str,
    start_date: str,
    end_date: str,
    rows: list[dict[str, Any]] | None = None,
    *,
    ad_rows: list[dict[str, Any]] | None = None,
    campaign_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    clean_advertiser_id = advertiser_id.strip()
    synced_at = _now()
    inserted = 0
    inserted_ads = 0
    inserted_campaign_adjustments = 0
    skipped_unmapped_ads = 0
    adjustment_cost = 0.0

    ad_rows = list(ad_rows if ad_rows is not None else (rows or []))
    campaign_rows = list(campaign_rows or [])
    campaign_report_available = bool(campaign_rows)
    ad_parent_map = _build_tiktok_ad_parent_map(db_path)
    ad_totals_by_campaign_day: dict[tuple[str, str], dict[str, float]] = defaultdict(
        lambda: {"cost": 0.0, "clicks": 0.0, "impressions": 0.0}
    )

    with connect(db_path) as conn:
        deleted = conn.execute(
            """
            DELETE FROM spend
            WHERE platform = 'tiktok'
              AND date BETWEEN ? AND ?
              AND account_id = ?
            """,
            (start_date, end_date, clean_advertiser_id),
        ).rowcount

        for r in ad_rows:
            day = _parse_import_date(str(_tiktok_value(r, "stat_time_day") or _tiktok_value(r, "date")).strip())
            if not day:
                continue

            ad_id = str(_tiktok_value(r, "ad_id")).strip()
            campaign_id = str(_tiktok_value(r, "campaign_id")).strip()
            adset_id = str(_tiktok_value(r, "adgroup_id") or _tiktok_value(r, "adset_id")).strip()

            if ad_id and (not campaign_id or not adset_id):
                parent = ad_parent_map.get(ad_id, {})
                if not campaign_id:
                    campaign_id = parent.get("campaign_id", "")
                if not adset_id:
                    adset_id = parent.get("adset_id", "")

            cost_value = _num(_tiktok_value(r, "spend"), 0.0)
            clicks_value = int(_num(_tiktok_value(r, "clicks"), 0.0))
            impressions_value = int(_num(_tiktok_value(r, "impressions"), 0.0))

            if campaign_report_available and not campaign_id:
                skipped_unmapped_ads += 1
                continue

            if campaign_id:
                ad_total = ad_totals_by_campaign_day[(day, campaign_id)]
                ad_total["cost"] += cost_value
                ad_total["clicks"] += clicks_value
                ad_total["impressions"] += impressions_value

            metadata = json.dumps(
                {
                    "campaign_name": str(_tiktok_value(r, "campaign_name")).strip(),
                    "adset_name": str(_tiktok_value(r, "adgroup_name") or _tiktok_value(r, "adset_name")).strip(),
                    "ad_name": str(_tiktok_value(r, "ad_name")).strip(),
                    "source": "tiktok_report_api_ad",
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
                    "tiktok",
                    day,
                    clean_advertiser_id,
                    campaign_id,
                    adset_id,
                    ad_id,
                    "",
                    str(clicks_value),
                    _fmt_decimal(cost_value),
                    str(impressions_value),
                    metadata,
                ),
            )
            inserted += 1
            inserted_ads += 1

        for r in campaign_rows:
            day = _parse_import_date(str(_tiktok_value(r, "stat_time_day") or _tiktok_value(r, "date")).strip())
            campaign_id = str(_tiktok_value(r, "campaign_id")).strip()
            if not day or not campaign_id:
                continue

            campaign_cost = _num(_tiktok_value(r, "spend"), 0.0)
            campaign_clicks = int(_num(_tiktok_value(r, "clicks"), 0.0))
            campaign_impressions = int(_num(_tiktok_value(r, "impressions"), 0.0))
            ad_total = ad_totals_by_campaign_day.get((day, campaign_id), {})

            cost_delta = round(campaign_cost - float(ad_total.get("cost") or 0.0), 6)
            clicks_delta = campaign_clicks - int(ad_total.get("clicks") or 0)
            impressions_delta = campaign_impressions - int(ad_total.get("impressions") or 0)

            if cost_delta <= 0.000001 and clicks_delta <= 0 and impressions_delta <= 0:
                continue

            metadata = json.dumps(
                {
                    "campaign_name": str(_tiktok_value(r, "campaign_name")).strip(),
                    "adset_name": "",
                    "ad_name": "",
                    "source": "tiktok_report_api_campaign_adjustment",
                    "synced_at": synced_at,
                    "campaign_total_cost": _fmt_decimal(campaign_cost),
                    "mapped_ad_cost": _fmt_decimal(float(ad_total.get("cost") or 0.0)),
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
                    "tiktok",
                    day,
                    clean_advertiser_id,
                    campaign_id,
                    "",
                    "",
                    "",
                    str(max(clicks_delta, 0)),
                    _fmt_decimal(max(cost_delta, 0.0)),
                    str(max(impressions_delta, 0)),
                    metadata,
                ),
            )
            inserted += 1
            inserted_campaign_adjustments += 1
            adjustment_cost += max(cost_delta, 0.0)

        conn.commit()

    return {
        "deleted": int(deleted if deleted is not None and deleted > 0 else 0),
        "inserted": inserted,
        "inserted_ads": inserted_ads,
        "inserted_campaign_adjustments": inserted_campaign_adjustments,
        "skipped_unmapped_ads": skipped_unmapped_ads,
        "adjustment_cost": round(adjustment_cost, 2),
    }


async def _sync_tiktok_spend(start_date: str, end_date: str) -> dict[str, Any]:
    db_path = _db()
    access_token = (await get_or_refresh_tiktok_token(db_path)).strip()
    advertiser_id = get_tiktok_advertiser_id(db_path).strip()

    if not access_token or not advertiser_id:
        return {
            "synced": 0,
            "error": "TikTok not connected. Visit /api/platform-auth/tiktok/connect to authorize.",
        }

    _ensure_spend_table(db_path)

    try:
        api_report = await _fetch_tiktok_report(access_token, advertiser_id, start_date, end_date)
        ad_rows = api_report.get("ad_rows") or []
        campaign_rows = api_report.get("campaign_rows") or []
        write_stats = _write_tiktok_spend_rows(
            db_path,
            advertiser_id,
            start_date,
            end_date,
            ad_rows=ad_rows,
            campaign_rows=campaign_rows,
        )
        return {
            "synced": write_stats["inserted"],
            "fetched": len(ad_rows) + len(campaign_rows),
            "fetched_ads": len(ad_rows),
            "fetched_campaigns": len(campaign_rows),
            "deleted": write_stats["deleted"],
            "inserted_ads": write_stats.get("inserted_ads", 0),
            "inserted_campaign_adjustments": write_stats.get("inserted_campaign_adjustments", 0),
            "skipped_unmapped_ads": write_stats.get("skipped_unmapped_ads", 0),
            "adjustment_cost": write_stats.get("adjustment_cost", 0.0),
            "date_range": {"start": start_date, "end": end_date},
            "advertiser_id": advertiser_id,
        }
    except httpx.HTTPStatusError as exc:
        details = exc.response.text[:600] if exc.response is not None else str(exc)
        return {"synced": 0, "error": f"TikTok API error: {details}"}
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
    - google (Google Ads API)
    - tiktok (TikTok Reporting API)

    Query params:
    - platform: meta | google | tiktok | all
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

    async def run_platform(label: str, coro: Any) -> dict[str, Any]:
        try:
            result = await coro
            if isinstance(result, dict):
                return result
            return {"synced": 0, "error": f"{label} sync returned an invalid response"}
        except Exception as exc:
            return {"synced": 0, "error": f"{label} sync failed: {exc}"}

    if p in {"meta", "all"}:
        meta_result = await run_platform("Meta", _sync_meta_spend(s, e))
        results["platforms"]["meta"] = meta_result
        results["synced"] += int(meta_result.get("synced", 0) or 0)
        if meta_result.get("error"):
            results["errors"].append(f"meta: {meta_result['error']}")

    if p in {"google", "all"}:
        google_result = await run_platform("Google", _sync_google_spend(s, e))
        results["platforms"]["google"] = google_result
        results["synced"] += int(google_result.get("synced", 0) or 0)
        if google_result.get("error"):
            results["errors"].append(f"google: {google_result['error']}")

    if p in {"tiktok", "all"}:
        tiktok_result = await run_platform("TikTok", _sync_tiktok_spend(s, e))
        results["platforms"]["tiktok"] = tiktok_result
        results["synced"] += int(tiktok_result.get("synced", 0) or 0)
        if tiktok_result.get("error"):
            results["errors"].append(f"tiktok: {tiktok_result['error']}")

    return results


@router.post("/import_csv")
async def import_spend_csv(payload: SpendCsvImportPayload):
    """Import exported ad spend CSV rows into the local spend table.

    This is the fallback path for accounts where platform APIs are blocked or
    still pending approval. It writes the same spend rows as API sync, so the
    attribution dashboard uses imported cost/click/impression data immediately.
    """
    return _import_spend_csv_rows(payload)


@router.post("/google-ads-script")
async def import_google_ads_script_spend(request: Request, payload: GoogleAdsScriptSpendPayload):
    """Receive Google Ads spend rows pushed by a Google Ads Script.

    This bypasses Google Ads API developer-token approval because the report is
    generated inside the advertiser account and posted into Mini Hyros.
    """
    _authorize_spend_push(request, payload.token)
    return _import_google_ads_script_rows(payload)
