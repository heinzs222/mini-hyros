from __future__ import annotations

import json

from attributionops.db import query
from attributionops.util import parse_json, to_float, to_int


def _traffic_source_for_platform(platform: str) -> tuple[str, str]:
    p = (platform or "").lower()
    if p == "meta":
        return ("facebook", "paid_social")
    if p == "google":
        return ("google", "cpc")
    if p == "tiktok":
        return ("tiktok", "paid_social")
    return (p or "unknown", "paid")


def ads_list_platforms(db_path: str) -> dict[str, object]:
    rows = query(db_path, "SELECT DISTINCT platform FROM spend ORDER BY platform;").rows
    platforms = [str(r["platform"]) for r in rows if r.get("platform")]
    return {"platforms": platforms}


def ads_get_spend(
    db_path: str,
    *,
    platform: str | None,
    start_date: str,
    end_date: str,
    breakdown: str,
) -> dict[str, object]:
    breakdown = breakdown.lower().strip()
    platform_filter = ""
    params = {"start_date": start_date, "end_date": end_date}
    if platform and platform.lower() != "all":
        platform_filter = "AND platform = :platform"
        params["platform"] = platform

    rows = query(
        db_path,
        f"""
        SELECT
          platform, date, account_id, campaign_id, adset_id, ad_id, creative_id,
          clicks, cost, impressions, metadata
        FROM spend
        WHERE date BETWEEN :start_date AND :end_date
        {platform_filter}
        """,
        params,
    ).rows

    agg: dict[tuple[str, ...], dict[str, object]] = {}

    for r in rows:
        p = str(r.get("platform") or "")
        d = str(r.get("date") or "")
        account_id = str(r.get("account_id") or "")
        campaign_id = str(r.get("campaign_id") or "")
        adset_id = str(r.get("adset_id") or "")
        ad_id = str(r.get("ad_id") or "")
        creative_id = str(r.get("creative_id") or "")
        md = parse_json(r.get("metadata"))

        if breakdown == "day":
            key = (d,)
            name = d
        elif breakdown == "traffic_source":
            utm_source, utm_medium = _traffic_source_for_platform(p)
            key = (utm_source, utm_medium)
            name = f"{utm_source} / {utm_medium}"
        elif breakdown == "ad_account":
            key = (p, account_id)
            name = account_id or f"{p}_unknown_account"
        elif breakdown == "campaign":
            key = (p, account_id, campaign_id)
            name = md.get("campaign_name") or campaign_id
        elif breakdown == "ad_set":
            key = (p, account_id, campaign_id, adset_id)
            name = md.get("adset_name") or adset_id
        elif breakdown == "ad":
            key = (p, account_id, campaign_id, adset_id, ad_id)
            name = md.get("ad_name") or ad_id
        else:
            raise ValueError("breakdown must be one of: day, traffic_source, ad_account, campaign, ad_set, ad")

        clicks = to_int(r.get("clicks"))
        cost = to_float(r.get("cost"))
        impressions = to_int(r.get("impressions"))

        if key not in agg:
            agg[key] = {
                "name": name,
                "platform": p,
                "account_id": account_id,
                "campaign_id": campaign_id,
                "adset_id": adset_id,
                "ad_id": ad_id,
                "creative_id": creative_id,
                "clicks": 0,
                "cost": 0.0,
                "impressions": 0,
                "metadata": md,
            }

        agg[key]["clicks"] = int(agg[key]["clicks"]) + clicks
        agg[key]["cost"] = float(agg[key]["cost"]) + cost
        agg[key]["impressions"] = int(agg[key]["impressions"]) + impressions

        if not agg[key]["metadata"] and md:
            agg[key]["metadata"] = md

    out_rows = list(agg.values())
    for row in out_rows:
        row["cost"] = float(f"{float(row['cost']):.2f}")
        row["metadata"] = json.dumps(row["metadata"], separators=(",", ":"))

    return {"rows": out_rows, "breakdown": breakdown, "start_date": start_date, "end_date": end_date}


def ads_get_reported_value(
    db_path: str,
    *,
    platform: str | None,
    start_date: str,
    end_date: str,
    breakdown: str,
    conversion_type: str,
) -> dict[str, object]:
    breakdown = breakdown.lower().strip()
    platform_filter = ""
    params = {"start_date": start_date, "end_date": end_date, "conversion_type": conversion_type}
    if platform and platform.lower() != "all":
        platform_filter = "AND platform = :platform"
        params["platform"] = platform

    rows = query(
        db_path,
        f"""
        SELECT platform, date, account_id, campaign_id, adset_id, ad_id, conversion_type, reported_value
        FROM reported_value
        WHERE date BETWEEN :start_date AND :end_date
          AND conversion_type = :conversion_type
        {platform_filter}
        """,
        params,
    ).rows

    agg: dict[tuple[str, ...], dict[str, object]] = {}
    for r in rows:
        p = str(r.get("platform") or "")
        d = str(r.get("date") or "")
        account_id = str(r.get("account_id") or "")
        campaign_id = str(r.get("campaign_id") or "")
        adset_id = str(r.get("adset_id") or "")
        ad_id = str(r.get("ad_id") or "")
        reported_value = to_float(r.get("reported_value"))

        if breakdown == "day":
            key = (d,)
            name = d
        elif breakdown == "traffic_source":
            utm_source, utm_medium = _traffic_source_for_platform(p)
            key = (utm_source, utm_medium)
            name = f"{utm_source} / {utm_medium}"
        elif breakdown == "ad_account":
            key = (p, account_id)
            name = account_id or f"{p}_unknown_account"
        elif breakdown == "campaign":
            key = (p, account_id, campaign_id)
            name = campaign_id
        elif breakdown == "ad_set":
            key = (p, account_id, campaign_id, adset_id)
            name = adset_id
        elif breakdown == "ad":
            key = (p, account_id, campaign_id, adset_id, ad_id)
            name = ad_id
        else:
            raise ValueError("breakdown must be one of: day, traffic_source, ad_account, campaign, ad_set, ad")

        if key not in agg:
            agg[key] = {
                "name": name,
                "platform": p,
                "account_id": account_id,
                "campaign_id": campaign_id,
                "adset_id": adset_id,
                "ad_id": ad_id,
                "conversion_type": conversion_type,
                "reported_value": 0.0,
            }
        agg[key]["reported_value"] = float(agg[key]["reported_value"]) + reported_value

    out_rows = list(agg.values())
    for row in out_rows:
        row["reported_value"] = float(f"{float(row['reported_value']):.2f}")
    return {"rows": out_rows, "breakdown": breakdown, "start_date": start_date, "end_date": end_date}

