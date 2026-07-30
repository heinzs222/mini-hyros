from __future__ import annotations

import json
import os

from attributionops.db import query
from attributionops.tools.campaign_filter import excluded_campaign_keys, is_excluded
from attributionops.util import parse_json, to_float, to_int


def report_currency() -> str:
    """Currency every spend total is reported in (mirrors the backend's
    REPORT_CURRENCY handling)."""
    return str(os.environ.get("REPORT_CURRENCY", "CAD") or "").strip().upper() or "CAD"


def spend_fx_rates() -> dict[str, float]:
    """Parse SPEND_FX_RATES into a {source_currency: rate} map.

    Format: ``"USD:1.40,EUR:1.55"`` — each rate is how many REPORT_CURRENCY
    units one source-currency unit is worth. Malformed or non-positive entries
    are skipped so a typo can never zero out spend.
    """
    rates: dict[str, float] = {}
    for part in str(os.environ.get("SPEND_FX_RATES", "") or "").split(","):
        code, sep, value = part.partition(":")
        code = code.strip().upper()
        if not sep or not code:
            continue
        try:
            rate = float(value.strip())
        except ValueError:
            continue
        if rate > 0:
            rates[code] = rate
    return rates


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

    # Older warehouses predate the spend.currency column (added by the spend
    # sync's schema evolution); select it defensively so reads never crash on a
    # not-yet-migrated database.
    spend_columns = {
        str(r.get("name") or "") for r in query(db_path, "PRAGMA table_info(spend);").rows
    }
    currency_select = "currency" if "currency" in spend_columns else "'' AS currency"

    rows = query(
        db_path,
        f"""
        SELECT
          platform, date, account_id, campaign_id, adset_id, ad_id, creative_id,
          clicks, cost, impressions, metadata, {currency_select}
        FROM spend
        WHERE date BETWEEN :start_date AND :end_date
        {platform_filter}
        """,
        params,
    ).rows

    # Read-time exclusion: campaigns flagged tracked=0 are dropped before any
    # aggregation, so excluded campaigns disappear from spend totals AND every
    # breakdown this serves (day/platform/campaign/etc.).
    excluded = excluded_campaign_keys(db_path)

    # Read-time FX conversion: spend rows store raw platform-billing-currency
    # cost. Rows whose currency is known and differs from REPORT_CURRENCY are
    # converted with the configured SPEND_FX_RATES rate; currencies without a
    # configured rate are left unconverted and surfaced via spend_by_currency.
    report_ccy = report_currency()
    fx_rates = spend_fx_rates()
    spend_by_currency: dict[str, dict[str, object]] = {}

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

        if is_excluded(excluded, p, campaign_id):
            continue

        if breakdown == "day":
            key = (d,)
            name = d
        elif breakdown == "platform":
            key = (p,)
            name = p or "unknown"
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
            raise ValueError("breakdown must be one of: day, platform, traffic_source, ad_account, campaign, ad_set, ad")

        clicks = to_int(r.get("clicks"))
        cost = to_float(r.get("cost"))
        impressions = to_int(r.get("impressions"))

        currency = str(r.get("currency") or "").strip().upper()
        rate = fx_rates.get(currency) if currency and currency != report_ccy else None
        bucket = spend_by_currency.setdefault(
            currency, {"cost": 0.0, "converted": rate is not None, "rate": rate}
        )
        bucket["cost"] = float(bucket["cost"]) + cost
        if rate is not None:
            cost *= rate

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

    # Platform APIs sometimes return inactive entities whose aggregate is exactly
    # zero for every metric. They add no information and previously produced rows
    # full of zeros. Attribution-only entities are still added later by report.py,
    # so filtering these spend-only ghosts cannot hide real sales.
    out_rows = [
        row
        for row in agg.values()
        if to_int(row.get("clicks")) != 0
        or abs(to_float(row.get("cost"))) > 0.000001
        or to_int(row.get("impressions")) != 0
    ]
    for row in out_rows:
        row["cost"] = float(f"{float(row['cost']):.2f}")
        row["metadata"] = json.dumps(row["metadata"], separators=(",", ":"))

    # Raw (pre-conversion) cost per stored currency, plus whether/at what rate
    # each was converted into REPORT_CURRENCY — additive so sync/report layers
    # can expose exactly what happened, including currencies missing a rate.
    for bucket in spend_by_currency.values():
        bucket["cost"] = float(f"{float(bucket['cost']):.2f}")

    return {
        "rows": out_rows,
        "breakdown": breakdown,
        "start_date": start_date,
        "end_date": end_date,
        "report_currency": report_ccy,
        "spend_by_currency": spend_by_currency,
    }


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

    # Same read-time exclusion as ads_get_spend so excluded campaigns drop out of
    # every reported-value breakdown too.
    excluded = excluded_campaign_keys(db_path)

    agg: dict[tuple[str, ...], dict[str, object]] = {}
    for r in rows:
        p = str(r.get("platform") or "")
        d = str(r.get("date") or "")
        account_id = str(r.get("account_id") or "")
        campaign_id = str(r.get("campaign_id") or "")
        adset_id = str(r.get("adset_id") or "")
        ad_id = str(r.get("ad_id") or "")
        reported_value = to_float(r.get("reported_value"))

        if is_excluded(excluded, p, campaign_id):
            continue

        if breakdown == "day":
            key = (d,)
            name = d
        elif breakdown == "platform":
            key = (p,)
            name = p or "unknown"
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
            raise ValueError("breakdown must be one of: day, platform, traffic_source, ad_account, campaign, ad_set, ad")

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
