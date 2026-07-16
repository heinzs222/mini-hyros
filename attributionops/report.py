from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Literal

from attributionops.db import query
from attributionops.refund_ledger import apply_refunds_as_of
from attributionops.tools.ads import ads_get_reported_value, ads_get_spend
from attributionops.tools.attribution import attribution_run_and_day_totals
from attributionops.tools.campaign_filter import ensure_campaign_settings_table, not_excluded_sql
from attributionops.tools.integrations import integrations_status
from attributionops.tools.tracking import tracking_health_check
from attributionops.report_integrity import (
    build_dimension_coverage,
    resolve_attribution_dimensions,
    visible_report_columns,
)
from attributionops.util import (
    iso_ts,
    local_day_bounds_utc,
    normalize_campaign_key,
    report_timezone,
    round_money,
    safe_div,
    to_float,
    to_int,
    try_parse_iso_ts,
)

logger = logging.getLogger(__name__)


def _ts_end_exclusive(end_date: str) -> str:
    """Exclusive upper bound for sargable ISO-timestamp range filters.

    ``ts`` columns store ISO-8601 strings (e.g. ``2026-06-17T23:40:00Z``), which
    sort lexicographically by date, so ``ts >= :start AND ts < :end_excl`` is an
    index-usable replacement for the non-sargable ``date(substr(ts,1,10))
    BETWEEN :start AND :end`` (which forces a full scan + per-row function call).
    """
    return (date.fromisoformat(end_date) + timedelta(days=1)).isoformat()


TabKey = Literal["traffic_source", "ad_account", "campaign", "ad_set", "ad"]


@dataclass(frozen=True)
class ReportInputs:
    report_name: str
    start_date: str
    end_date: str
    preset: str | None
    currency: str
    attribution_model: str
    lookback_days: int
    conversion_type: str
    use_date_of_click_attribution: bool
    active_tab: TabKey


def _tabs() -> list[dict[str, str]]:
    return [
        {"key": "traffic_source", "label": "Traffic Source"},
        {"key": "ad_account", "label": "Ad Account"},
        {"key": "campaign", "label": "Campaign"},
        {"key": "ad_set", "label": "Ad Set"},
        {"key": "ad", "label": "Ad"},
    ]


def _tab_to_breakdown(tab: TabKey) -> str:
    return {
        "traffic_source": "traffic_source",
        "ad_account": "ad_account",
        "campaign": "campaign",
        "ad_set": "ad_set",
        "ad": "ad",
    }[tab]


def _format_model_label(model: str) -> str:
    m = model.replace("-", "_").replace(" ", "_").strip().lower()
    mapping = {
        "last_click": "Last Click",
        "first_click": "First Click",
        "linear": "Linear",
        "time_decay": "Time-Decay",
        "data_driven_proxy": "Data-Driven Proxy",
    }
    return mapping.get(m, model)


def _format_metrics(
    *,
    clicks: int,
    cost: float,
    total_revenue: float,
    revenue: float,
    cogs: float,
    fees: float,
    orders: float,
    reported: float | None,
    fixed_cost_allocation: float | None,
    impressions: int = 0,
) -> dict[str, Any]:
    clicks_i = int(clicks)
    impressions_i = int(impressions)
    orders_f = float(orders)
    profit = revenue - cost - cogs - fees
    net_profit = profit if fixed_cost_allocation is None else profit - fixed_cost_allocation
    reported_delta = None if reported is None else (revenue - reported)
    cpc = safe_div(cost, clicks_i)
    cpa = safe_div(cost, orders_f)
    roas = safe_div(revenue, cost)
    cvr = safe_div(orders_f, clicks_i)
    aov = safe_div(revenue, orders_f)
    rpc = safe_div(revenue, clicks_i)
    margin_pct = safe_div(profit, revenue)
    cpm = safe_div(cost * 1000.0, impressions_i)
    ctr = safe_div(clicks_i, impressions_i)
    return {
        "clicks": clicks_i,
        "impressions": impressions_i,
        "orders": round(orders_f, 2),
        "cost": round_money(cost),
        "cpc": round_money(cpc) if cpc is not None else None,
        "cpm": round_money(cpm) if cpm is not None else None,
        "ctr": round(ctr * 100.0, 2) if ctr is not None else None,
        "cpa": round_money(cpa) if cpa is not None else None,
        "total_revenue": round_money(total_revenue),
        "revenue": round_money(revenue),
        "aov": round_money(aov) if aov is not None else None,
        "rpc": round_money(rpc) if rpc is not None else None,
        "roas": round(roas, 2) if roas is not None else None,
        "cvr": round(cvr * 100.0, 2) if cvr is not None else None,
        "margin_pct": round(margin_pct * 100.0, 2) if margin_pct is not None else None,
        "profit": round_money(profit),
        "net_profit": round_money(net_profit),
        "reported": round_money(reported) if reported is not None else None,
        "reported_delta": round_money(reported_delta) if reported_delta is not None else None,
    }


def _traffic_source_for_platform(platform: str) -> str:
    p = (platform or "").lower()
    if p == "meta":
        return "facebook / paid_social"
    if p == "google":
        return "google / cpc"
    if p == "tiktok":
        return "tiktok / paid_social"
    return "unknown / paid"


def _platform_label(platform: str) -> str:
    p = (platform or "").lower()
    if p == "meta":
        return "Facebook Ads"
    if p == "google":
        return "Google Ads"
    if p == "tiktok":
        return "TikTok Ads"
    return "Unknown"


def _normalize_landing_path(raw_url: str) -> str:
    raw = str(raw_url or "").strip()
    if not raw:
        return "/"

    # Strip scheme/domain if full URL is stored.
    if "://" in raw:
        after = raw.split("://", 1)[1]
        if "/" in after:
            raw = after.split("/", 1)[1]
            raw = "/" + raw
        else:
            raw = "/"

    raw = raw.split("?", 1)[0].split("#", 1)[0].strip()
    if not raw:
        return "/"
    if not raw.startswith("/"):
        raw = "/" + raw
    return raw


def _funnel_key_from_path(path: str) -> str:
    clean = _normalize_landing_path(path)
    parts = [p for p in clean.strip("/").split("/") if p]
    if not parts:
        return "/"
    return "/" + parts[0]


def _humanize_funnel_name(funnel_key: str) -> str:
    if funnel_key == "/":
        return "Homepage"
    token = funnel_key.strip("/").replace("-", " ").replace("_", " ").strip()
    return " ".join(x.capitalize() for x in token.split()) if token else funnel_key


def _funnel_name_map(db_path: str) -> dict[str, str]:
    try:
        rows = query(
            db_path,
            """
            SELECT entity_id, name
            FROM ad_names
            WHERE entity_type = 'funnel';
            """,
        ).rows
    except Exception:
        return {}
    out: dict[str, str] = {}
    for r in rows:
        entity_id = str(r.get("entity_id") or "").strip()
        name = str(r.get("name") or "").strip()
        if entity_id and name:
            out[entity_id] = name
    return out


def _build_platform_comparison(
    db_path: str,
    *,
    start_date: str,
    end_date: str,
    attrib_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    start_utc, end_excl_utc = local_day_bounds_utc(start_date, end_date)
    spend_rows = query(
        db_path,
        f"""
        SELECT
          platform,
          SUM(CAST(clicks AS REAL)) AS clicks,
          SUM(CAST(cost AS REAL)) AS cost
        FROM spend
        WHERE date BETWEEN :start_date AND :end_date
          AND {not_excluded_sql("spend.platform", "spend.campaign_id")}
        GROUP BY platform;
        """,
        {"start_date": start_date, "end_date": end_date},
    ).rows

    touch_rows = query(
        db_path,
        f"""
        SELECT
          platform,
          COUNT(DISTINCT session_id) AS touch_clicks
        FROM touchpoints
        WHERE ts >= :start_utc AND ts < :end_excl_utc
          AND COALESCE(platform, '') <> ''
          AND {not_excluded_sql("touchpoints.platform", "touchpoints.campaign_id")}
        GROUP BY platform;
        """,
        {"start_utc": start_utc, "end_excl_utc": end_excl_utc},
    ).rows

    by_platform: dict[str, dict[str, float]] = defaultdict(
        lambda: {
            "clicks": 0.0,
            "touch_clicks": 0.0,
            "cost": 0.0,
            "orders": 0.0,
            "revenue": 0.0,
            "total_revenue": 0.0,
            "cogs": 0.0,
            "fees": 0.0,
        }
    )

    for r in spend_rows:
        p = str(r.get("platform") or "").lower().strip()
        if not p:
            continue
        by_platform[p]["clicks"] += to_float(r.get("clicks"))
        by_platform[p]["cost"] += to_float(r.get("cost"))

    for r in touch_rows:
        p = str(r.get("platform") or "").lower().strip()
        if not p:
            continue
        by_platform[p]["touch_clicks"] += to_float(r.get("touch_clicks"))

    for r in attrib_rows:
        p = str(r.get("platform") or "").lower().strip()
        if not p:
            continue
        by_platform[p]["orders"] += float(str(r.get("orders") or "0") or 0)
        by_platform[p]["revenue"] += to_float(r.get("revenue"))
        by_platform[p]["total_revenue"] += to_float(r.get("total_revenue"))
        by_platform[p]["cogs"] += to_float(r.get("cogs"))
        by_platform[p]["fees"] += to_float(r.get("fees"))

    rows: list[dict[str, Any]] = []
    for platform, v in by_platform.items():
        clicks = int(v["clicks"]) if v["clicks"] > 0 else int(v["touch_clicks"])
        cost = float(v["cost"])
        orders = float(v["orders"])
        revenue = float(v["revenue"])
        total_revenue = float(v["total_revenue"])
        cogs = float(v["cogs"])
        fees = float(v["fees"])
        profit = revenue - cost - cogs - fees

        roas = safe_div(revenue, cost)
        cvr = safe_div(orders, clicks)
        cpa = safe_div(cost, orders)
        cpc = safe_div(cost, clicks)
        aov = safe_div(revenue, orders)

        rows.append(
            {
                "platform": platform,
                "label": _platform_label(platform),
                "clicks": clicks,
                "orders": round(orders, 2),
                "cost": round_money(cost),
                "revenue": round_money(revenue),
                "total_revenue": round_money(total_revenue),
                "profit": round_money(profit),
                "roas": round(roas, 2) if roas is not None else None,
                "cvr": round(cvr * 100.0, 2) if cvr is not None else None,
                "cpa": round_money(cpa) if cpa is not None else None,
                "cpc": round_money(cpc) if cpc is not None else None,
                "aov": round_money(aov) if aov is not None else None,
            }
        )

    total_clicks = sum(int(r["clicks"]) for r in rows)
    total_revenue = sum(float(r["revenue"] or 0.0) for r in rows)
    for r in rows:
        clicks_share = safe_div(float(r["clicks"]), float(total_clicks))
        revenue_share = safe_div(float(r["revenue"]), float(total_revenue))
        r["clicks_share"] = round((clicks_share or 0.0) * 100.0, 2)
        r["revenue_share"] = round((revenue_share or 0.0) * 100.0, 2)

    rows.sort(key=lambda r: (r.get("revenue") or 0.0), reverse=True)
    return rows


def _build_funnel_snapshot(db_path: str, *, start_date: str, end_date: str) -> list[dict[str, Any]]:
    start_utc, end_excl_utc = local_day_bounds_utc(start_date, end_date)
    conversions = query(
        db_path,
        """
        SELECT customer_key, type, value
        FROM conversions
        WHERE ts >= :start_utc AND ts < :end_excl_utc
          AND COALESCE(customer_key, '') <> '';
        """,
        {"start_utc": start_utc, "end_excl_utc": end_excl_utc},
    ).rows

    funnel_name_map = _funnel_name_map(db_path)

    # Visits per funnel = distinct sessions whose landing page maps to that
    # funnel. A session has exactly one landing page, so grouping by landing_page
    # in SQL (a handful of distinct values) and summing the distinct-session
    # counts per funnel is equivalent to building a per-funnel set of every
    # session_id in Python — but avoids materializing the full sessions table.
    landing_visit_rows = query(
        db_path,
        """
        SELECT landing_page, COUNT(DISTINCT session_id) AS visits
        FROM sessions
        WHERE ts >= :start_utc AND ts < :end_excl_utc
        GROUP BY landing_page;
        """,
        {"start_utc": start_utc, "end_excl_utc": end_excl_utc},
    ).rows

    visits_by_funnel: dict[str, int] = defaultdict(int)
    for r in landing_visit_rows:
        funnel_id = _funnel_key_from_path(str(r.get("landing_page") or "/"))
        visits_by_funnel[funnel_id] += to_int(r.get("visits"))

    # Each customer's first funnel = the funnel of their earliest in-range
    # session. Picking the earliest row per customer in SQL returns one row per
    # customer instead of every session. (ts, rowid) ordering matches the
    # original "first by timestamp, earliest-inserted on ties" behaviour.
    first_funnel_rows = query(
        db_path,
        """
        SELECT customer_key, landing_page FROM (
            SELECT customer_key, landing_page,
                   ROW_NUMBER() OVER (PARTITION BY customer_key ORDER BY ts, rowid) AS rn
            FROM sessions
            WHERE ts >= :start_utc AND ts < :end_excl_utc
              AND COALESCE(customer_key, '') <> ''
        ) WHERE rn = 1;
        """,
        {"start_utc": start_utc, "end_excl_utc": end_excl_utc},
    ).rows

    first_funnel_by_customer: dict[str, str] = {}
    for r in first_funnel_rows:
        customer_key = str(r.get("customer_key") or "")
        if customer_key:
            first_funnel_by_customer[customer_key] = _funnel_key_from_path(str(r.get("landing_page") or "/"))

    lead_customers: dict[str, set[str]] = defaultdict(set)
    purchase_customers: dict[str, set[str]] = defaultdict(set)
    revenue_by_funnel: dict[str, float] = defaultdict(float)

    for c in conversions:
        customer_key = str(c.get("customer_key") or "")
        if not customer_key:
            continue
        funnel_id = first_funnel_by_customer.get(customer_key)
        if funnel_id is None:
            continue
        conv_type = str(c.get("type") or "").lower()
        value = to_float(c.get("value"))

        if conv_type in ("lead", "formsubmission", "form_submission"):
            lead_customers[funnel_id].add(customer_key)
        if conv_type in ("purchase", "payment"):
            purchase_customers[funnel_id].add(customer_key)
            revenue_by_funnel[funnel_id] += value

    rows: list[dict[str, Any]] = []
    all_funnels = set(visits_by_funnel.keys()) | set(lead_customers.keys()) | set(purchase_customers.keys())
    for funnel_id in all_funnels:
        visits = visits_by_funnel.get(funnel_id, 0)
        leads = len(lead_customers.get(funnel_id, set()))
        purchases = len(purchase_customers.get(funnel_id, set()))
        revenue = float(revenue_by_funnel.get(funnel_id, 0.0))

        lead_rate = safe_div(float(leads), float(visits))
        purchase_rate = safe_div(float(purchases), float(visits))
        aov = safe_div(revenue, float(purchases))

        rows.append(
            {
                "funnel_id": funnel_id,
                "funnel_name": funnel_name_map.get(funnel_id) or _humanize_funnel_name(funnel_id),
                "visits": visits,
                "leads": leads,
                "purchases": purchases,
                "lead_rate": round((lead_rate or 0.0) * 100.0, 2),
                "purchase_rate": round((purchase_rate or 0.0) * 100.0, 2),
                "revenue": round_money(revenue),
                "aov": round_money(aov) if aov is not None else None,
            }
        )

    rows.sort(key=lambda r: int(r.get("visits") or 0), reverse=True)
    return rows[:12]


def _norm_account_seg(value: Any) -> str:
    """Canonicalize the account_id segment of a join key (lowercase/strip)."""
    return str(value or "").strip().lower()


def _entity_key(
    breakdown: str,
    *,
    platform: Any,
    account_id: Any,
    campaign_id: Any,
    adset_id: Any,
    ad_id: Any,
) -> str:
    """Build the normalized spend<->attribution JOIN key for a dimension row.

    campaign/adset/ad segments run through ``normalize_campaign_key`` and the
    account segment is lowercased so that a spend row keyed by numeric/braced ids
    and a touchpoint keyed by a UTM slug or display name collapse to the SAME
    token — the two sides must produce identical strings for the same campaign.
    The human-readable display name is carried separately by the caller.
    """
    platform = str(platform or "")
    account = _norm_account_seg(account_id)
    campaign = normalize_campaign_key(campaign_id)
    adset = normalize_campaign_key(adset_id)
    ad = normalize_campaign_key(ad_id)
    if breakdown == "ad_account":
        return f"{platform}|{account}"
    if breakdown == "campaign":
        return f"{platform}|{account}|{campaign}"
    if breakdown == "ad_set":
        return f"{platform}|{account}|{campaign}|{adset}"
    if breakdown == "ad":
        return f"{platform}|{account}|{campaign}|{adset}|{ad}"
    raise ValueError("invalid breakdown")


def _key_from_spend_row(row: dict[str, Any], breakdown: str) -> str:
    if breakdown == "traffic_source":
        return str(row.get("name") or "")
    if breakdown == "day":
        return str(row.get("name") or "")
    return _entity_key(
        breakdown,
        platform=row.get("platform", ""),
        account_id=row.get("account_id", ""),
        campaign_id=row.get("campaign_id", ""),
        adset_id=row.get("adset_id", ""),
        ad_id=row.get("ad_id", ""),
    )


def _key_from_attrib_row(row: dict[str, Any], breakdown: str) -> str:
    if breakdown == "traffic_source":
        channel = str(row.get("channel") or "")
        platform = str(row.get("platform") or "")
        if channel == "email":
            return "newsletter / email"
        if channel == "organic":
            return "organic / organic"
        if platform:
            return _traffic_source_for_platform(platform)
        return "unknown / unknown"
    return _entity_key(
        breakdown,
        platform=row.get("platform", ""),
        account_id=row.get("account_id", ""),
        campaign_id=row.get("campaign_id", ""),
        adset_id=row.get("adset_id", ""),
        ad_id=row.get("ad_id", ""),
    )


def _default_row_name_for_breakdown(row: dict[str, Any], breakdown: str, fallback: str) -> str:
    if breakdown == "ad_account":
        account_id = str(row.get("account_id") or "").strip()
        platform = str(row.get("platform") or "").strip()
        if account_id:
            return account_id
        if platform:
            return f"{platform} account"
        return fallback
    if breakdown == "campaign":
        return str(row.get("campaign_id") or "").strip() or fallback
    if breakdown == "ad_set":
        return str(row.get("adset_id") or "").strip() or fallback
    if breakdown == "ad":
        return str(row.get("ad_id") or "").strip() or fallback
    return fallback


def _breakdown_key_has_value(key: str, breakdown: str) -> bool:
    parts = key.split("|") if key else []
    if breakdown == "ad_account":
        return len(parts) >= 2 and bool(parts[0].strip() or parts[1].strip())
    if breakdown == "campaign":
        return len(parts) >= 3 and bool(parts[2].strip())
    if breakdown == "ad_set":
        return len(parts) >= 4 and bool(parts[3].strip())
    if breakdown == "ad":
        return len(parts) >= 5 and bool(parts[4].strip())
    return True


def _account_spend_totals(db_path: str, *, start_date: str, end_date: str) -> dict[str, float]:
    """Account-level ad spend totals for headline widgets.

    Breakdown tabs intentionally hide rows that do not have the selected
    dimension populated. Hyros' account cards still include that spend, so keep
    the dashboard summary independent from the selected table grouping.
    """

    rows = ads_get_spend(
        db_path,
        platform="all",
        start_date=start_date,
        end_date=end_date,
        breakdown="platform",
    )["rows"]

    return {
        "clicks": float(sum(to_int(r.get("clicks")) for r in rows)),
        "impressions": float(sum(to_int(r.get("impressions")) for r in rows)),
        "cost": float(sum(to_float(r.get("cost")) for r in rows)),
    }


def _child_count_map(db_path: str, *, start_date: str, end_date: str, active_tab: TabKey) -> dict[str, int]:
    if active_tab == "ad":
        return {}

    start_utc, end_excl_utc = local_day_bounds_utc(start_date, end_date)

    # Only the DISTINCT entity tuples matter for counting children, so let SQLite
    # do the dedup: this materializes a few hundred distinct rows instead of
    # pulling every spend + touchpoint row (~19k) into Python. Excluded campaigns
    # are dropped so they neither appear nor inflate parent child-counts.
    spend_raw = query(
        db_path,
        f"""
        SELECT DISTINCT platform, account_id, campaign_id, adset_id, ad_id
        FROM spend
        WHERE date BETWEEN :start_date AND :end_date
          AND {not_excluded_sql("spend.platform", "spend.campaign_id")};
        """,
        {"start_date": start_date, "end_date": end_date},
    ).rows

    touch_raw = query(
        db_path,
        f"""
        SELECT DISTINCT
          platform,
          '' AS account_id,
          campaign_id,
          adset_id,
          ad_id
        FROM touchpoints
        WHERE ts >= :start_utc AND ts < :end_excl_utc
          AND {not_excluded_sql("touchpoints.platform", "touchpoints.campaign_id")};
        """,
        {"start_utc": start_utc, "end_excl_utc": end_excl_utc},
    ).rows

    raw_rows = list(spend_raw) + list(touch_raw)

    children: dict[str, set[str]] = defaultdict(set)

    for r in raw_rows:
        # Normalize the join segments identically to the by_key builders so the
        # parent keys here line up with the table rows' ids.
        platform = str(r.get("platform") or "")
        account = _norm_account_seg(r.get("account_id"))
        campaign = normalize_campaign_key(r.get("campaign_id"))
        adset = normalize_campaign_key(r.get("adset_id"))
        ad = normalize_campaign_key(r.get("ad_id"))

        if active_tab == "traffic_source":
            parent = _traffic_source_for_platform(platform)
            child = f"{platform}|{account}"
            children[parent].add(child)
        elif active_tab == "ad_account":
            parent = f"{platform}|{account}"
            child = f"{platform}|{account}|{campaign}"
            children[parent].add(child)
        elif active_tab == "campaign":
            parent = f"{platform}|{account}|{campaign}"
            child = f"{platform}|{account}|{campaign}|{adset}"
            children[parent].add(child)
        elif active_tab == "ad_set":
            parent = f"{platform}|{account}|{campaign}|{adset}"
            child = f"{platform}|{account}|{campaign}|{adset}|{ad}"
            children[parent].add(child)

    return {k: len(v) for k, v in children.items()}


_ORDER_IDENTITY_SQL = """
CASE
  WHEN COALESCE(customer_key, '') != '' THEN 'customer:' || customer_key
  WHEN COALESCE(processor_customer_id, '') != '' THEN 'processor:' || processor_customer_id
  WHEN COALESCE(payment_fingerprint, '') != '' THEN 'fingerprint:' || payment_fingerprint
  ELSE 'sale:' || COALESCE(NULLIF(sale_group_id, ''), order_id)
END
"""


def _order_group_id(row: dict[str, Any]) -> str:
    return str(row.get("sale_group_id") or row.get("order_id") or "")


def _order_identity(row: dict[str, Any]) -> str:
    if row.get("customer_key"):
        return f"customer:{row['customer_key']}"
    if row.get("processor_customer_id"):
        return f"processor:{row['processor_customer_id']}"
    if row.get("payment_fingerprint"):
        return f"fingerprint:{row['payment_fingerprint']}"
    return f"sale:{_order_group_id(row)}"


def build_hyros_like_report(db_path: str, inputs: ReportInputs) -> dict[str, Any]:
    # Ensure the read-time exclusion table exists before any raw SQL references it
    # (memoized). Compute the timezone-aware, sargable date bounds once and reuse
    # them for every ts-range filter below (ts >= :start_utc AND ts < :end_excl_utc).
    ensure_campaign_settings_table(db_path)
    start_utc, end_excl_utc = local_day_bounds_utc(inputs.start_date, inputs.end_date)

    # Collects non-fatal data errors (a failed sub-query that we degrade past)
    # so the response can tell the UI "a query failed" instead of silently
    # rendering an all-zero report that looks identical to "no data".
    data_errors: list[dict[str, str]] = []

    # 1) integrations.status + tracking.health_check
    integrations = integrations_status(db_path)
    # Scope the health check to the SELECTED report window. Called unscoped it
    # scans full lifetime sessions/orders/touchpoints/conversions on every build,
    # which dominated report latency and made every date-range switch equally slow
    # regardless of how much data the window actually contains.
    health = tracking_health_check(
        db_path,
        start_date=inputs.start_date,
        end_date=inputs.end_date,
        lookback_days_for_order_source=inputs.lookback_days,
    )

    cov = health.get("coverage", {})
    orders_with_source = int(cov.get("orders_with_source", 0))
    orders_total = int(cov.get("orders_total", 0))
    sessions_with_click_id = int(cov.get("sessions_with_click_id", 0))
    sessions_total = int(cov.get("sessions_total", 0))

    order_rate = (orders_with_source / orders_total) if orders_total else 0.0
    click_id_rate = (sessions_with_click_id / sessions_total) if sessions_total else 0.0
    tracking_percentage = round(100.0 * (0.5 * order_rate + 0.5 * click_id_rate), 2)

    tracking = {
        "tracking_percentage": tracking_percentage,
        "how_computed": "50% orders_with_source/orders_total + 50% sessions_with_click_id/sessions_total, based on local warehouse tables.",
        "coverage_breakdown": {
            "orders_with_source": orders_with_source,
            "orders_total": orders_total,
            "sessions_with_click_id": sessions_with_click_id,
            "sessions_total": sessions_total,
        },
        "top_tracking_gaps": health.get("top_tracking_gaps", []),
    }

    breakdown = _tab_to_breakdown(inputs.active_tab)
    date_basis = "click" if inputs.use_date_of_click_attribution else "conversion"

    # 2) spend
    spend_rows = ads_get_spend(
        db_path,
        platform="all",
        start_date=inputs.start_date,
        end_date=inputs.end_date,
        breakdown=breakdown,
    )["rows"]

    # 3) attribution — compute the dimension breakdown AND the per-day series in
    # a single pass over orders/touchpoints (previously two full passes).
    _attr = attribution_run_and_day_totals(
        db_path,
        model=inputs.attribution_model,
        start_date=inputs.start_date,
        end_date=inputs.end_date,
        lookback_days=inputs.lookback_days,
        conversion_type=inputs.conversion_type,
        value_type="revenue",
        date_basis=date_basis,
    )
    attrib_rows = resolve_attribution_dimensions(
        spend_rows,
        list(_attr["run"]["rows"]),
        active_tab=inputs.active_tab,
    )
    dimension_coverage = build_dimension_coverage(
        attrib_rows,
        active_tab=inputs.active_tab,
    )
    attrib_by_day = _attr["day_totals"]["rows"]

    # 4) reported
    reported_rows = ads_get_reported_value(
        db_path,
        platform="all",
        start_date=inputs.start_date,
        end_date=inputs.end_date,
        breakdown=breakdown,
        conversion_type=inputs.conversion_type,
    )["rows"]

    by_key: dict[str, dict[str, Any]] = {}

    # spend base
    for r in spend_rows:
        key = _key_from_spend_row(r, breakdown)
        by_key.setdefault(
            key,
            {
                "id": key,
                "name": str(r.get("name") or key),
                "level": inputs.active_tab,
                "clicks": 0,
                "impressions": 0,
                "cost": 0.0,
                "total_revenue": 0.0,
                "revenue": 0.0,
                "cogs": 0.0,
                "fees": 0.0,
                "reported": None,
                "_orders": 0.0,
            },
        )
        by_key[key]["clicks"] += to_int(r.get("clicks"))
        by_key[key]["impressions"] = int(by_key[key].get("impressions") or 0) + to_int(r.get("impressions"))
        by_key[key]["cost"] += to_float(r.get("cost"))

    # attribution adds components
    for r in attrib_rows:
        key = _key_from_attrib_row(r, breakdown)
        by_key.setdefault(
            key,
            {
                "id": key,
                "name": _default_row_name_for_breakdown(r, breakdown, key),
                "level": inputs.active_tab,
                "clicks": 0,
                "cost": 0.0,
                "total_revenue": 0.0,
                "revenue": 0.0,
                "cogs": 0.0,
                "fees": 0.0,
                "reported": None,
                "_orders": 0.0,
            },
        )
        by_key[key]["total_revenue"] += to_float(r.get("total_revenue"))
        by_key[key]["revenue"] += to_float(r.get("revenue"))
        by_key[key]["cogs"] += to_float(r.get("cogs"))
        by_key[key]["fees"] += to_float(r.get("fees"))
        by_key[key]["_orders"] += float(str(r.get("orders") or "0") or 0)

    # reported adds value
    for r in reported_rows:
        key = _key_from_spend_row(r, breakdown)
        by_key.setdefault(
            key,
            {
                "id": key,
                "name": str(r.get("name") or key),
                "level": inputs.active_tab,
                "clicks": 0,
                "cost": 0.0,
                "total_revenue": 0.0,
                "revenue": 0.0,
                "cogs": 0.0,
                "fees": 0.0,
                "reported": 0.0,
                "_orders": 0.0,
            },
        )
        by_key[key]["reported"] = (by_key[key]["reported"] or 0.0) + to_float(r.get("reported_value"))

    has_dimension_values = any(_breakdown_key_has_value(str(k), breakdown) for k in by_key.keys())

    # fallback: if spend/orders are missing, still surface campaign/ad-set/ad entities from touchpoints
    if inputs.active_tab in {"ad_account", "campaign", "ad_set", "ad"} and not has_dimension_values:
        touchpoint_rows = query(
            db_path,
            f"""
            SELECT platform, campaign_id, adset_id, ad_id, COUNT(*) AS touches
            FROM touchpoints
            WHERE ts >= :start_utc AND ts < :end_excl_utc
              AND {not_excluded_sql("touchpoints.platform", "touchpoints.campaign_id")}
            GROUP BY platform, campaign_id, adset_id, ad_id;
            """,
            {"start_utc": start_utc, "end_excl_utc": end_excl_utc},
        ).rows

        for r in touchpoint_rows:
            platform = str(r.get("platform") or "").strip()
            campaign_id = str(r.get("campaign_id") or "").strip()
            adset_id = str(r.get("adset_id") or "").strip()
            ad_id = str(r.get("ad_id") or "").strip()
            # Normalize join segments (keep raw values for the display name).
            campaign_n = normalize_campaign_key(campaign_id)
            adset_n = normalize_campaign_key(adset_id)
            ad_n = normalize_campaign_key(ad_id)

            if inputs.active_tab == "ad_account":
                if not platform:
                    continue
                key = f"{platform}|"
                name = f"{platform} account"
            elif inputs.active_tab == "campaign":
                if not campaign_id:
                    continue
                key = f"{platform}||{campaign_n}"
                name = campaign_id
            elif inputs.active_tab == "ad_set":
                if not adset_id:
                    continue
                key = f"{platform}||{campaign_n}|{adset_n}"
                name = adset_id
            else:  # ad
                if not ad_id:
                    continue
                key = f"{platform}||{campaign_n}|{adset_n}|{ad_n}"
                name = ad_id

            by_key.setdefault(
                key,
                {
                    "id": key,
                    "name": name,
                    "level": inputs.active_tab,
                    "clicks": 0,
                    "cost": 0.0,
                    "total_revenue": 0.0,
                    "revenue": 0.0,
                    "cogs": 0.0,
                    "fees": 0.0,
                    "reported": None,
                    "_orders": 0.0,
                },
            )
            by_key[key]["clicks"] += to_int(r.get("touches"))

    # clicks proxy for non-paid traffic sources (sessions count)
    if inputs.active_tab == "traffic_source":
        session_counts = query(
            db_path,
            """
            SELECT
              CASE
                WHEN utm_medium = 'email' OR utm_source = 'newsletter' THEN 'newsletter / email'
                WHEN utm_medium = 'organic' THEN 'organic / organic'
                WHEN utm_source = 'direct' OR utm_medium = '(none)' THEN 'direct / (none)'
                WHEN utm_medium = 'referral' THEN 'referral / referral'
                ELSE 'other / other'
              END AS traffic_source,
              COUNT(*) AS sessions
            FROM sessions
            WHERE ts >= :start_utc AND ts < :end_excl_utc
            GROUP BY 1;
            """,
            {"start_utc": start_utc, "end_excl_utc": end_excl_utc},
        ).rows
        for r in session_counts:
            key = str(r.get("traffic_source") or "")
            sessions = to_int(r.get("sessions"))
            if not key:
                continue
            by_key.setdefault(
                key,
                {
                    "id": key,
                    "name": key,
                    "level": inputs.active_tab,
                    "clicks": 0,
                    "cost": 0.0,
                    "total_revenue": 0.0,
                    "revenue": 0.0,
                    "cogs": 0.0,
                    "fees": 0.0,
                    "reported": None,
                    "_orders": 0.0,
                },
            )
            # Only apply as proxy when we have no paid clicks.
            if float(by_key[key]["cost"]) == 0.0 and int(by_key[key]["clicks"]) == 0:
                by_key[key]["clicks"] = sessions

    child_counts = _child_count_map(db_path, start_date=inputs.start_date, end_date=inputs.end_date, active_tab=inputs.active_tab)
    has_children = inputs.active_tab != "ad"

    totals = {
        "clicks": 0,
        "cost": 0.0,
        "total_revenue": 0.0,
        "revenue": 0.0,
        "cogs": 0.0,
        "fees": 0.0,
        "reported": 0.0,
        "_reported_any": False,
        "_orders": 0.0,
    }

    table_rows = []
    for key, v in by_key.items():
        if breakdown in {"ad_account", "campaign", "ad_set", "ad"} and not _breakdown_key_has_value(str(key), breakdown):
            continue

        clicks = int(v["clicks"])
        impressions = int(v.get("impressions") or 0)
        cost = float(v["cost"])
        total_revenue = float(v["total_revenue"])
        revenue = float(v["revenue"])
        cogs = float(v["cogs"])
        fees = float(v["fees"])
        orders = float(v["_orders"])
        reported_value = float(v["reported"]) if v["reported"] is not None else None

        metrics = _format_metrics(
            clicks=clicks,
            impressions=impressions,
            cost=cost,
            total_revenue=total_revenue,
            revenue=revenue,
            cogs=cogs,
            fees=fees,
            orders=orders,
            reported=reported_value,
            fixed_cost_allocation=None,
        )

        cc = int(child_counts.get(key, 0)) if has_children else 0
        table_rows.append(
            {
                "id": str(v["id"]),
                "name": str(v["name"]),
                "level": inputs.active_tab,
                "metrics": metrics,
                "children_available": bool(has_children and cc > 0),
                "children_count": int(cc) if has_children else None,
            }
        )

        totals["clicks"] += clicks
        totals["impressions"] = int(totals.get("impressions") or 0) + impressions
        totals["cost"] += cost
        totals["total_revenue"] += total_revenue
        totals["revenue"] += revenue
        totals["cogs"] += cogs
        totals["fees"] += fees
        totals["_orders"] += float(v["_orders"])
        if reported_value is not None:
            totals["reported"] += reported_value
            totals["_reported_any"] = True

    table_rows.sort(key=lambda r: (r["metrics"]["profit"] or 0.0), reverse=True)
    # Ascending-by-profit view, computed once (stable) and reused for losers below.
    table_rows_asc = sorted(table_rows, key=lambda r: (r["metrics"]["profit"] or 0.0))

    # Tab-independent attributed totals: the summary widgets (attributed_revenue,
    # attribution_rate, summary ROAS, the summary card) must NOT change when the
    # user switches breakdown tabs. The per-tab `totals` above only accumulate the
    # rows that carry the active dimension (others are `continue`d), so accumulate
    # the summary figures over ALL attribution rows instead. The per-tab `totals`
    # stay reserved for the visible table's totals row.
    attributed_total_revenue_all = sum(to_float(r.get("total_revenue")) for r in attrib_rows)
    attributed_revenue_all = sum(to_float(r.get("revenue")) for r in attrib_rows)
    attributed_cogs_all = sum(to_float(r.get("cogs")) for r in attrib_rows)
    attributed_fees_all = sum(to_float(r.get("fees")) for r in attrib_rows)
    attributed_orders_all = sum(float(str(r.get("orders") or "0") or 0) for r in attrib_rows)
    reported_all = sum(to_float(r.get("reported_value")) for r in reported_rows)
    reported_any_all = bool(reported_rows)

    totals_metrics = _format_metrics(
        clicks=int(totals["clicks"]),
        impressions=int(totals.get("impressions") or 0),
        cost=float(totals["cost"]),
        total_revenue=float(totals["total_revenue"]),
        revenue=float(totals["revenue"]),
        cogs=float(totals["cogs"]),
        fees=float(totals["fees"]),
        orders=float(totals["_orders"]),
        reported=float(totals["reported"]) if totals["_reported_any"] else None,
        fixed_cost_allocation=None,
    )

    account_spend = _account_spend_totals(
        db_path,
        start_date=inputs.start_date,
        end_date=inputs.end_date,
    )
    account_clicks = int(account_spend["clicks"])
    account_impressions = int(account_spend["impressions"])
    account_cost = float(account_spend["cost"])

    summary_totals_metrics = _format_metrics(
        clicks=account_clicks,
        impressions=account_impressions,
        cost=account_cost,
        total_revenue=attributed_total_revenue_all,
        revenue=attributed_revenue_all,
        cogs=attributed_cogs_all,
        fees=attributed_fees_all,
        orders=attributed_orders_all,
        reported=reported_all if reported_any_all else None,
        fixed_cost_allocation=None,
    )

    total_cost = account_cost
    roas = safe_div(attributed_revenue_all, total_cost)
    conversions_count = attributed_orders_all
    cpa = safe_div(total_cost, conversions_count) if conversions_count else None

    # Raw sales and customer metrics are deliberately separate. Hyros counts
    # every processor order as a Sale, groups same-buyer charges inside a short
    # window for Customer/AOV measures, and excludes recurring groups from the
    # New Customers widget. NET CAC uses genuinely first-ever identities.
    order_rows: list[dict[str, Any]] = []
    customer_order_rows: list[dict[str, Any]] = []
    first_customer_rows: list[dict[str, Any]] = []
    first_by_identity: dict[str, str] = {}
    all_orders_count = 0
    all_orders_gross_revenue = 0.0
    all_orders_revenue = 0.0
    all_orders_cogs = 0.0
    all_orders_fees = 0.0
    total_customers = 0
    new_customers = 0
    recurring_customers = 0
    unique_customers = 0
    net_new_customers = 0
    returning_customers = 0
    # Stage 1 — base order aggregates (cheap, robust). A failure here logs and
    # degrades to zeros, but must NOT be masked as "no orders": it's recorded in
    # data_errors so the UI can distinguish a query error from an empty range.
    try:
        order_rows = query(
            db_path,
            """SELECT order_id, ts, gross, net, refunds, chargebacks, cogs, fees, customer_key,
                      processor,
                      processor_customer_id, payment_fingerprint,
                      COALESCE(NULLIF(sale_group_id, ''), order_id) AS sale_group_id,
                      COALESCE(is_recurring, 0) AS is_recurring
               FROM orders
               WHERE ts >= :start_utc AND ts < :end_excl_utc""",
            {"start_utc": start_utc, "end_excl_utc": end_excl_utc},
        ).rows
        order_rows = apply_refunds_as_of(db_path, order_rows, end_excl_utc)
        all_orders_count = len(order_rows)
        all_orders_gross_revenue = sum(
            to_float(row.get("gross")) or to_float(row.get("net")) for row in order_rows
        )
        all_orders_revenue = sum(to_float(row.get("net")) for row in order_rows)
        all_orders_cogs = sum(to_float(row.get("cogs")) for row in order_rows)
        all_orders_fees = sum(to_float(row.get("fees")) for row in order_rows)

        # Hyros's New Customers widget is sale-based despite its label: every
        # non-refunded sale contributes one customer, split into recurring and
        # non-recurring rows. Any refunded amount excludes that sale from this
        # widget, while the General Overview still includes it in Sales and
        # subtracts the refund from revenue.
        customer_order_rows = [
            row
            for row in order_rows
            if to_float(row.get("refunds")) + to_float(row.get("chargebacks")) <= 0
        ]
        total_customers = len(customer_order_rows)
        # Hyros reports recurrence independently of its non-refunded customer
        # denominator. A refunded recurring charge remains classified recurring,
        # while Total Customers still excludes refunded sales.
        recurring_customers = sum(to_int(row.get("is_recurring")) != 0 for row in order_rows)
        new_customers = max(total_customers - recurring_customers, 0)
    except Exception as exc:
        logger.exception(
            "order base metrics query failed for %s %s..%s",
            db_path, inputs.start_date, inputs.end_date,
        )
        data_errors.append({"component": "orders", "error": str(exc)})
        order_rows = []
        customer_order_rows = []
        all_orders_count = 0
        all_orders_gross_revenue = 0.0
        all_orders_revenue = 0.0
        all_orders_cogs = 0.0
        all_orders_fees = 0.0
        total_customers = 0
        new_customers = 0
        recurring_customers = 0

    # Stage 1b — HYROS-parity sale-group metrics. HYROS's headline widgets count
    # SALE GROUPS (one checkout event), not processor charges, and classify a
    # charge whose (buyer identity, net amount) already occurred in the trailing
    # 30 days as a subscription RENEWAL. Reverse-engineered and validated to the
    # integer against a real HYROS account export (Jul 01–15 window):
    #   New Customers = non-renewal sale groups          (236/236 exact)
    #   Sales         = non-renewal AND non-refunded     (214/214 exact)
    #   AOV           = revenue / ALL sale groups        (0.17% gap)
    all_orders_sale_groups = 0
    hyros_sales_count = 0
    hyros_new_customers = 0
    try:
        def _charge_amount(row: dict[str, Any]) -> float:
            # The renewal key matches on the CHARGED amount (gross of refunds):
            # a refunded prior rebill still marks the next charge as a renewal.
            # Validated: net-based matching over-counts new customers (241 vs
            # HYROS's 236) precisely on refunded-rebill chains.
            return round(to_float(row.get("gross")) or to_float(row.get("net")), 2)

        window_groups: dict[str, dict[str, Any]] = {}
        for row in order_rows:
            gid = _order_group_id(row)
            group = window_groups.setdefault(
                gid,
                {"refunds": 0.0, "ts": "", "identity": _order_identity(row), "amounts": set()},
            )
            group["refunds"] += to_float(row.get("refunds")) + to_float(row.get("chargebacks"))
            group["amounts"].add(_charge_amount(row))
            row_ts = str(row.get("ts") or "")
            if row_ts and (not group["ts"] or row_ts < group["ts"]):
                group["ts"] = row_ts
        all_orders_sale_groups = len(window_groups)

        # Renewal lookback: charges from the 30 days before the window (35-day
        # fetch buffer for safety) plus the window's own earlier charges.
        history_rows: list[dict[str, Any]] = []
        window_start_dt = try_parse_iso_ts(start_utc)
        if window_start_dt is not None and window_groups:
            history_start = iso_ts(window_start_dt - timedelta(days=35))
            history_rows = query(
                db_path,
                """SELECT order_id, ts, gross, net, customer_key, processor,
                          processor_customer_id, payment_fingerprint,
                          COALESCE(NULLIF(sale_group_id, ''), order_id) AS sale_group_id
                   FROM orders
                   WHERE ts >= :history_start AND ts < :start_utc""",
                {"history_start": history_start, "start_utc": start_utc},
            ).rows

        # (identity, charged amount) -> sorted timestamps of every prior CHARGE.
        prior_ts_by_key: dict[tuple[str, float], list[datetime]] = defaultdict(list)
        for row in list(history_rows) + list(order_rows):
            parsed = try_parse_iso_ts(str(row.get("ts") or ""))
            if parsed is None:
                continue
            prior_ts_by_key[(_order_identity(row), _charge_amount(row))].append(parsed)
        for ts_list in prior_ts_by_key.values():
            ts_list.sort()

        renewal_window = timedelta(days=30)
        for group in window_groups.values():
            group_ts = try_parse_iso_ts(group["ts"])
            is_renewal = False
            if group_ts is not None:
                for amount in group["amounts"]:
                    key = (str(group["identity"]), float(amount))
                    for prior in prior_ts_by_key.get(key, ()):
                        if prior >= group_ts:
                            break
                        if group_ts - prior <= renewal_window:
                            is_renewal = True
                            break
                    if is_renewal:
                        break
            if is_renewal:
                continue
            hyros_new_customers += 1
            if float(group["refunds"]) <= 0:
                hyros_sales_count += 1
    except Exception as exc:
        logger.exception(
            "sale-group parity metrics failed for %s %s..%s",
            db_path, inputs.start_date, inputs.end_date,
        )
        data_errors.append({"component": "sale_groups", "error": str(exc)})
        all_orders_sale_groups = 0
        hyros_sales_count = 0
        hyros_new_customers = 0

    # Stage 2 — customer identity / net-new acquisition (needs a first-ever
    # identity scan and is more fragile). Isolated so a failure here can't wipe
    # the base aggregates that already succeeded in stage 1.
    try:
        # Hyros NET CAC uses distinct non-refunded processor customers. Stripe
        # charges without a Stripe customer id are not added to that denominator.
        # Non-Stripe/manual imports fall back to the warehouse identity so their
        # CAC remains useful when no processor customer concept exists.
        acquisition_identities: set[str] = set()
        for row in customer_order_rows:
            processor = str(row.get("processor") or "").strip().lower()
            processor_customer_id = str(row.get("processor_customer_id") or "").strip()
            if processor == "stripe" and processor_customer_id:
                acquisition_identities.add(f"stripe:{processor_customer_id}")
            else:
                # Stripe charges without a Stripe customer id fall back to the
                # warehouse identity (email hash / fingerprint) instead of being
                # dropped from the denominator entirely.
                identity = _order_identity(row)
                if identity:
                    acquisition_identities.add(identity)
        unique_customers = len(acquisition_identities)

        first_customer_rows = query(
            db_path,
            f"""SELECT identity, MIN(ts) AS first_ts
                FROM (SELECT {_ORDER_IDENTITY_SQL} AS identity, ts FROM orders)
                GROUP BY identity""",
        ).rows
        first_by_identity = {
            str(row.get("identity") or ""): str(row.get("first_ts") or "")
            for row in first_customer_rows
        }
        window_identities = {_order_identity(row) for row in order_rows}
        net_new_customers = sum(
            1
            for identity in window_identities
            if start_utc <= first_by_identity.get(identity, "") < end_excl_utc
        )
        returning_customers = max(len(window_identities) - net_new_customers, 0)
    except Exception as exc:
        logger.exception(
            "order identity metrics failed for %s %s..%s",
            db_path, inputs.start_date, inputs.end_date,
        )
        data_errors.append({"component": "orders_identity", "error": str(exc)})
        first_customer_rows = []
        first_by_identity = {}
        unique_customers = 0
        net_new_customers = 0
        returning_customers = 0

    # New vs returning customers + leads — these power true NET CAC and Cost-per-Lead.
    # A "new customer" is one whose first-ever order falls inside the window (not just
    # any order in the window), so CAC reflects net-new acquisition rather than CPA.
    lead_rows: list[dict[str, Any]] = []
    leads_count = 0
    lead_contacts = 0
    new_leads = 0
    lead_optins = 0
    lead_types = "'lead','formsubmission','form_submission','signup','optin','opt_in'"
    # Resolve which identity columns the conversions table actually has ONCE, and
    # build every lead query from only those columns. A warehouse missing
    # session_id/visitor_id then degrades gracefully instead of raising
    # "no such column" and losing lead metrics.
    try:
        lead_cols = {str(r.get("name") or "") for r in query(db_path, "PRAGMA table_info(conversions)").rows}
    except Exception:
        lead_cols = set()
    lead_sid = "session_id" if "session_id" in lead_cols else "'' AS session_id"
    lead_vid = "visitor_id" if "visitor_id" in lead_cols else "'' AS visitor_id"
    _identity_parts = ["NULLIF(customer_key, '')"]
    if "visitor_id" in lead_cols:
        _identity_parts.append("NULLIF(visitor_id, '')")
    if "session_id" in lead_cols:
        _identity_parts.append("NULLIF(session_id, '')")
    _identity_parts.append("conversion_id")
    lead_identity_expr = "COALESCE(" + ", ".join(_identity_parts) + ")"

    # Stage 1 — in-window lead counts.
    try:
        lead_rows = query(
            db_path,
            f"""SELECT conversion_id, ts, LOWER(type) AS type, customer_key, {lead_sid}, {lead_vid}
                FROM conversions
                WHERE ts >= :start_utc AND ts < :end_excl_utc
                  AND LOWER(type) IN ({lead_types})""",
            {"start_utc": start_utc, "end_excl_utc": end_excl_utc},
        ).rows
        leads_count = len(lead_rows)

        def lead_identity(row: dict[str, Any]) -> str:
            return str(
                row.get("customer_key")
                or row.get("visitor_id")
                or row.get("session_id")
                or row.get("conversion_id")
                or ""
            )

        lead_contacts = len({lead_identity(row) for row in lead_rows if lead_identity(row)})
        lead_optins = sum(1 for row in lead_rows if str(row.get("type") or "") != "lead")
    except Exception as exc:
        logger.exception(
            "lead metrics query failed for %s %s..%s",
            db_path, inputs.start_date, inputs.end_date,
        )
        data_errors.append({"component": "leads", "error": str(exc)})
        lead_rows = []
        leads_count = 0
        lead_contacts = 0
        lead_optins = 0

    # Stage 2 — net-new leads (first-ever opt-in falls in window). Isolated so a
    # failure of the first-lead scan doesn't discard the counts from stage 1.
    try:
        first_leads = query(
            db_path,
            f"""SELECT identity, MIN(ts) AS first_ts FROM (
                    SELECT {lead_identity_expr} AS identity, ts
                    FROM conversions WHERE LOWER(type) IN ({lead_types})
                ) GROUP BY identity""",
        ).rows
        new_leads = sum(
            1
            for row in first_leads
            if start_utc <= str(row.get("first_ts") or "") < end_excl_utc
        )
    except Exception as exc:
        logger.exception(
            "net-new lead scan failed for %s %s..%s",
            db_path, inputs.start_date, inputs.end_date,
        )
        data_errors.append({"component": "leads_net_new", "error": str(exc)})
        new_leads = 0

    # HYROS's NET CAC divides spend by customers whose FIRST-EVER purchase falls
    # in the window — validated to the decimal against a HYROS export (cost /
    # CAC = 178 = exactly the first-ever-buyer count). net_new_customers already
    # computes that; the old denominator (unique_customers) included returning
    # buyers and understated CAC.
    net_cac = safe_div(total_cost, float(net_new_customers)) if net_new_customers else None
    # Cost per Lead = ad spend / leads (distinct from CAC and CPA).
    cost_per_lead = safe_div(total_cost, float(leads_count)) if leads_count else None

    total_clicks = account_clicks
    # Tab-independent: use the all-attribution-rows totals so attributed_revenue,
    # attribution_rate and unattributed_* don't shift when switching breakdown tabs.
    attributed_orders = round(attributed_orders_all, 2)
    attributed_revenue = attributed_revenue_all
    attributed_sale_groups = int(_attr["run"].get("sale_groups") or 0)
    attributed_aov = safe_div(attributed_revenue, float(attributed_sale_groups))
    source_attributed_orders = to_int(_attr["run"].get("source_attributed_orders"))
    source_attributed_revenue = to_float(_attr["run"].get("source_attributed_revenue"))
    source_unique_sales = to_int(_attr["run"].get("source_unique_sales"))
    source_aov = safe_div(source_attributed_revenue, float(source_unique_sales))
    unattributed_orders = round(max(float(all_orders_count) - attributed_orders, 0.0), 2)
    unattributed_revenue = round_money(max(all_orders_revenue - attributed_revenue, 0.0))
    attribution_rate = safe_div(attributed_orders, float(all_orders_count))
    # MER = total (Stripe) revenue / total ad spend — always blended
    mer = safe_div(all_orders_revenue, total_cost)
    blended_roas = safe_div(all_orders_revenue, total_cost)
    blended_cvr = safe_div(all_orders_count, total_clicks)
    blended_aov = safe_div(all_orders_revenue, all_orders_count) if all_orders_count else None
    grouped_aov = safe_div(all_orders_revenue, total_customers) if total_customers else None
    # HYROS AOV = revenue over ALL sale groups (incl. refunded and renewals) —
    # NOT the attribution-filtered source_aov, which inflated AOV ~31% by
    # dropping unattributed checkouts from the denominator.
    all_orders_aov = (
        safe_div(all_orders_revenue, float(all_orders_sale_groups))
        if all_orders_sale_groups
        else None
    )
    blended_profit = round_money(all_orders_revenue - total_cost - all_orders_cogs - all_orders_fees)
    blended_cpa = safe_div(total_cost, all_orders_count) if all_orders_count else None

    summary_totals = {
        **summary_totals_metrics,
        "roas": round_money(roas) if roas is not None else None,
        "mer": round_money(mer) if mer is not None else None,
        "cac": round_money(net_cac) if net_cac is not None else None,
        "cpa": round_money(cpa) if cpa is not None else None,
        "cpl": round_money(cost_per_lead) if cost_per_lead is not None else None,
        "new_customers": new_customers,
        "unique_customers": unique_customers,
        "net_new_customers": net_new_customers,
        "returning_customers": returning_customers,
        "recurring_customers": recurring_customers,
        "total_customers": total_customers,
        "leads": leads_count,
        "lead_contacts": lead_contacts,
        "new_leads": new_leads,
        "lead_optins": lead_optins,
        "all_orders_count": all_orders_count,
        "all_orders_revenue": round_money(all_orders_revenue),
        "all_orders_gross_revenue": round_money(all_orders_gross_revenue),
        "all_orders_cogs": round_money(all_orders_cogs),
        "all_orders_fees": round_money(all_orders_fees),
        "tracked_orders": all_orders_count,
        "tracked_revenue": round_money(all_orders_revenue),
        "tracked_gross_revenue": round_money(all_orders_gross_revenue),
        # HYROS-parity sale-group metrics (see Stage 1b): a "sale group" is one
        # checkout event; renewals are 30-day (identity, amount) repeats.
        "tracked_sale_groups": all_orders_sale_groups,
        "hyros_sales_count": hyros_sales_count,
        "hyros_new_customers": hyros_new_customers,
        "all_orders_aov": round_money(all_orders_aov) if all_orders_aov is not None else None,
        "attributed_orders": attributed_orders,
        "attributed_sale_groups": attributed_sale_groups,
        "attributed_revenue": round_money(attributed_revenue),
        "attributed_aov": round_money(attributed_aov) if attributed_aov is not None else None,
        "source_attributed_orders": source_attributed_orders,
        "source_attributed_revenue": round_money(source_attributed_revenue),
        "source_unique_sales": source_unique_sales,
        "source_aov": round_money(source_aov) if source_aov is not None else None,
        "unattributed_orders": unattributed_orders,
        "unattributed_revenue": unattributed_revenue,
        "attribution_rate": round(attribution_rate * 100.0, 2) if attribution_rate is not None else None,
        "blended_roas": round(blended_roas, 2) if blended_roas is not None else None,
        "blended_cvr": round(blended_cvr * 100.0, 3) if blended_cvr is not None else None,
        "blended_aov": round_money(blended_aov) if blended_aov is not None else None,
        "grouped_aov": round_money(grouped_aov) if grouped_aov is not None else None,
        "blended_profit": blended_profit,
        "blended_cpa": round_money(blended_cpa) if blended_cpa is not None else None,
    }

    platform_comparison = _build_platform_comparison(
        db_path,
        start_date=inputs.start_date,
        end_date=inputs.end_date,
        attrib_rows=attrib_rows,
    )
    funnel_snapshot = _build_funnel_snapshot(
        db_path,
        start_date=inputs.start_date,
        end_date=inputs.end_date,
    )

    # Charts: day series
    spend_by_day = ads_get_spend(
        db_path,
        platform="all",
        start_date=inputs.start_date,
        end_date=inputs.end_date,
        breakdown="day",
    )["rows"]
    # attrib_by_day already computed above in the single attribution pass.

    spend_day_map = {r["name"]: r for r in spend_by_day}
    attrib_day_map = {r["date"]: r for r in attrib_by_day}

    # Tracked (all-orders) totals by local reporting day. Do not bucket with
    # substr(ts, 1, 10): warehouse timestamps are UTC while Hyros reports in the
    # account timezone, so late-evening orders otherwise appear a day early.
    tracked_day_map: dict[str, dict[str, Any]] = {}
    new_cust_day_map: dict[str, int] = {}
    net_new_cust_day_map: dict[str, int] = {}
    try:
        tz = report_timezone()
        grouped_by_day: dict[str, set[str]] = defaultdict(set)
        nonrecurring_by_day: dict[str, int] = defaultdict(int)
        net_new_by_day: dict[str, set[str]] = defaultdict(set)

        for row in order_rows:
            parsed_ts = try_parse_iso_ts(row.get("ts"))
            if parsed_ts is None:
                continue
            day = parsed_ts.astimezone(tz).date().isoformat()
            bucket = tracked_day_map.setdefault(
                day,
                {"orders": 0, "revenue": 0.0, "gross": 0.0, "sale_groups": 0},
            )
            bucket["orders"] += 1
            bucket["revenue"] += to_float(row.get("net"))
            bucket["gross"] += to_float(row.get("gross")) or to_float(row.get("net"))

            group_id = _order_group_id(row)
            if group_id:
                grouped_by_day[day].add(group_id)
            if (
                to_float(row.get("refunds")) + to_float(row.get("chargebacks")) <= 0
                and not to_int(row.get("is_recurring"))
            ):
                nonrecurring_by_day[day] += 1

            identity = _order_identity(row)
            first_ts = try_parse_iso_ts(first_by_identity.get(identity))
            if identity and first_ts is not None and first_ts == parsed_ts:
                net_new_by_day[day].add(identity)

        for day, bucket in tracked_day_map.items():
            bucket["sale_groups"] = len(grouped_by_day.get(day, set()))
        new_cust_day_map = dict(nonrecurring_by_day)
        net_new_cust_day_map = {day: len(ids) for day, ids in net_new_by_day.items()}
    except Exception:
        tracked_day_map = {}
        new_cust_day_map = {}
        net_new_cust_day_map = {}

    # Per-day lead counts so the dashboard's Cost-per-Lead trend plots real
    # leads/day (cost / leads) instead of reusing the cost-per-order proxy.
    leads_day_map: dict[str, int] = {}
    try:
        tz_leads = report_timezone()
        for row in lead_rows:
            parsed_ts = try_parse_iso_ts(row.get("ts"))
            if parsed_ts is None:
                continue
            day = parsed_ts.astimezone(tz_leads).date().isoformat()
            leads_day_map[day] = leads_day_map.get(day, 0) + 1
    except Exception:
        leads_day_map = {}

    start_d = date.fromisoformat(inputs.start_date)
    end_d = date.fromisoformat(inputs.end_date)
    all_days: list[str] = []
    d = start_d
    while d <= end_d:
        all_days.append(d.isoformat())
        d += timedelta(days=1)

    time_series = []
    for day in all_days:
        s = spend_day_map.get(day, {})
        a = attrib_day_map.get(day, {})
        t = tracked_day_map.get(day, {})
        cost = to_float(s.get("cost"))
        clicks = to_int(s.get("clicks"))
        revenue = to_float(a.get("revenue"))
        orders = to_float(a.get("orders"))
        cogs = to_float(a.get("cogs"))
        fees = to_float(a.get("fees"))
        tracked_revenue = to_float(t.get("revenue"))
        tracked_orders = to_int(t.get("orders"))
        tracked_sale_groups = to_int(t.get("sale_groups"))
        new_customers_day = int(new_cust_day_map.get(day, 0))
        net_new_customers_day = int(net_new_cust_day_map.get(day, 0))
        leads_day = int(leads_day_map.get(day, 0))
        attributed_sale_groups_day = to_int(a.get("sale_groups"))
        attributed_aov_day = safe_div(revenue, float(attributed_sale_groups_day))
        source_attributed_orders_day = to_int(a.get("source_attributed_orders"))
        source_attributed_revenue_day = to_float(a.get("source_attributed_revenue"))
        source_unique_sales_day = to_int(a.get("source_unique_sales"))
        source_aov_day = safe_div(source_attributed_revenue_day, float(source_unique_sales_day))
        profit = revenue - cost - cogs - fees
        roas = safe_div(revenue, cost)
        blended_roas_day = safe_div(tracked_revenue, cost)
        cvr = safe_div(orders, clicks)
        time_series.append(
            {
                "date": day,
                "cost": round_money(cost),
                "revenue": round_money(revenue),
                "tracked_revenue": round_money(tracked_revenue),
                "profit": round_money(profit),
                "clicks": clicks,
                "orders": round(orders, 2),
                "tracked_orders": tracked_orders,
                "tracked_sale_groups": tracked_sale_groups,
                "new_customers": new_customers_day,
                "net_new_customers": net_new_customers_day,
                "leads": leads_day,
                "attributed_sale_groups": attributed_sale_groups_day,
                "attributed_aov": round_money(attributed_aov_day) if attributed_aov_day is not None else None,
                "source_attributed_orders": source_attributed_orders_day,
                "source_attributed_revenue": round_money(source_attributed_revenue_day),
                "source_unique_sales": source_unique_sales_day,
                "source_aov": round_money(source_aov_day) if source_aov_day is not None else None,
                "roas": round(roas, 2) if roas is not None else None,
                "blended_roas": round(blended_roas_day, 2) if blended_roas_day is not None else None,
                "cvr": round(cvr * 100.0, 2) if cvr is not None else None,
            }
        )

    top_winners = [{"name": r["name"], "profit": r["metrics"]["profit"]} for r in table_rows[:5]]
    top_losers = [{"name": r["name"], "profit": r["metrics"]["profit"]} for r in table_rows_asc[:5]]

    charts = {"time_series": time_series, "top_winners": top_winners, "top_losers": top_losers}

    # Diagnostics
    last_event_ts = health.get("freshness", {}).get("sessions_last_ts") or health.get("freshness", {}).get("orders_last_ts")
    last_spend_ts = integrations.get("ads", {}).get("spend_last_date")
    diagnostics = {
        "data_freshness": {
            "last_event_ts": last_event_ts,
            "last_spend_ts": last_spend_ts,
            "spend_by_platform": integrations.get("ads", {}).get("platforms", {}),
            "notes": "Computed from local warehouse tables (sessions, touchpoints, orders, spend).",
        },
        "attribution_notes": [
            f"Model={_format_model_label(inputs.attribution_model)}, lookback_days={inputs.lookback_days}, conversion_type={inputs.conversion_type}.",
            f"Attribution date basis={date_basis} (conversion vs click).",
        ],
        "assumptions": [
            "This report is computed from local event, touchpoint, and order tables.",
            "net_profit == profit (no fixed cost allocation provided).",
            "For non-paid sources, clicks may be approximated from sessions.",
        ],
        "anomalies": [],
        # Non-fatal query failures we degraded past. Empty in the normal case;
        # the UI uses this to explain an all-zero report caused by a data error
        # rather than an empty range.
        "data_errors": data_errors,
    }

    if inputs.use_date_of_click_attribution:
        diagnostics["assumptions"].append(
            "reported value is typically stored by conversion date; reported_delta may not be perfectly comparable under click-date attribution."
        )

    for platform_name, platform_status in integrations.get("ads", {}).get("platforms", {}).items():
        platform_last_date = str((platform_status or {}).get("last_date") or "")
        if platform_last_date and platform_last_date < inputs.end_date:
            diagnostics["anomalies"].append(
                {
                    "what": f"{platform_name.title()} spend is stale at {platform_last_date} for a report ending {inputs.end_date}.",
                    "likely_cause": "That platform did not refresh through the current sync path or its external push has not arrived.",
                    "verification_step": "Run the platform sync/push and confirm its latest spend date reaches the report end date.",
                }
            )

    if dimension_coverage.get("unmapped_orders", 0) > 0:
        diagnostics["anomalies"].append(
            {
                "what": (
                    f"{dimension_coverage['unmapped_orders']} source-attributed orders "
                    f"are not uniquely mapped to the selected {inputs.active_tab.replace('_', ' ')}."
                ),
                "likely_cause": "Source/identity is known but the platform dimension ID was not captured or uniquely resolvable.",
                "verification_step": "Inspect GHL UTM/click-ID fields and campaign aliases; ambiguous names remain unmapped by design.",
            }
        )

    if tracking_percentage < 85:
        diagnostics["anomalies"].append(
            {
                "what": f"Tracking percentage is {tracking_percentage}%.",
                "likely_cause": "Missing click IDs and/or weak identity stitching to customer_key.",
                "verification_step": "Run tracking.health_check() and inspect sessions click-id coverage + orders_with_source coverage.",
            }
        )

    # Action plan
    action_plan: list[dict[str, Any]] = []
    losers = [r for r in table_rows_asc if (r["metrics"]["profit"] or 0.0) < 0 and (r["metrics"]["cost"] or 0.0) > 50]
    winners = [r for r in table_rows if (r["metrics"]["profit"] or 0.0) > 0 and (r["metrics"]["cost"] or 0.0) > 50]

    if losers:
        worst = losers[0]
        action_plan.append(
            {
                "title": f"Cut/refresh losing {inputs.active_tab.replace('_',' ')}: {worst['name']}",
                "why": "Negative profit with meaningful spend in the selected range.",
                "expected_impact": "high",
                "effort": "low",
                "steps": ["Pause worst performers or cap spend.", "Refresh creative/angle and tighten targeting.", "Verify landing page + tracking for this segment."],
            }
        )
    if winners:
        best = winners[0]
        action_plan.append(
            {
                "title": f"Scale winner {inputs.active_tab.replace('_',' ')}: {best['name']}",
                "why": "Positive profit with measurable spend suggests scale potential.",
                "expected_impact": "med",
                "effort": "low",
                "steps": ["Increase budget 10–20%/day while monitoring CAC.", "Duplicate to adjacent audiences/placements.", "Test 2–3 close creative variants."],
            }
        )
    if tracking_percentage < 90:
        action_plan.append(
            {
                "title": "Improve tracking coverage (click IDs + order source)",
                "why": "Low coverage increases attribution drift and weakens optimization decisions.",
                "expected_impact": "high",
                "effort": "med",
                "steps": [
                    "Persist click IDs (gclid/fbclid/ttclid) first-party and pass server-side.",
                    "Ensure customer_key is set consistently across sessions, touchpoints, and orders.",
                    "Add UTM governance checks + alerts for missing UTMs.",
                ],
            }
        )

    return {
        "report_meta": {
            "report_name": inputs.report_name,
            "date_range": {"start": inputs.start_date, "end": inputs.end_date, **({"preset": inputs.preset} if inputs.preset else {})},
            "attribution_model": _format_model_label(inputs.attribution_model),
            "use_date_of_click_attribution": inputs.use_date_of_click_attribution,
            "base_grouping": "Source",
            "currency": inputs.currency,
            "filters_applied": {
                "conversion_type": inputs.conversion_type,
                "lookback_days": inputs.lookback_days,
                "data_mode": "local_warehouse",
            },
        },
        "tracking": tracking,
        "summary_totals": summary_totals,
        "tabs": _tabs(),
        "table": {
            "active_tab": inputs.active_tab,
            "coverage": dimension_coverage,
            "columns": visible_report_columns([
                {
                    "key": "name",
                    "label": {
                        "traffic_source": "Source",
                        "ad_account": "Ad Account",
                        "campaign": "Campaign",
                        "ad_set": "Ad Set",
                        "ad": "Ad",
                    }.get(inputs.active_tab, "Name"),
                    "type": "dimension",
                },
                {"key": "impressions", "label": "Impressions", "type": "number"},
                {"key": "clicks", "label": "Clicks", "type": "number"},
                {"key": "ctr", "label": "CTR", "type": "percent"},
                {"key": "orders", "label": "Attr. Orders", "type": "number"},
                {"key": "cost", "label": "Cost", "type": "money"},
                {"key": "cpc", "label": "CPC", "type": "money"},
                {"key": "cpm", "label": "CPM", "type": "money"},
                {"key": "cpa", "label": "CPA", "type": "money"},
                {"key": "cvr", "label": "CVR", "type": "percent"},
                {"key": "total_revenue", "label": "Gross Rev.", "type": "money"},
                {"key": "revenue", "label": "Net Rev.", "type": "money"},
                {"key": "aov", "label": "AOV", "type": "money"},
                {"key": "roas", "label": "ROAS", "type": "ratio"},
                {"key": "profit", "label": "Profit", "type": "money"},
                {"key": "margin_pct", "label": "Margin", "type": "percent"},
                {"key": "net_profit", "label": "Net Profit", "type": "money"},
                {"key": "reported", "label": "Ads Reported", "type": "money"},
                {"key": "reported_delta", "label": "Ads Delta", "type": "money"},
            ], reported_rows=reported_rows),
            "rows": table_rows,
            "totals_row": totals_metrics,
            "sort": {"by": "profit", "dir": "desc"},
            "search_hint": "Search by name/id/utm",
        },
        "charts": charts,
        "platform_comparison": {"rows": platform_comparison},
        "funnels": {"rows": funnel_snapshot},
        "diagnostics": diagnostics,
        "action_plan": action_plan,
    }
