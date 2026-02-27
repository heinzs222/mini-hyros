from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Literal

from attributionops.db import query
from attributionops.tools.ads import ads_get_reported_value, ads_get_spend
from attributionops.tools.attribution import attribution_day_totals, attribution_run
from attributionops.tools.integrations import integrations_status
from attributionops.tools.tracking import tracking_health_check
from attributionops.util import round_money, safe_div, to_float, to_int


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
) -> dict[str, Any]:
    clicks_i = int(clicks)
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
    return {
        "clicks": clicks_i,
        "orders": round(orders_f, 2),
        "cost": round_money(cost),
        "cpc": round_money(cpc) if cpc is not None else None,
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
    spend_rows = query(
        db_path,
        """
        SELECT
          platform,
          SUM(CAST(clicks AS REAL)) AS clicks,
          SUM(CAST(cost AS REAL)) AS cost
        FROM spend
        WHERE date BETWEEN :start_date AND :end_date
        GROUP BY platform;
        """,
        {"start_date": start_date, "end_date": end_date},
    ).rows

    touch_rows = query(
        db_path,
        """
        SELECT
          platform,
          COUNT(DISTINCT session_id) AS touch_clicks
        FROM touchpoints
        WHERE date(substr(ts,1,10)) BETWEEN :start_date AND :end_date
          AND COALESCE(platform, '') <> ''
        GROUP BY platform;
        """,
        {"start_date": start_date, "end_date": end_date},
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
    sessions = query(
        db_path,
        """
        SELECT session_id, ts, customer_key, landing_page
        FROM sessions
        WHERE date(substr(ts,1,10)) BETWEEN :start_date AND :end_date;
        """,
        {"start_date": start_date, "end_date": end_date},
    ).rows

    conversions = query(
        db_path,
        """
        SELECT customer_key, type, value
        FROM conversions
        WHERE date(substr(ts,1,10)) BETWEEN :start_date AND :end_date
          AND COALESCE(customer_key, '') <> '';
        """,
        {"start_date": start_date, "end_date": end_date},
    ).rows

    funnel_name_map = _funnel_name_map(db_path)

    visits_by_funnel: dict[str, set[str]] = defaultdict(set)
    first_funnel_by_customer: dict[str, tuple[str, str]] = {}

    for s in sessions:
        session_id = str(s.get("session_id") or "")
        ts = str(s.get("ts") or "")
        customer_key = str(s.get("customer_key") or "")
        funnel_id = _funnel_key_from_path(str(s.get("landing_page") or "/"))

        if session_id:
            visits_by_funnel[funnel_id].add(session_id)

        if customer_key:
            prev = first_funnel_by_customer.get(customer_key)
            if prev is None or ts < prev[0]:
                first_funnel_by_customer[customer_key] = (ts, funnel_id)

    lead_customers: dict[str, set[str]] = defaultdict(set)
    purchase_customers: dict[str, set[str]] = defaultdict(set)
    revenue_by_funnel: dict[str, float] = defaultdict(float)

    for c in conversions:
        customer_key = str(c.get("customer_key") or "")
        if not customer_key:
            continue
        owner = first_funnel_by_customer.get(customer_key)
        if owner is None:
            continue
        funnel_id = owner[1]
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
        visits = len(visits_by_funnel.get(funnel_id, set()))
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


def _key_from_spend_row(row: dict[str, Any], breakdown: str) -> str:
    if breakdown == "traffic_source":
        return str(row.get("name") or "")
    if breakdown == "ad_account":
        return f"{row.get('platform','')}|{row.get('account_id','')}"
    if breakdown == "campaign":
        return f"{row.get('platform','')}|{row.get('account_id','')}|{row.get('campaign_id','')}"
    if breakdown == "ad_set":
        return f"{row.get('platform','')}|{row.get('account_id','')}|{row.get('campaign_id','')}|{row.get('adset_id','')}"
    if breakdown == "ad":
        return f"{row.get('platform','')}|{row.get('account_id','')}|{row.get('campaign_id','')}|{row.get('adset_id','')}|{row.get('ad_id','')}"
    if breakdown == "day":
        return str(row.get("name") or "")
    raise ValueError("invalid breakdown")


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
    if breakdown == "ad_account":
        return f"{row.get('platform','')}|{row.get('account_id','')}"
    if breakdown == "campaign":
        return f"{row.get('platform','')}|{row.get('account_id','')}|{row.get('campaign_id','')}"
    if breakdown == "ad_set":
        return f"{row.get('platform','')}|{row.get('account_id','')}|{row.get('campaign_id','')}|{row.get('adset_id','')}"
    if breakdown == "ad":
        return f"{row.get('platform','')}|{row.get('account_id','')}|{row.get('campaign_id','')}|{row.get('adset_id','')}|{row.get('ad_id','')}"
    raise ValueError("invalid breakdown")


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


def _child_count_map(db_path: str, *, start_date: str, end_date: str, active_tab: TabKey) -> dict[str, int]:
    if active_tab == "ad":
        return {}

    spend_raw = query(
        db_path,
        """
        SELECT platform, account_id, campaign_id, adset_id, ad_id
        FROM spend
        WHERE date BETWEEN :start_date AND :end_date;
        """,
        {"start_date": start_date, "end_date": end_date},
    ).rows

    touch_raw = query(
        db_path,
        """
        SELECT
          platform,
          '' AS account_id,
          campaign_id,
          adset_id,
          ad_id
        FROM touchpoints
        WHERE date(substr(ts,1,10)) BETWEEN :start_date AND :end_date;
        """,
        {"start_date": start_date, "end_date": end_date},
    ).rows

    raw_rows = list(spend_raw) + list(touch_raw)

    children: dict[str, set[str]] = defaultdict(set)

    for r in raw_rows:
        platform = str(r.get("platform") or "")
        account_id = str(r.get("account_id") or "")
        campaign_id = str(r.get("campaign_id") or "")
        adset_id = str(r.get("adset_id") or "")
        ad_id = str(r.get("ad_id") or "")

        if active_tab == "traffic_source":
            parent = _traffic_source_for_platform(platform)
            child = f"{platform}|{account_id}"
            children[parent].add(child)
        elif active_tab == "ad_account":
            parent = f"{platform}|{account_id}"
            child = f"{platform}|{account_id}|{campaign_id}"
            children[parent].add(child)
        elif active_tab == "campaign":
            parent = f"{platform}|{account_id}|{campaign_id}"
            child = f"{platform}|{account_id}|{campaign_id}|{adset_id}"
            children[parent].add(child)
        elif active_tab == "ad_set":
            parent = f"{platform}|{account_id}|{campaign_id}|{adset_id}"
            child = f"{platform}|{account_id}|{campaign_id}|{adset_id}|{ad_id}"
            children[parent].add(child)

    return {k: len(v) for k, v in children.items()}


def build_hyros_like_report(db_path: str, inputs: ReportInputs) -> dict[str, Any]:
    # 1) integrations.status + tracking.health_check
    integrations = integrations_status(db_path)
    health = tracking_health_check(db_path)

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

    # 3) attribution
    attrib_rows = attribution_run(
        db_path,
        model=inputs.attribution_model,
        start_date=inputs.start_date,
        end_date=inputs.end_date,
        lookback_days=inputs.lookback_days,
        conversion_type=inputs.conversion_type,
        value_type="revenue",
        date_basis=date_basis,
    )["rows"]

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

    # ── Build ad_names lookup for human-readable display names ─────────────
    _ad_name_map: dict[tuple[str, str, str], str] = {}  # (platform, entity_type, entity_id) → name
    try:
        _ad_name_rows = query(
            db_path,
            "SELECT platform, entity_type, entity_id, name FROM ad_names;",
            {},
        ).rows
        for anr in _ad_name_rows:
            _p = str(anr.get("platform") or "").strip()
            _et = str(anr.get("entity_type") or "").strip()
            _eid = str(anr.get("entity_id") or "").strip()
            _nm = str(anr.get("name") or "").strip()
            if _p and _et and _eid and _nm:
                _ad_name_map[(_p, _et, _eid)] = _nm
    except Exception:
        pass  # table may not exist yet

    def _resolve_name(platform: str, entity_type: str, entity_id: str) -> str:
        """Look up human-readable name from ad_names, fall back to raw ID."""
        if not entity_id:
            return ""
        return _ad_name_map.get((platform, entity_type, entity_id), entity_id)

    # ── Always merge touchpoint-derived entities (not just a fallback) ────
    # This is the core of "Hyros-style" tracking: we learn campaign/adset/ad
    # hierarchy purely from the tracking pixel UTM params.
    if inputs.active_tab in {"ad_account", "campaign", "ad_set", "ad"}:
        touchpoint_rows = query(
            db_path,
            """
            SELECT platform, campaign_id, adset_id, ad_id, COUNT(*) AS touches
            FROM touchpoints
            WHERE date(substr(ts,1,10)) BETWEEN :start_date AND :end_date
              AND platform != ''
            GROUP BY platform, campaign_id, adset_id, ad_id;
            """,
            {"start_date": inputs.start_date, "end_date": inputs.end_date},
        ).rows

        for r in touchpoint_rows:
            platform = str(r.get("platform") or "").strip()
            campaign_id = str(r.get("campaign_id") or "").strip()
            adset_id = str(r.get("adset_id") or "").strip()
            ad_id = str(r.get("ad_id") or "").strip()

            if inputs.active_tab == "ad_account":
                if not platform:
                    continue
                key = f"{platform}|"
                name = f"{platform} account"
            elif inputs.active_tab == "campaign":
                if not campaign_id:
                    continue
                key = f"{platform}||{campaign_id}"
                name = _resolve_name(platform, "campaign", campaign_id)
            elif inputs.active_tab == "ad_set":
                if not adset_id:
                    continue
                key = f"{platform}||{campaign_id}|{adset_id}"
                name = _resolve_name(platform, "adset", adset_id)
            else:  # ad
                if not ad_id:
                    continue
                key = f"{platform}||{campaign_id}|{adset_id}|{ad_id}"
                name = _resolve_name(platform, "ad", ad_id)

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

    # ── Also enrich existing row names from ad_names when they are raw IDs ─
    for key, v in by_key.items():
        current_name = str(v.get("name") or "")
        if not current_name or current_name == key:
            continue
        # Try to resolve from ad_names if name still looks like an ID
        parts = key.split("|") if key else []
        _plat = parts[0] if parts else ""
        if breakdown == "campaign" and len(parts) >= 3:
            resolved = _resolve_name(_plat, "campaign", parts[2])
            if resolved and resolved != parts[2]:
                v["name"] = resolved
        elif breakdown == "ad_set" and len(parts) >= 4:
            resolved = _resolve_name(_plat, "adset", parts[3])
            if resolved and resolved != parts[3]:
                v["name"] = resolved
        elif breakdown == "ad" and len(parts) >= 5:
            resolved = _resolve_name(_plat, "ad", parts[4])
            if resolved and resolved != parts[4]:
                v["name"] = resolved

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
            WHERE date(substr(ts,1,10)) BETWEEN :start_date AND :end_date
            GROUP BY 1;
            """,
            {"start_date": inputs.start_date, "end_date": inputs.end_date},
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
        cost = float(v["cost"])
        total_revenue = float(v["total_revenue"])
        revenue = float(v["revenue"])
        cogs = float(v["cogs"])
        fees = float(v["fees"])
        orders = float(v["_orders"])
        reported_value = float(v["reported"]) if v["reported"] is not None else None

        metrics = _format_metrics(
            clicks=clicks,
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

    totals_metrics = _format_metrics(
        clicks=int(totals["clicks"]),
        cost=float(totals["cost"]),
        total_revenue=float(totals["total_revenue"]),
        revenue=float(totals["revenue"]),
        cogs=float(totals["cogs"]),
        fees=float(totals["fees"]),
        orders=float(totals["_orders"]),
        reported=float(totals["reported"]) if totals["_reported_any"] else None,
        fixed_cost_allocation=None,
    )

    roas = safe_div(float(totals["revenue"]), float(totals["cost"]))
    mer = safe_div(float(totals["total_revenue"]), float(totals["cost"]))
    conversions_count = float(totals["_orders"])
    cpa = safe_div(float(totals["cost"]), conversions_count) if conversions_count else None

    summary_totals = {
        **totals_metrics,
        "roas": round_money(roas) if roas is not None else None,
        "mer": round_money(mer) if mer is not None else None,
        "cac": round_money(cpa) if cpa is not None else None,
        "cpa": round_money(cpa) if cpa is not None else None,
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
    attrib_by_day = attribution_day_totals(
        db_path,
        model=inputs.attribution_model,
        start_date=inputs.start_date,
        end_date=inputs.end_date,
        lookback_days=inputs.lookback_days,
        conversion_type=inputs.conversion_type,
        date_basis=date_basis,
    )["rows"]

    spend_day_map = {r["name"]: r for r in spend_by_day}
    attrib_day_map = {r["date"]: r for r in attrib_by_day}

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
        cost = to_float(s.get("cost"))
        clicks = to_int(s.get("clicks"))
        revenue = to_float(a.get("revenue"))
        orders = to_float(a.get("orders"))
        cogs = to_float(a.get("cogs"))
        fees = to_float(a.get("fees"))
        profit = revenue - cost - cogs - fees
        roas = safe_div(revenue, cost)
        cvr = safe_div(orders, clicks)
        time_series.append(
            {
                "date": day,
                "cost": round_money(cost),
                "revenue": round_money(revenue),
                "profit": round_money(profit),
                "clicks": clicks,
                "orders": round(orders, 2),
                "roas": round(roas, 2) if roas is not None else None,
                "cvr": round(cvr * 100.0, 2) if cvr is not None else None,
            }
        )

    top_winners = [{"name": r["name"], "profit": r["metrics"]["profit"]} for r in table_rows[:5]]
    top_losers = [{"name": r["name"], "profit": r["metrics"]["profit"]} for r in sorted(table_rows, key=lambda r: (r["metrics"]["profit"] or 0.0))[:5]]

    charts = {"time_series": time_series, "top_winners": top_winners, "top_losers": top_losers}

    # Diagnostics
    last_event_ts = health.get("freshness", {}).get("sessions_last_ts") or health.get("freshness", {}).get("orders_last_ts")
    last_spend_ts = integrations.get("ads", {}).get("spend_last_date")
    diagnostics = {
        "data_freshness": {
            "last_event_ts": last_event_ts,
            "last_spend_ts": last_spend_ts,
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
    }

    if inputs.use_date_of_click_attribution:
        diagnostics["assumptions"].append(
            "reported value is typically stored by conversion date; reported_delta may not be perfectly comparable under click-date attribution."
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
    losers = [r for r in sorted(table_rows, key=lambda r: (r["metrics"]["profit"] or 0.0)) if (r["metrics"]["profit"] or 0.0) < 0 and (r["metrics"]["cost"] or 0.0) > 50]
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
            "columns": [
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
                {"key": "clicks", "label": "Clicks", "type": "number"},
                {"key": "orders", "label": "Orders", "type": "number"},
                {"key": "cost", "label": "Cost", "type": "money"},
                {"key": "cpc", "label": "CPC", "type": "money"},
                {"key": "cpa", "label": "CPA", "type": "money"},
                {"key": "cvr", "label": "CVR", "type": "percent"},
                {"key": "total_revenue", "label": "Total Revenue", "type": "money"},
                {"key": "revenue", "label": "Revenue", "type": "money"},
                {"key": "aov", "label": "AOV", "type": "money"},
                {"key": "roas", "label": "ROAS", "type": "ratio"},
                {"key": "profit", "label": "Profit", "type": "money"},
                {"key": "margin_pct", "label": "Margin", "type": "percent"},
                {"key": "net_profit", "label": "Net Profit", "type": "money"},
                {"key": "reported", "label": "Reported", "type": "money"},
                {"key": "reported_delta", "label": "Rep. Delta", "type": "money"},
            ],
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
