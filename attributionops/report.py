from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
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
    reported: float | None,
    fixed_cost_allocation: float | None,
) -> dict[str, Any]:
    profit = revenue - cost - cogs - fees
    net_profit = profit if fixed_cost_allocation is None else profit - fixed_cost_allocation
    reported_delta = None if reported is None else (revenue - reported)
    return {
        "clicks": int(clicks),
        "cost": round_money(cost),
        "total_revenue": round_money(total_revenue),
        "revenue": round_money(revenue),
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

    children: dict[str, set[str]] = defaultdict(set)

    for r in spend_raw:
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
        clicks = int(v["clicks"])
        cost = float(v["cost"])
        total_revenue = float(v["total_revenue"])
        revenue = float(v["revenue"])
        cogs = float(v["cogs"])
        fees = float(v["fees"])
        reported_value = float(v["reported"]) if v["reported"] is not None else None

        metrics = _format_metrics(
            clicks=clicks,
            cost=cost,
            total_revenue=total_revenue,
            revenue=revenue,
            cogs=cogs,
            fees=fees,
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
    }

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
    all_days = sorted(set(list(spend_day_map.keys()) + list(attrib_day_map.keys())))
    time_series = []
    for day in all_days:
        s = spend_day_map.get(day, {})
        a = attrib_day_map.get(day, {})
        cost = to_float(s.get("cost"))
        clicks = to_int(s.get("clicks"))
        revenue = to_float(a.get("revenue"))
        cogs = to_float(a.get("cogs"))
        fees = to_float(a.get("fees"))
        profit = revenue - cost - cogs - fees
        time_series.append({"date": day, "cost": round_money(cost), "revenue": round_money(revenue), "profit": round_money(profit), "clicks": clicks})

    top_winners = [{"name": r["name"], "profit": r["metrics"]["profit"]} for r in table_rows[:5]]
    top_losers = [{"name": r["name"], "profit": r["metrics"]["profit"]} for r in sorted(table_rows, key=lambda r: (r["metrics"]["profit"] or 0.0))[:5]]

    charts = {"time_series": time_series, "top_winners": top_winners, "top_losers": top_losers}

    # Diagnostics
    last_event_ts = health.get("freshness", {}).get("sessions_last_ts") or health.get("freshness", {}).get("orders_last_ts")
    last_spend_ts = integrations.get("ads", {}).get("spend_last_date")
    diagnostics = {
        "data_freshness": {"last_event_ts": last_event_ts, "last_spend_ts": last_spend_ts, "notes": "Local dummy dataset timestamps."},
        "attribution_notes": [
            f"Model={_format_model_label(inputs.attribution_model)}, lookback_days={inputs.lookback_days}, conversion_type={inputs.conversion_type}.",
            f"Attribution date basis={date_basis} (conversion vs click).",
        ],
        "assumptions": [
            "This report is computed from synthetic dummy data (not business truth).",
            "net_profit == profit (no fixed cost allocation provided).",
            "For non-paid sources, clicks may be approximated from sessions.",
        ],
        "anomalies": [],
    }

    if inputs.use_date_of_click_attribution:
        diagnostics["assumptions"].append(
            "reported value is stored by conversion date in the dummy dataset; reported_delta may not be comparable under click-date attribution."
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
                "data_mode": "local_dummy",
            },
        },
        "tracking": tracking,
        "summary_totals": summary_totals,
        "tabs": _tabs(),
        "table": {
            "active_tab": inputs.active_tab,
            "columns": [
                {"key": "name", "label": "Campaign", "type": "dimension"},
                {"key": "clicks", "label": "Clicks", "type": "number"},
                {"key": "cost", "label": "Cost", "type": "money"},
                {"key": "total_revenue", "label": "Total Revenue", "type": "money"},
                {"key": "revenue", "label": "Revenue", "type": "money"},
                {"key": "profit", "label": "Profit", "type": "money"},
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
        "diagnostics": diagnostics,
        "action_plan": action_plan,
    }
