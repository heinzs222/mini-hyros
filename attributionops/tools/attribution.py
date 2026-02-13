from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from datetime import timedelta
from typing import Any

from attributionops.db import query
from attributionops.util import exp_decay_weight, parse_iso_ts, to_float


@dataclass(frozen=True)
class AttributedTouch:
    channel: str
    platform: str
    campaign_id: str
    adset_id: str
    ad_id: str
    creative_id: str
    account_id: str


def _build_account_map(spend_rows: list[dict[str, Any]]) -> dict[tuple[str, str, str, str], str]:
    mapping: dict[tuple[str, str, str, str], str] = {}
    for r in spend_rows:
        key = (
            str(r.get("platform") or ""),
            str(r.get("campaign_id") or ""),
            str(r.get("adset_id") or ""),
            str(r.get("ad_id") or ""),
        )
        acct = str(r.get("account_id") or "")
        if key not in mapping and acct:
            mapping[key] = acct
    return mapping


def _weights_for_model(model: str, touchpoints: list[dict[str, Any]], order_ts, *, half_life_days: float = 7.0) -> list[float]:
    model = model.lower().strip().replace("-", "_").replace(" ", "_")
    n = len(touchpoints)
    if n == 0:
        return []
    if n == 1:
        return [1.0]

    if model in ("last_click", "last"):
        return [0.0] * (n - 1) + [1.0]
    if model in ("first_click", "first"):
        return [1.0] + [0.0] * (n - 1)
    if model in ("linear",):
        return [1.0 / n] * n
    if model in ("time_decay", "timedecay"):
        weights = []
        for tp in touchpoints:
            tp_ts = parse_iso_ts(str(tp["ts"]))
            delta_days = (order_ts - tp_ts).total_seconds() / 86400.0
            weights.append(exp_decay_weight(delta_days, half_life_days=half_life_days))
        s = sum(weights) or 1.0
        return [w / s for w in weights]
    if model in ("data_driven_proxy", "data_driven", "ddp"):
        # Simple proxy: 40% first, 40% last, 20% distributed across middle.
        middle = n - 2
        weights = [0.4] + [0.0] * middle + [0.4]
        if middle > 0:
            each = 0.2 / middle
            for i in range(1, n - 1):
                weights[i] = each
        return weights

    raise ValueError("model must be one of: last_click, first_click, linear, time_decay, data_driven_proxy")


def attribution_run(
    db_path: str,
    *,
    model: str,
    start_date: str,
    end_date: str,
    lookback_days: int,
    conversion_type: str,
    value_type: str,
    date_basis: str = "conversion",
) -> dict[str, object]:
    if conversion_type.lower() != "purchase":
        raise ValueError("dummy dataset only supports conversion_type=Purchase")

    date_basis = date_basis.lower().strip()
    if date_basis not in ("conversion", "click"):
        raise ValueError("date_basis must be one of: conversion, click")

    start_d = date.fromisoformat(start_date)
    end_d = date.fromisoformat(end_date)
    orders_end_d = end_d if date_basis == "conversion" else (end_d + timedelta(days=lookback_days))

    spend_rows = query(
        db_path,
        "SELECT DISTINCT platform, account_id, campaign_id, adset_id, ad_id FROM spend;",
    ).rows
    account_map = _build_account_map(spend_rows)

    orders = query(
        db_path,
        """
        SELECT order_id, ts, gross, net, refunds, chargebacks, cogs, fees, customer_key
        FROM orders
        WHERE date(substr(ts,1,10)) BETWEEN :start_date AND :end_date
          AND COALESCE(customer_key,'') <> '';
        """,
        {"start_date": start_date, "end_date": orders_end_d.isoformat()},
    ).rows

    touchpoints = query(
        db_path,
        """
        SELECT ts, channel, platform, campaign_id, adset_id, ad_id, creative_id, customer_key
        FROM touchpoints
        WHERE COALESCE(customer_key,'') <> '';
        """,
    ).rows

    tps_by_customer: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for tp in touchpoints:
        tps_by_customer[str(tp["customer_key"])].append(tp)
    for ck in tps_by_customer:
        tps_by_customer[ck].sort(key=lambda r: r["ts"])

    lookback = timedelta(days=lookback_days)

    agg: dict[tuple[str, str, str, str, str, str, str], dict[str, Any]] = {}

    unattributed_orders = 0
    for o in orders:
        customer_key = str(o["customer_key"])
        if customer_key not in tps_by_customer:
            unattributed_orders += 1
            continue
        order_ts = parse_iso_ts(str(o["ts"]))
        window_start = order_ts - lookback

        window_tps = []
        for tp in tps_by_customer[customer_key]:
            tp_ts = parse_iso_ts(str(tp["ts"]))
            if tp_ts < window_start:
                continue
            if tp_ts <= order_ts:
                window_tps.append(tp)
        if not window_tps:
            unattributed_orders += 1
            continue

        weights = _weights_for_model(model, window_tps, order_ts)

        gross = to_float(o.get("gross"))
        net = to_float(o.get("net"))
        cogs = to_float(o.get("cogs"))
        fees = to_float(o.get("fees"))

        for tp, w in zip(window_tps, weights, strict=True):
            platform = str(tp.get("platform") or "")
            campaign_id = str(tp.get("campaign_id") or "")
            adset_id = str(tp.get("adset_id") or "")
            ad_id = str(tp.get("ad_id") or "")
            creative_id = str(tp.get("creative_id") or "")
            channel = str(tp.get("channel") or "")

            account_id = account_map.get((platform, campaign_id, adset_id, ad_id), "")

            if date_basis == "click":
                tp_ts = parse_iso_ts(str(tp["ts"]))
                tp_d = tp_ts.date()
                if tp_d < start_d or tp_d > end_d:
                    continue

            dim_key = (channel, platform, account_id, campaign_id, adset_id, ad_id, creative_id)
            if dim_key not in agg:
                agg[dim_key] = {
                    "channel": channel,
                    "platform": platform,
                    "account_id": account_id,
                    "campaign_id": campaign_id,
                    "adset_id": adset_id,
                    "ad_id": ad_id,
                    "creative_id": creative_id,
                    "orders": 0.0,
                    "total_revenue": 0.0,
                    "revenue": 0.0,
                    "cogs": 0.0,
                    "fees": 0.0,
                }
            agg[dim_key]["orders"] = float(agg[dim_key]["orders"]) + w
            agg[dim_key]["total_revenue"] = float(agg[dim_key]["total_revenue"]) + gross * w
            agg[dim_key]["revenue"] = float(agg[dim_key]["revenue"]) + net * w
            agg[dim_key]["cogs"] = float(agg[dim_key]["cogs"]) + cogs * w
            agg[dim_key]["fees"] = float(agg[dim_key]["fees"]) + fees * w

    out_rows = []
    for row in agg.values():
        row_out = dict(row)
        vt = value_type.lower().strip()
        if vt in ("total_revenue", "gross"):
            primary_value = row_out["total_revenue"]
        elif vt in ("revenue", "net_revenue", "net"):
            primary_value = row_out["revenue"]
        elif vt in ("cogs",):
            primary_value = row_out["cogs"]
        elif vt in ("fees", "processing_fees"):
            primary_value = row_out["fees"]
        elif vt in ("orders", "conversions"):
            primary_value = row_out["orders"]
        else:
            raise ValueError("value_type must be one of: total_revenue, revenue, cogs, fees, orders")

        row_out["primary_value"] = float(primary_value)
        for k in ("total_revenue", "revenue", "cogs", "fees", "primary_value"):
            row_out[k] = float(f"{float(row_out[k]):.2f}")
        row_out["orders"] = float(f"{float(row_out['orders']):.4f}")
        out_rows.append(row_out)

    return {
        "model": model,
        "start_date": start_date,
        "end_date": end_date,
        "lookback_days": lookback_days,
        "date_basis": date_basis,
        "conversion_type": conversion_type,
        "value_type": value_type,
        "rows": out_rows,
        "unattributed_orders": int(unattributed_orders),
        "notes": [
            "Local dummy attribution computed from touchpoints+orders by customer_key.",
            "Orders without any touchpoint in lookback window are excluded from attributed rows.",
        ],
    }


def attribution_day_totals(
    db_path: str,
    *,
    model: str,
    start_date: str,
    end_date: str,
    lookback_days: int,
    conversion_type: str,
    date_basis: str = "conversion",
) -> dict[str, object]:
    # Convenience for report charting: aggregates attributed order components by attribution date.
    date_basis = date_basis.lower().strip()
    if date_basis not in ("conversion", "click"):
        raise ValueError("date_basis must be one of: conversion, click")

    start_d = date.fromisoformat(start_date)
    end_d = date.fromisoformat(end_date)
    orders_end_d = end_d if date_basis == "conversion" else (end_d + timedelta(days=lookback_days))

    orders = query(
        db_path,
        """
        SELECT ts, gross, net, cogs, fees, customer_key
        FROM orders
        WHERE date(substr(ts,1,10)) BETWEEN :start_date AND :end_date
          AND COALESCE(customer_key,'') <> '';
        """,
        {"start_date": start_date, "end_date": orders_end_d.isoformat()},
    ).rows

    touchpoints = query(
        db_path,
        """
        SELECT ts, channel, platform, campaign_id, adset_id, ad_id, creative_id, customer_key
        FROM touchpoints
        WHERE COALESCE(customer_key,'') <> '';
        """,
    ).rows

    tps_by_customer: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for tp in touchpoints:
        tps_by_customer[str(tp["customer_key"])].append(tp)
    for ck in tps_by_customer:
        tps_by_customer[ck].sort(key=lambda r: r["ts"])

    lookback = timedelta(days=lookback_days)

    by_day: dict[str, dict[str, float]] = defaultdict(lambda: {"orders": 0.0, "total_revenue": 0.0, "revenue": 0.0, "cogs": 0.0, "fees": 0.0})

    unattributed_orders = 0
    for o in orders:
        ck = str(o["customer_key"])
        if ck not in tps_by_customer:
            unattributed_orders += 1
            continue

        order_ts = parse_iso_ts(str(o["ts"]))
        window_start = order_ts - lookback

        window_tps = []
        for tp in tps_by_customer[ck]:
            tp_ts = parse_iso_ts(str(tp["ts"]))
            if tp_ts < window_start:
                continue
            if tp_ts <= order_ts:
                window_tps.append(tp)
        if not window_tps:
            unattributed_orders += 1
            continue

        weights = _weights_for_model(model, window_tps, order_ts)

        gross = to_float(o.get("gross"))
        net = to_float(o.get("net"))
        cogs = to_float(o.get("cogs"))
        fees = to_float(o.get("fees"))

        for tp, w in zip(window_tps, weights, strict=True):
            if date_basis == "conversion":
                d = order_ts.date()
                if d < start_d or d > end_d:
                    continue
                key = d.isoformat()
            else:
                tp_ts = parse_iso_ts(str(tp["ts"]))
                d = tp_ts.date()
                if d < start_d or d > end_d:
                    continue
                key = d.isoformat()

            by_day[key]["orders"] += w
            by_day[key]["total_revenue"] += gross * w
            by_day[key]["revenue"] += net * w
            by_day[key]["cogs"] += cogs * w
            by_day[key]["fees"] += fees * w

    rows_out = []
    for d in sorted(by_day.keys()):
        row = by_day[d]
        rows_out.append(
            {
                "date": d,
                "orders": float(f"{row['orders']:.4f}"),
                "total_revenue": float(f"{row['total_revenue']:.2f}"),
                "revenue": float(f"{row['revenue']:.2f}"),
                "cogs": float(f"{row['cogs']:.2f}"),
                "fees": float(f"{row['fees']:.2f}"),
            }
        )

    return {
        "model": model,
        "start_date": start_date,
        "end_date": end_date,
        "lookback_days": lookback_days,
        "date_basis": date_basis,
        "conversion_type": conversion_type,
        "rows": rows_out,
        "unattributed_orders": int(unattributed_orders),
        "notes": ["Local helper for charting: attributed totals grouped by attribution date."],
    }
