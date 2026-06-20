from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
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
            tp_ts = tp.get("_ts") or parse_iso_ts(str(tp["ts"]))
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


# ── Shared loading + windowing ───────────────────────────────────────────────
# The previous implementation loaded the ENTIRE touchpoints table (no date or
# customer filter) and re-parsed every timestamp once per candidate order in a
# linear inner scan — and did all of that twice per report (run + day totals).
#
# The helpers below load orders and touchpoints once, bounded to the relevant
# date window AND to customers that actually have an in-range order, parse each
# timestamp exactly once, and use binary search to slice each order's lookback
# window out of the customer's already-sorted touch list. ``run`` and
# ``day_totals`` are derived from a single pass via ``_attributed``.


def _normalize_basis(date_basis: str) -> str:
    date_basis = date_basis.lower().strip()
    if date_basis not in ("conversion", "click"):
        raise ValueError("date_basis must be one of: conversion, click")
    return date_basis


def _load_orders_and_touchpoints(
    db_path: str,
    *,
    start_d: date,
    lookback_days: int,
    orders_end_d: date,
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]], dict[str, list[datetime]]]:
    # Orders within [start_d, orders_end_d] (inclusive). Sargable range on ts:
    # ts is ISO-8601, so lexical comparison against date strings is correct and
    # index-usable (`ts >= 'YYYY-MM-DD'`, `ts < next-day`).
    orders_end_excl = (orders_end_d + timedelta(days=1)).isoformat()
    orders = query(
        db_path,
        """
        SELECT order_id, ts, gross, net, refunds, chargebacks, cogs, fees, customer_key
        FROM orders
        WHERE ts >= :start AND ts < :end_excl
          AND COALESCE(customer_key,'') <> '';
        """,
        {"start": start_d.isoformat(), "end_excl": orders_end_excl},
    ).rows

    # Touchpoints only need to span [start_d - lookback, orders_end_d] and only
    # for customers that have an in-range order (others are never inspected).
    win_start = (start_d - timedelta(days=lookback_days)).isoformat()
    touchpoints = query(
        db_path,
        """
        SELECT ts, channel, platform, campaign_id, adset_id, ad_id, creative_id, customer_key
        FROM touchpoints
        WHERE COALESCE(customer_key,'') <> ''
          AND ts >= :win_start AND ts < :end_excl
          AND customer_key IN (
            SELECT customer_key FROM orders
            WHERE ts >= :start AND ts < :end_excl AND COALESCE(customer_key,'') <> ''
          );
        """,
        {"win_start": win_start, "start": start_d.isoformat(), "end_excl": orders_end_excl},
    ).rows

    tps_by_customer: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for tp in touchpoints:
        tp["_ts"] = parse_iso_ts(str(tp["ts"]))  # parse exactly once
        tps_by_customer[str(tp["customer_key"])].append(tp)

    dts_by_customer: dict[str, list[datetime]] = {}
    for ck, tps in tps_by_customer.items():
        tps.sort(key=lambda r: r["_ts"])
        dts_by_customer[ck] = [t["_ts"] for t in tps]

    return orders, tps_by_customer, dts_by_customer


def _attributed(
    orders: list[dict[str, Any]],
    tps_by_customer: dict[str, list[dict[str, Any]]],
    dts_by_customer: dict[str, list[datetime]],
    *,
    model: str,
    lookback: timedelta,
) -> tuple[list[tuple[dict[str, Any], datetime, list[dict[str, Any]], list[float]]], int]:
    """Single pass: for each attributable order return (order, order_ts, window_tps, weights)."""
    contributions: list[tuple[dict[str, Any], datetime, list[dict[str, Any]], list[float]]] = []
    unattributed = 0
    for o in orders:
        ck = str(o["customer_key"])
        dts = dts_by_customer.get(ck)
        if not dts:
            unattributed += 1
            continue
        order_ts = parse_iso_ts(str(o["ts"]))
        window_start = order_ts - lookback
        lo = bisect_left(dts, window_start)
        hi = bisect_right(dts, order_ts)
        if lo >= hi:
            unattributed += 1
            continue
        window_tps = tps_by_customer[ck][lo:hi]
        weights = _weights_for_model(model, window_tps, order_ts)
        contributions.append((o, order_ts, window_tps, weights))
    return contributions, unattributed


def _aggregate_dim_rows(
    contributions: list[tuple[dict[str, Any], datetime, list[dict[str, Any]], list[float]]],
    *,
    account_map: dict[tuple[str, str, str, str], str],
    value_type: str,
    date_basis: str,
    start_d: date,
    end_d: date,
) -> list[dict[str, Any]]:
    agg: dict[tuple[str, str, str, str, str, str, str], dict[str, Any]] = {}

    for o, order_ts, window_tps, weights in contributions:
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
                tp_d = tp["_ts"].date()
                if tp_d < start_d or tp_d > end_d:
                    continue

            dim_key = (channel, platform, account_id, campaign_id, adset_id, ad_id, creative_id)
            row = agg.get(dim_key)
            if row is None:
                row = {
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
                agg[dim_key] = row
            row["orders"] += w
            row["total_revenue"] += gross * w
            row["revenue"] += net * w
            row["cogs"] += cogs * w
            row["fees"] += fees * w

    vt = value_type.lower().strip()
    out_rows = []
    for row in agg.values():
        row_out = dict(row)
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

    return out_rows


def _aggregate_day_rows(
    contributions: list[tuple[dict[str, Any], datetime, list[dict[str, Any]], list[float]]],
    *,
    date_basis: str,
    start_d: date,
    end_d: date,
) -> list[dict[str, Any]]:
    by_day: dict[str, dict[str, float]] = defaultdict(
        lambda: {"orders": 0.0, "total_revenue": 0.0, "revenue": 0.0, "cogs": 0.0, "fees": 0.0}
    )

    for o, order_ts, window_tps, weights in contributions:
        gross = to_float(o.get("gross"))
        net = to_float(o.get("net"))
        cogs = to_float(o.get("cogs"))
        fees = to_float(o.get("fees"))

        order_day = order_ts.date()
        for tp, w in zip(window_tps, weights, strict=True):
            if date_basis == "conversion":
                d = order_day
            else:
                d = tp["_ts"].date()
            if d < start_d or d > end_d:
                continue
            key = d.isoformat()
            bucket = by_day[key]
            bucket["orders"] += w
            bucket["total_revenue"] += gross * w
            bucket["revenue"] += net * w
            bucket["cogs"] += cogs * w
            bucket["fees"] += fees * w

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
    return rows_out


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

    date_basis = _normalize_basis(date_basis)
    start_d = date.fromisoformat(start_date)
    end_d = date.fromisoformat(end_date)
    orders_end_d = end_d if date_basis == "conversion" else (end_d + timedelta(days=lookback_days))

    spend_rows = query(
        db_path,
        "SELECT DISTINCT platform, account_id, campaign_id, adset_id, ad_id FROM spend;",
    ).rows
    account_map = _build_account_map(spend_rows)

    orders, tps_by_customer, dts_by_customer = _load_orders_and_touchpoints(
        db_path, start_d=start_d, lookback_days=lookback_days, orders_end_d=orders_end_d
    )
    contributions, unattributed_orders = _attributed(
        orders, tps_by_customer, dts_by_customer, model=model, lookback=timedelta(days=lookback_days)
    )
    out_rows = _aggregate_dim_rows(
        contributions, account_map=account_map, value_type=value_type,
        date_basis=date_basis, start_d=start_d, end_d=end_d,
    )

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
    date_basis = _normalize_basis(date_basis)
    start_d = date.fromisoformat(start_date)
    end_d = date.fromisoformat(end_date)
    orders_end_d = end_d if date_basis == "conversion" else (end_d + timedelta(days=lookback_days))

    orders, tps_by_customer, dts_by_customer = _load_orders_and_touchpoints(
        db_path, start_d=start_d, lookback_days=lookback_days, orders_end_d=orders_end_d
    )
    contributions, unattributed_orders = _attributed(
        orders, tps_by_customer, dts_by_customer, model=model, lookback=timedelta(days=lookback_days)
    )
    rows_out = _aggregate_day_rows(contributions, date_basis=date_basis, start_d=start_d, end_d=end_d)

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


def attribution_run_and_day_totals(
    db_path: str,
    *,
    model: str,
    start_date: str,
    end_date: str,
    lookback_days: int,
    conversion_type: str,
    value_type: str,
    date_basis: str = "conversion",
) -> dict[str, dict[str, object]]:
    """Compute the dimension breakdown and the per-day series in a SINGLE pass.

    Equivalent to calling ``attribution_run`` and ``attribution_day_totals``
    separately, but loads orders/touchpoints and runs the windowing pass only
    once — used by the report builder, which needs both.
    """
    if conversion_type.lower() != "purchase":
        raise ValueError("dummy dataset only supports conversion_type=Purchase")

    date_basis = _normalize_basis(date_basis)
    start_d = date.fromisoformat(start_date)
    end_d = date.fromisoformat(end_date)
    orders_end_d = end_d if date_basis == "conversion" else (end_d + timedelta(days=lookback_days))

    spend_rows = query(
        db_path,
        "SELECT DISTINCT platform, account_id, campaign_id, adset_id, ad_id FROM spend;",
    ).rows
    account_map = _build_account_map(spend_rows)

    orders, tps_by_customer, dts_by_customer = _load_orders_and_touchpoints(
        db_path, start_d=start_d, lookback_days=lookback_days, orders_end_d=orders_end_d
    )
    contributions, unattributed_orders = _attributed(
        orders, tps_by_customer, dts_by_customer, model=model, lookback=timedelta(days=lookback_days)
    )

    run_rows = _aggregate_dim_rows(
        contributions, account_map=account_map, value_type=value_type,
        date_basis=date_basis, start_d=start_d, end_d=end_d,
    )
    day_rows = _aggregate_day_rows(contributions, date_basis=date_basis, start_d=start_d, end_d=end_d)

    base = {
        "model": model,
        "start_date": start_date,
        "end_date": end_date,
        "lookback_days": lookback_days,
        "date_basis": date_basis,
        "conversion_type": conversion_type,
        "unattributed_orders": int(unattributed_orders),
    }
    return {
        "run": {**base, "value_type": value_type, "rows": run_rows},
        "day_totals": {**base, "rows": day_rows},
    }
