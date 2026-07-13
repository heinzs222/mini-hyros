from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from datetime import timedelta
from typing import Any

from attributionops.db import query
from attributionops.tools.campaign_filter import excluded_campaign_keys, is_excluded
from attributionops.util import (
    exp_decay_weight,
    local_day_bounds_utc,
    local_day_start_utc,
    normalize_campaign_key,
    report_timezone,
    to_float,
    try_parse_iso_ts,
)


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
        # Normalize the campaign/adset/ad segments so spend's numeric/braced ids
        # and touchpoints' slug/display variants resolve to the same account.
        key = (
            str(r.get("platform") or ""),
            normalize_campaign_key(r.get("campaign_id")),
            normalize_campaign_key(r.get("adset_id")),
            normalize_campaign_key(r.get("ad_id")),
        )
        acct = str(r.get("account_id") or "")
        if key not in mapping and acct:
            mapping[key] = acct
    return mapping


def _build_excluded(db_path: str) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
    """Return (raw, normalized) excluded-campaign key sets.

    ``campaign_settings`` stores the campaign_id exactly as it appears in the
    spend feed (typically the numeric platform id). Touchpoints, however, carry
    slug/braced/display variants, so we also keep a normalized set and check both
    to catch excluded campaigns regardless of which representation a touchpoint
    stored.
    """
    raw = excluded_campaign_keys(db_path)
    norm = {(p, normalize_campaign_key(c)) for (p, c) in raw}
    return raw, norm


def _campaign_excluded(
    excluded_raw: set[tuple[str, str]],
    excluded_norm: set[tuple[str, str]],
    platform: str | None,
    campaign_id: str | None,
) -> bool:
    if not excluded_raw:
        return False
    if is_excluded(excluded_raw, platform, campaign_id):
        return True
    return (str(platform or "").lower(), normalize_campaign_key(campaign_id)) in excluded_norm


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
            tp_ts = tp.get("_ts") or try_parse_iso_ts(str(tp.get("ts") or ""))
            if tp_ts is None:
                # Unparseable ts: treat as zero-delay rather than crash.
                weights.append(exp_decay_weight(0.0, half_life_days=half_life_days))
                continue
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
        # Normalize so the weights always sum to 1.0 (the raw form sums to 0.8
        # for n==2, silently dropping 20% of every 2-touch order's value).
        s = sum(weights) or 1.0
        return [w / s for w in weights]

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


def _table_columns(db_path: str, table: str) -> set[str]:
    try:
        return {str(r.get("name") or "") for r in query(db_path, f"PRAGMA table_info({table})").rows}
    except Exception:
        return set()


def _identity_keys(row: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    customer_key = str(row.get("customer_key") or "").strip()
    session_id = str(row.get("session_id") or "").strip()
    visitor_id = str(row.get("visitor_id") or "").strip()
    if customer_key:
        keys.append(f"customer:{customer_key}")
    if session_id:
        keys.append(f"session:{session_id}")
    if visitor_id:
        keys.append(f"visitor:{visitor_id}")
    return keys


def _has_attribution_signal(row: dict[str, Any]) -> bool:
    channel = str(row.get("channel") or "").strip().lower()
    if channel and channel not in {"organic", "direct"}:
        return True
    return any(
        str(row.get(k) or "").strip()
        for k in (
            "platform",
            "campaign_id",
            "adset_id",
            "ad_id",
            "creative_id",
            "gclid",
            "fbclid",
            "ttclid",
        )
    )


def _touch_from_order(o: dict[str, Any], order_ts: datetime) -> dict[str, Any] | None:
    if not _has_attribution_signal(o):
        return None
    return {
        "ts": str(o.get("ts") or ""),
        "_ts": order_ts,
        "_rowid": f"order:{o.get('order_id') or ''}",
        "channel": str(o.get("channel") or ""),
        "platform": str(o.get("platform") or ""),
        "campaign_id": str(o.get("campaign_id") or ""),
        "adset_id": str(o.get("adset_id") or ""),
        "ad_id": str(o.get("ad_id") or ""),
        "creative_id": str(o.get("creative_id") or ""),
        "gclid": str(o.get("gclid") or ""),
        "fbclid": str(o.get("fbclid") or ""),
        "ttclid": str(o.get("ttclid") or ""),
        "customer_key": str(o.get("customer_key") or ""),
        "session_id": str(o.get("session_id") or ""),
        "visitor_id": str(o.get("visitor_id") or ""),
    }


def _load_orders_and_touchpoints(
    db_path: str,
    *,
    start_d: date,
    lookback_days: int,
    orders_end_d: date,
    excluded_raw: set[tuple[str, str]] | None = None,
    excluded_norm: set[tuple[str, str]] | None = None,
) -> tuple[
    list[dict[str, Any]],
    dict[str, list[dict[str, Any]]],
    dict[str, list[datetime]],
    dict[str, list[dict[str, Any]]],
]:
    # Orders within [start_d, orders_end_d] (inclusive), bounded by the same
    # reporting-timezone day boundaries the blended report queries use
    # (report.py via local_day_bounds_utc). Windowing by RAW UTC day here instead
    # would drop evening orders whose UTC timestamp rolls onto the next day for a
    # UTC-offset store — so the dashboard would count a sale but attribution would
    # not, yielding $0 attributed revenue on single-day views. Comparisons stay
    # sargable: the bounds are ISO-8601 instants (`ts >= '...T04:00:00Z'`).
    start_utc, orders_end_excl = local_day_bounds_utc(
        start_d.isoformat(), orders_end_d.isoformat()
    )
    order_cols = _table_columns(db_path, "orders")
    optional_order_cols = [
        "session_id",
        "visitor_id",
        "channel",
        "platform",
        "campaign_id",
        "adset_id",
        "ad_id",
        "creative_id",
        "gclid",
        "fbclid",
        "ttclid",
        "sale_group_id",
        "is_recurring",
    ]
    select_cols = [
        "order_id",
        "ts",
        "gross",
        "net",
        "refunds",
        "chargebacks",
        "cogs",
        "fees",
        "customer_key",
    ]
    for col in optional_order_cols:
        if col in order_cols:
            select_cols.append(col)
        else:
            select_cols.append(f"'' AS {col}")

    orders = query(
        db_path,
        f"""
        SELECT {", ".join(select_cols)}
        FROM orders
        WHERE ts >= :start AND ts < :end_excl
        """,
        {"start": start_utc, "end_excl": orders_end_excl},
    ).rows

    conversion_cols = _table_columns(db_path, "conversions")
    if orders and conversion_cols:
        conversion_select = ["order_id", "ts", "customer_key"]
        for col in ("session_id", "visitor_id"):
            if col in conversion_cols:
                conversion_select.append(col)
            else:
                conversion_select.append(f"'' AS {col}")

        conversions = query(
            db_path,
            f"""
            SELECT {", ".join(conversion_select)}
            FROM conversions
            WHERE COALESCE(order_id, '') != ''
              AND ts >= :start
              AND ts < :end_excl
            ORDER BY ts DESC
            """,
            {"start": start_utc, "end_excl": orders_end_excl},
        ).rows
        conversions_by_order: dict[str, dict[str, Any]] = {}
        for conv in conversions:
            order_id = str(conv.get("order_id") or "")
            if order_id and order_id not in conversions_by_order:
                conversions_by_order[order_id] = conv

        for order in orders:
            conv = conversions_by_order.get(str(order.get("order_id") or ""))
            if not conv:
                continue
            order["_conversion_ts"] = str(conv.get("ts") or "")
            for col in ("customer_key", "session_id", "visitor_id"):
                if not str(order.get(col) or "").strip() and str(conv.get(col) or "").strip():
                    order[col] = str(conv.get(col) or "").strip()

    # Touchpoints only need to span [start_d - lookback, orders_end_d]. They are
    # grouped by multiple identities because browser-side purchases can be
    # anonymous at checkout time but still carry the same session/visitor id as
    # prior ad clicks. This mirrors HYROS-style stitching more closely than
    # requiring customer_key on every order.
    win_start = local_day_start_utc(start_d - timedelta(days=lookback_days))
    touchpoint_cols = _table_columns(db_path, "touchpoints")
    touchpoint_visitor_expr = (
        "COALESCE(NULLIF(t.visitor_id, ''), s.visitor_id, '') AS visitor_id"
        if "visitor_id" in touchpoint_cols
        else "COALESCE(s.visitor_id, '') AS visitor_id"
    )
    touchpoints = query(
        db_path,
        f"""
        SELECT t.rowid AS _rowid, t.ts, t.channel, t.platform, t.campaign_id, t.adset_id,
               t.ad_id, t.creative_id, t.gclid, t.fbclid, t.ttclid,
               COALESCE(NULLIF(t.customer_key, ''), s.customer_key, '') AS customer_key,
               t.session_id,
               {touchpoint_visitor_expr}
        FROM touchpoints t
        LEFT JOIN (
            SELECT session_id,
                   MAX(NULLIF(visitor_id, '')) AS visitor_id,
                   MAX(NULLIF(customer_key, '')) AS customer_key
            FROM sessions
            WHERE COALESCE(session_id, '') != ''
            GROUP BY session_id
        ) s ON s.session_id = t.session_id
        WHERE t.ts >= :win_start AND t.ts < :end_excl;
        """,
        {"win_start": win_start, "end_excl": orders_end_excl},
    ).rows

    excluded_raw = excluded_raw or set()
    excluded_norm = excluded_norm or set()

    tps_by_identity: dict[str, list[dict[str, Any]]] = defaultdict(list)
    tps_by_exact_ts: dict[str, list[dict[str, Any]]] = defaultdict(list)
    skipped_bad_ts = 0
    for tp in touchpoints:
        parsed = try_parse_iso_ts(str(tp.get("ts") or ""))
        if parsed is None:
            # One unparseable touchpoint ts must never crash the whole report.
            skipped_bad_ts += 1
            continue
        # Read-time exclusion: touchpoints for a tracked=0 campaign are dropped so
        # that campaign's revenue becomes unattributed rather than attributed.
        if _campaign_excluded(excluded_raw, excluded_norm, tp.get("platform"), tp.get("campaign_id")):
            continue
        tp["_ts"] = parsed  # parse exactly once
        tps_by_exact_ts[str(tp.get("ts") or "")].append(tp)
        for key in _identity_keys(tp):
            tps_by_identity[key].append(tp)

    dts_by_identity: dict[str, list[datetime]] = {}
    for key, tps in tps_by_identity.items():
        tps.sort(key=lambda r: (r["_ts"], str(r.get("_rowid") or "")))
        dts_by_identity[key] = [t["_ts"] for t in tps]

    return orders, tps_by_identity, dts_by_identity, tps_by_exact_ts


def _attributed(
    orders: list[dict[str, Any]],
    tps_by_identity: dict[str, list[dict[str, Any]]],
    dts_by_identity: dict[str, list[datetime]],
    tps_by_exact_ts: dict[str, list[dict[str, Any]]],
    *,
    model: str,
    lookback: timedelta,
    excluded_raw: set[tuple[str, str]] | None = None,
    excluded_norm: set[tuple[str, str]] | None = None,
) -> tuple[list[tuple[dict[str, Any], datetime, list[dict[str, Any]], list[float]]], int]:
    """Single pass: for each attributable order return (order, order_ts, window_tps, weights)."""
    excluded_raw = excluded_raw or set()
    excluded_norm = excluded_norm or set()
    contributions: list[tuple[dict[str, Any], datetime, list[dict[str, Any]], list[float]]] = []
    unattributed = 0
    for o in orders:
        order_ts = try_parse_iso_ts(str(o.get("ts") or ""))
        if order_ts is None:
            # Unparseable order ts: cannot window it — leave unattributed instead
            # of crashing the whole report.
            unattributed += 1
            continue
        window_start = order_ts - lookback
        window_tps: list[dict[str, Any]] = []
        seen: set[str] = set()

        def add_touch(tp: dict[str, Any]) -> None:
            marker = str(tp.get("_rowid") or f"{tp.get('ts')}|{tp.get('session_id')}|{tp.get('platform')}")
            if marker in seen:
                return
            if not _has_attribution_signal(tp):
                return
            seen.add(marker)
            window_tps.append(tp)

        for key in _identity_keys(o):
            dts = dts_by_identity.get(key)
            if not dts:
                continue
            lo = bisect_left(dts, window_start)
            hi = bisect_right(dts, order_ts)
            for tp in tps_by_identity[key][lo:hi]:
                add_touch(tp)

        # Legacy rows did not store session_id on orders. Pixel purchases write a
        # touchpoint with the exact same timestamp as the order. Accept such a
        # touch when the order shares an identity with it, OR when the order is
        # fully anonymous (no session/visitor/customer at all) — the legacy pixel
        # case where the exact-timestamp pairing is the only available signal.
        # When we recover a touch this way, only expand within its SESSION window
        # (a single browsing session), never a stranger's customer/visitor history.
        if not window_tps:
            order_identities = set(_identity_keys(o))
            exact_timestamps = [str(o.get("ts") or "")]
            conversion_ts_raw = str(o.get("_conversion_ts") or "")
            if conversion_ts_raw and conversion_ts_raw not in exact_timestamps:
                exact_timestamps.append(conversion_ts_raw)
            for exact_ts in exact_timestamps:
                for exact_tp in tps_by_exact_ts.get(exact_ts, []):
                    tp_identities = set(_identity_keys(exact_tp))
                    # If the order has its own identity, require overlap (prevents
                    # grabbing an unrelated visitor's same-second touch).
                    if order_identities and not order_identities.intersection(tp_identities):
                        continue
                    add_touch(exact_tp)
                    # Recover earlier touches in the SAME session only.
                    sess = str(exact_tp.get("session_id") or "").strip()
                    skey = f"session:{sess}" if sess else ""
                    dts = dts_by_identity.get(skey) if skey else None
                    if dts:
                        lo = bisect_left(dts, window_start)
                        hi = bisect_right(dts, order_ts)
                        for tp in tps_by_identity[skey][lo:hi]:
                            add_touch(tp)

        if not window_tps and str(o.get("_conversion_ts") or ""):
            conversion_ts = try_parse_iso_ts(str(o.get("_conversion_ts") or ""))
            if conversion_ts is not None:
                conversion_window_start = conversion_ts - lookback
                for key in _identity_keys(o):
                    dts = dts_by_identity.get(key)
                    if not dts:
                        continue
                    lo = bisect_left(dts, conversion_window_start)
                    hi = bisect_right(dts, conversion_ts)
                    for tp in tps_by_identity[key][lo:hi]:
                        add_touch(tp)

        if not window_tps:
            pseudo = _touch_from_order(o, order_ts)
            if pseudo is not None and not _campaign_excluded(
                excluded_raw, excluded_norm, pseudo.get("platform"), pseudo.get("campaign_id")
            ):
                add_touch(pseudo)

        if not window_tps:
            unattributed += 1
            continue
        window_tps.sort(key=lambda r: (r["_ts"], str(r.get("_rowid") or "")))
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

            # Normalize the join segments the same way _build_account_map did so
            # slug/braced/numeric campaign-id variants resolve to their account.
            account_id = account_map.get(
                (
                    platform,
                    normalize_campaign_key(campaign_id),
                    normalize_campaign_key(adset_id),
                    normalize_campaign_key(ad_id),
                ),
                "",
            )

            if date_basis == "click":
                # Compare the click's LOCAL-zone day against the (local) report
                # window so click-date attribution aligns with the order window.
                tp_d = tp["_ts"].astimezone(report_timezone()).date()
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
    sale_groups_by_day: dict[str, set[str]] = defaultdict(set)

    tz = report_timezone()
    for o, order_ts, window_tps, weights in contributions:
        gross = to_float(o.get("gross"))
        net = to_float(o.get("net"))
        cogs = to_float(o.get("cogs"))
        fees = to_float(o.get("fees"))
        sale_group_id = str(o.get("sale_group_id") or o.get("order_id") or "")

        # Bucket by LOCAL-zone day so the daily series aligns with the report's
        # (local) day boundaries rather than raw UTC days.
        order_day = order_ts.astimezone(tz).date()
        for tp, w in zip(window_tps, weights, strict=True):
            if date_basis == "conversion":
                d = order_day
            else:
                d = tp["_ts"].astimezone(tz).date()
            if d < start_d or d > end_d:
                continue
            key = d.isoformat()
            bucket = by_day[key]
            bucket["orders"] += w
            bucket["total_revenue"] += gross * w
            bucket["revenue"] += net * w
            bucket["cogs"] += cogs * w
            bucket["fees"] += fees * w
            if w > 0 and sale_group_id:
                sale_groups_by_day[key].add(sale_group_id)

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
                "sale_groups": len(sale_groups_by_day.get(d, set())),
            }
        )
    return rows_out


def _attributed_sale_group_count(
    contributions: list[tuple[dict[str, Any], datetime, list[dict[str, Any]], list[float]]],
    *,
    date_basis: str,
    start_d: date,
    end_d: date,
) -> int:
    groups: set[str] = set()
    tz = report_timezone()
    for order, order_ts, touchpoints, weights in contributions:
        if date_basis == "conversion":
            in_range = start_d <= order_ts.astimezone(tz).date() <= end_d
        else:
            in_range = any(
                weight > 0 and start_d <= tp["_ts"].astimezone(tz).date() <= end_d
                for tp, weight in zip(touchpoints, weights, strict=True)
            )
        if in_range:
            group_id = str(order.get("sale_group_id") or order.get("order_id") or "")
            if group_id:
                groups.add(group_id)
    return len(groups)


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
    date_basis = _normalize_basis(date_basis)
    if conversion_type.lower() != "purchase":
        # Non-purchase conversions are unsupported here; return empty rows rather
        # than raising (a raise would 500 the whole report endpoint).
        return {
            "model": model,
            "start_date": start_date,
            "end_date": end_date,
            "lookback_days": lookback_days,
            "date_basis": date_basis,
            "conversion_type": conversion_type,
            "value_type": value_type,
            "rows": [],
            "unattributed_orders": 0,
            "notes": [
                f"conversion_type={conversion_type!r} is not supported; only 'purchase' is attributed. Returning empty rows.",
            ],
        }

    start_d = date.fromisoformat(start_date)
    end_d = date.fromisoformat(end_date)
    orders_end_d = end_d if date_basis == "conversion" else (end_d + timedelta(days=lookback_days))

    spend_rows = query(
        db_path,
        "SELECT DISTINCT platform, account_id, campaign_id, adset_id, ad_id FROM spend;",
    ).rows
    account_map = _build_account_map(spend_rows)
    excluded_raw, excluded_norm = _build_excluded(db_path)

    orders, tps_by_identity, dts_by_identity, tps_by_exact_ts = _load_orders_and_touchpoints(
        db_path, start_d=start_d, lookback_days=lookback_days, orders_end_d=orders_end_d,
        excluded_raw=excluded_raw, excluded_norm=excluded_norm,
    )
    contributions, unattributed_orders = _attributed(
        orders, tps_by_identity, dts_by_identity, tps_by_exact_ts, model=model,
        lookback=timedelta(days=lookback_days), excluded_raw=excluded_raw, excluded_norm=excluded_norm,
    )
    out_rows = _aggregate_dim_rows(
        contributions, account_map=account_map, value_type=value_type,
        date_basis=date_basis, start_d=start_d, end_d=end_d,
    )
    sale_groups = _attributed_sale_group_count(
        contributions, date_basis=date_basis, start_d=start_d, end_d=end_d
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
        "sale_groups": sale_groups,
        "unattributed_orders": int(unattributed_orders),
        "notes": [
            "Local attribution computed from touchpoints+orders by customer_key, session_id, visitor_id, then direct order source.",
            "Orders without any source touchpoint in lookback window are excluded from attributed rows.",
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
    excluded_raw, excluded_norm = _build_excluded(db_path)

    orders, tps_by_identity, dts_by_identity, tps_by_exact_ts = _load_orders_and_touchpoints(
        db_path, start_d=start_d, lookback_days=lookback_days, orders_end_d=orders_end_d,
        excluded_raw=excluded_raw, excluded_norm=excluded_norm,
    )
    contributions, unattributed_orders = _attributed(
        orders, tps_by_identity, dts_by_identity, tps_by_exact_ts, model=model,
        lookback=timedelta(days=lookback_days), excluded_raw=excluded_raw, excluded_norm=excluded_norm,
    )
    rows_out = _aggregate_day_rows(contributions, date_basis=date_basis, start_d=start_d, end_d=end_d)
    sale_groups = _attributed_sale_group_count(
        contributions, date_basis=date_basis, start_d=start_d, end_d=end_d
    )

    return {
        "model": model,
        "start_date": start_date,
        "end_date": end_date,
        "lookback_days": lookback_days,
        "date_basis": date_basis,
        "conversion_type": conversion_type,
        "rows": rows_out,
        "sale_groups": sale_groups,
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
    date_basis = _normalize_basis(date_basis)
    if conversion_type.lower() != "purchase":
        # Return empty (but well-shaped) rows instead of raising so the report
        # endpoint can render an empty/400 state rather than 500.
        base = {
            "model": model,
            "start_date": start_date,
            "end_date": end_date,
            "lookback_days": lookback_days,
            "date_basis": date_basis,
            "conversion_type": conversion_type,
            "unattributed_orders": 0,
        }
        return {
            "run": {**base, "value_type": value_type, "rows": []},
            "day_totals": {**base, "rows": []},
        }

    start_d = date.fromisoformat(start_date)
    end_d = date.fromisoformat(end_date)
    orders_end_d = end_d if date_basis == "conversion" else (end_d + timedelta(days=lookback_days))

    spend_rows = query(
        db_path,
        "SELECT DISTINCT platform, account_id, campaign_id, adset_id, ad_id FROM spend;",
    ).rows
    account_map = _build_account_map(spend_rows)
    excluded_raw, excluded_norm = _build_excluded(db_path)

    orders, tps_by_identity, dts_by_identity, tps_by_exact_ts = _load_orders_and_touchpoints(
        db_path, start_d=start_d, lookback_days=lookback_days, orders_end_d=orders_end_d,
        excluded_raw=excluded_raw, excluded_norm=excluded_norm,
    )
    contributions, unattributed_orders = _attributed(
        orders, tps_by_identity, dts_by_identity, tps_by_exact_ts, model=model,
        lookback=timedelta(days=lookback_days), excluded_raw=excluded_raw, excluded_norm=excluded_norm,
    )

    run_rows = _aggregate_dim_rows(
        contributions, account_map=account_map, value_type=value_type,
        date_basis=date_basis, start_d=start_d, end_d=end_d,
    )
    day_rows = _aggregate_day_rows(contributions, date_basis=date_basis, start_d=start_d, end_d=end_d)
    sale_groups = _attributed_sale_group_count(
        contributions, date_basis=date_basis, start_d=start_d, end_d=end_d
    )

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
        "run": {**base, "value_type": value_type, "rows": run_rows, "sale_groups": sale_groups},
        "day_totals": {**base, "rows": day_rows, "sale_groups": sale_groups},
    }
