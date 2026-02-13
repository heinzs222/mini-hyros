from __future__ import annotations

from datetime import timedelta

from attributionops.db import query
from attributionops.util import parse_iso_ts


def tracking_health_check(db_path: str, *, lookback_days_for_order_source: int = 30) -> dict[str, object]:
    # Coverage stats
    sessions_total = query(db_path, "SELECT COUNT(*) AS n FROM sessions;").rows[0]["n"]
    sessions_with_click_id = query(
        db_path,
        """
        SELECT COUNT(*) AS n
        FROM sessions
        WHERE COALESCE(gclid,'') <> '' OR COALESCE(fbclid,'') <> '' OR COALESCE(ttclid,'') <> '';
        """,
    ).rows[0]["n"]

    orders_total = query(db_path, "SELECT COUNT(*) AS n FROM orders;").rows[0]["n"]

    # orders_with_source = orders that have at least one touchpoint for that customer within lookback window
    # before the order timestamp.
    rows = query(
        db_path,
        "SELECT order_id, ts, customer_key FROM orders WHERE COALESCE(customer_key,'') <> '';",
    ).rows

    # Preload touchpoints per customer (ts only) for speed.
    tp_rows = query(
        db_path,
        "SELECT customer_key, ts, channel, platform, campaign_id, adset_id, ad_id FROM touchpoints WHERE COALESCE(customer_key,'') <> '';",
    ).rows
    tps_by_customer: dict[str, list[dict[str, str]]] = {}
    for tp in tp_rows:
        ck = str(tp["customer_key"])
        tps_by_customer.setdefault(ck, []).append(tp)
    for ck in tps_by_customer:
        tps_by_customer[ck].sort(key=lambda r: r["ts"])

    lookback = timedelta(days=lookback_days_for_order_source)
    orders_with_source = 0
    for o in rows:
        ck = str(o["customer_key"])
        if ck not in tps_by_customer:
            continue
        order_ts = parse_iso_ts(str(o["ts"]))
        window_start = order_ts - lookback
        has_tp = False
        for tp in tps_by_customer[ck]:
            tp_ts = parse_iso_ts(str(tp["ts"]))
            if tp_ts < window_start:
                continue
            if tp_ts <= order_ts:
                has_tp = True
                break
        if has_tp:
            orders_with_source += 1

    # Freshness
    last_session_ts = query(db_path, "SELECT MAX(ts) AS max_ts FROM sessions;").rows[0]["max_ts"]
    last_order_ts = query(db_path, "SELECT MAX(ts) AS max_ts FROM orders;").rows[0]["max_ts"]
    last_touch_ts = query(db_path, "SELECT MAX(ts) AS max_ts FROM touchpoints;").rows[0]["max_ts"]
    last_conv_ts = query(db_path, "SELECT MAX(ts) AS max_ts FROM conversions;").rows[0]["max_ts"]

    # Gaps
    gaps: list[dict[str, str]] = []
    if int(sessions_total) > 0:
        missing_click_id_rate = 1.0 - (int(sessions_with_click_id) / int(sessions_total))
        if missing_click_id_rate > 0.25:
            gaps.append(
                {
                    "issue": "High session click-id loss",
                    "impact": f"{missing_click_id_rate:.0%} of sessions have no click_id (gclid/fbclid/ttclid).",
                    "fix": "Ensure click IDs are captured on landing and persisted (server-side + first-party cookie).",
                }
            )
    if int(orders_total) > 0:
        missing_source_rate = 1.0 - (int(orders_with_source) / int(orders_total))
        if missing_source_rate > 0.10:
            gaps.append(
                {
                    "issue": "Orders missing attributable source",
                    "impact": f"{missing_source_rate:.0%} of orders have no touchpoint within {lookback_days_for_order_source}d.",
                    "fix": "Verify identity stitching (customer_key), server-side touchpoint logging, and UTM governance.",
                }
            )

    status = "ok"
    if int(orders_total) == 0 or int(sessions_total) == 0 or gaps:
        status = "warn"

    return {
        "status": status,
        "mode": "local_warehouse",
        "coverage": {
            "orders_with_source": int(orders_with_source),
            "orders_total": int(orders_total),
            "sessions_with_click_id": int(sessions_with_click_id),
            "sessions_total": int(sessions_total),
        },
        "freshness": {
            "sessions_last_ts": str(last_session_ts) if last_session_ts else None,
            "touchpoints_last_ts": str(last_touch_ts) if last_touch_ts else None,
            "orders_last_ts": str(last_order_ts) if last_order_ts else None,
            "conversions_last_ts": str(last_conv_ts) if last_conv_ts else None,
        },
        "errors": [],
        "top_tracking_gaps": gaps,
        "notes": [
            "Tracking health computed from live local SQLite warehouse tables.",
        ],
    }

