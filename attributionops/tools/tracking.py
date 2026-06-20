from __future__ import annotations

from attributionops.db import query


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

    # orders_with_source = orders that have at least one touchpoint for that
    # customer within the lookback window before the order timestamp. This was
    # previously a full touchpoints load + O(orders × touches) Python loop with
    # repeated timestamp parsing; SQLite can do it directly with an EXISTS
    # subquery backed by the touchpoints(customer_key, ts) index. The ts columns
    # are ISO-8601 strings, so strftime reproduces the exact stored format for a
    # sargable lexical comparison.
    orders_with_source = query(
        db_path,
        """
        SELECT COUNT(*) AS n
        FROM orders o
        WHERE COALESCE(o.customer_key, '') <> ''
          AND EXISTS (
            SELECT 1 FROM touchpoints t
            WHERE t.customer_key = o.customer_key
              AND t.ts <= o.ts
              AND t.ts >= strftime('%Y-%m-%dT%H:%M:%SZ', o.ts, :lookback)
          );
        """,
        {"lookback": f"-{int(lookback_days_for_order_source)} days"},
    ).rows[0]["n"]

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

