from __future__ import annotations

from attributionops.db import query
from attributionops.util import local_day_bounds_utc, session_identity_agg_exprs, table_columns


def tracking_health_check(
    db_path: str,
    *,
    lookback_days_for_order_source: int = 30,
    start_date: str = "",
    end_date: str = "",
) -> dict[str, object]:
    """Measure tracking coverage, optionally inside one report date range.

    The old report mixed selected-period revenue with lifetime tracking counts,
    which made the health percentage look unrelated to the report on screen.
    Supplying both dates scopes sessions, orders, source coverage, and freshness to
    the same local-day boundaries used by the report builder. Callers that omit
    dates retain the original lifetime behavior.
    """

    scoped = bool(start_date and end_date)
    params: dict[str, str] = {"lookback": f"-{int(lookback_days_for_order_source)} days"}
    session_where = ""
    order_where = ""
    touch_where = ""
    if scoped:
        start_utc, end_excl_utc = local_day_bounds_utc(start_date, end_date)
        params.update({"start_utc": start_utc, "end_excl_utc": end_excl_utc})
        session_where = "WHERE ts >= :start_utc AND ts < :end_excl_utc"
        order_where = "WHERE o.ts >= :start_utc AND o.ts < :end_excl_utc"
        # Include the source lookback before the first selected day, but never
        # scan touchpoints after the report window.
        touch_where = (
            "WHERE t.ts >= strftime('%Y-%m-%dT%H:%M:%SZ', :start_utc, :lookback) "
            "AND t.ts < :end_excl_utc"
        )

    sessions_total = query(
        db_path,
        f"SELECT COUNT(*) AS n FROM sessions {session_where};",
        params,
    ).rows[0]["n"]
    sessions_with_click_id = query(
        db_path,
        f"""
        SELECT COUNT(*) AS n
        FROM sessions
        {session_where}
        {"AND" if session_where else "WHERE"}
          (COALESCE(gclid,'') <> '' OR COALESCE(fbclid,'') <> '' OR COALESCE(ttclid,'') <> '');
        """,
        params,
    ).rows[0]["n"]

    orders_total = query(
        db_path,
        f"SELECT COUNT(*) AS n FROM orders o {order_where};",
        params,
    ).rows[0]["n"]

    order_cols = table_columns(db_path, "orders")
    conversion_cols = table_columns(db_path, "conversions")
    touchpoint_cols = table_columns(db_path, "touchpoints")
    session_cols = table_columns(db_path, "sessions")

    session_visitor_agg, session_customer_agg = session_identity_agg_exprs(session_cols)

    def order_expr(col: str) -> str:
        return f"COALESCE(o.{col}, '')" if col in order_cols else "''"

    def conversion_agg(col: str) -> str:
        return f"MAX(NULLIF({col}, '')) AS {col}" if col in conversion_cols else f"'' AS {col}"

    touchpoint_visitor_expr = (
        "COALESCE(NULLIF(t.visitor_id, ''), s.visitor_id, '') AS visitor_id"
        if "visitor_id" in touchpoint_cols
        else "COALESCE(s.visitor_id, '') AS visitor_id"
    )
    order_direct_source = " OR ".join(
        [
            f"LOWER({order_expr('channel')}) NOT IN ('', 'organic', 'direct')",
            f"{order_expr('platform')} <> ''",
            f"{order_expr('campaign_id')} <> ''",
            f"{order_expr('adset_id')} <> ''",
            f"{order_expr('ad_id')} <> ''",
            f"{order_expr('creative_id')} <> ''",
            f"{order_expr('gclid')} <> ''",
            f"{order_expr('fbclid')} <> ''",
            f"{order_expr('ttclid')} <> ''",
        ]
    )

    orders_with_source = query(
        db_path,
        f"""
        WITH conversion_identity AS (
            SELECT order_id,
                   MAX(NULLIF(customer_key, '')) AS customer_key,
                   {conversion_agg("session_id")},
                   {conversion_agg("visitor_id")}
            FROM conversions
            WHERE COALESCE(order_id, '') <> ''
            GROUP BY order_id
        ),
        order_identity AS (
            SELECT o.order_id, o.ts,
                   COALESCE(NULLIF(o.customer_key, ''), c.customer_key, '') AS customer_key,
                   COALESCE(NULLIF({order_expr("session_id")}, ''), c.session_id, '') AS session_id,
                   COALESCE(NULLIF({order_expr("visitor_id")}, ''), c.visitor_id, '') AS visitor_id,
                   ({order_direct_source}) AS has_direct_source
            FROM orders o
            LEFT JOIN conversion_identity c ON c.order_id = o.order_id
            {order_where}
        ),
        touch_identity AS (
            SELECT t.ts, t.channel, t.platform, t.campaign_id, t.adset_id, t.ad_id, t.creative_id,
                   t.gclid, t.fbclid, t.ttclid,
                   COALESCE(NULLIF(t.customer_key, ''), s.customer_key, '') AS customer_key,
                   t.session_id,
                   {touchpoint_visitor_expr}
            FROM touchpoints t
            LEFT JOIN (
                SELECT session_id,
                       {session_visitor_agg},
                       {session_customer_agg}
                FROM sessions
                WHERE COALESCE(session_id, '') <> ''
                GROUP BY session_id
            ) s ON s.session_id = t.session_id
            {touch_where}
        )
        SELECT COUNT(*) AS n
        FROM order_identity o
        WHERE o.has_direct_source
           OR EXISTS (
            SELECT 1 FROM touch_identity t
            WHERE (
                (COALESCE(o.customer_key, '') <> '' AND t.customer_key = o.customer_key)
                OR (COALESCE(o.session_id, '') <> '' AND t.session_id = o.session_id)
                OR (COALESCE(o.visitor_id, '') <> '' AND t.visitor_id = o.visitor_id)
              )
              AND t.ts <= o.ts
              AND t.ts >= strftime('%Y-%m-%dT%H:%M:%SZ', o.ts, :lookback)
              AND (
                LOWER(COALESCE(t.channel, '')) NOT IN ('', 'organic', 'direct')
                OR COALESCE(t.platform, '') <> ''
                OR COALESCE(t.campaign_id, '') <> ''
                OR COALESCE(t.adset_id, '') <> ''
                OR COALESCE(t.ad_id, '') <> ''
                OR COALESCE(t.creative_id, '') <> ''
                OR COALESCE(t.gclid, '') <> ''
                OR COALESCE(t.fbclid, '') <> ''
                OR COALESCE(t.ttclid, '') <> ''
              )
          );
        """,
        params,
    ).rows[0]["n"]

    last_session_ts = query(
        db_path, f"SELECT MAX(ts) AS max_ts FROM sessions {session_where};", params
    ).rows[0]["max_ts"]
    last_order_ts = query(
        db_path, f"SELECT MAX(ts) AS max_ts FROM orders o {order_where};", params
    ).rows[0]["max_ts"]
    last_touch_ts = query(
        db_path,
        f"SELECT MAX(t.ts) AS max_ts FROM touchpoints t {touch_where};",
        params,
    ).rows[0]["max_ts"]
    conversion_where = (
        "WHERE ts >= :start_utc AND ts < :end_excl_utc" if scoped else ""
    )
    last_conv_ts = query(
        db_path,
        f"SELECT MAX(ts) AS max_ts FROM conversions {conversion_where};",
        params,
    ).rows[0]["max_ts"]

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
                    "impact": f"{missing_source_rate:.0%} of orders have no paid source within {lookback_days_for_order_source}d.",
                    "fix": "Verify identity stitching, server-side touchpoint logging, and UTM/click-id capture.",
                }
            )

    status = "ok"
    if int(orders_total) == 0 or int(sessions_total) == 0 or gaps:
        status = "warn"

    return {
        "status": status,
        "mode": "local_warehouse",
        "scope": {
            "type": "date_range" if scoped else "lifetime",
            "start_date": start_date or None,
            "end_date": end_date or None,
        },
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
            "Coverage is scoped to the selected report range." if scoped else "Coverage is lifetime.",
        ],
    }
