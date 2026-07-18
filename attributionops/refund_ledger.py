"""Apply timestamped refunds and chargebacks to historical order snapshots."""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Any

from attributionops.db import connect
from attributionops.util import parse_iso_ts, to_float


_MONEY_EPSILON = 0.011


def _chunks(values: list[str], size: int = 400):
    for index in range(0, len(values), size):
        yield values[index : index + size]


def apply_refunds_as_of(
    db_path: str,
    orders: list[dict[str, Any]],
    end_exclusive_utc: str,
) -> list[dict[str, Any]]:
    """Return order copies with refunds limited to events before ``end``.

    ``orders.refunds`` and ``orders.chargebacks`` remain the current lifetime
    totals used by ingestion. Historical reports use ``refund_log`` only when
    that ledger is complete for a component. This preserves legacy/manual
    orders whose aggregate refund value has no timestamped events yet.
    """
    if not orders:
        return []

    order_ids = sorted(
        {str(order.get("order_id") or "").strip() for order in orders}
        - {""}
    )
    if not order_ids:
        return [dict(order) for order in orders]

    try:
        cutoff = parse_iso_ts(end_exclusive_utc)
    except (TypeError, ValueError):
        return [dict(order) for order in orders]

    events: dict[str, dict[str, list[tuple[Any, float]]]] = defaultdict(
        lambda: {"refund": [], "chargeback": []}
    )
    from attributionops.dbmeta import table_exists

    if not table_exists(db_path, "refund_log"):
        return [dict(order) for order in orders]
    try:
        with connect(db_path) as conn:
            for chunk in _chunks(order_ids):
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    f"""SELECT order_id, ts, type, amount
                        FROM refund_log
                        WHERE order_id IN ({placeholders})""",
                    chunk,
                ).fetchall()
                for row in rows:
                    try:
                        event_ts = parse_iso_ts(str(row[1] or ""))
                    except (TypeError, ValueError):
                        continue
                    kind = "chargeback" if str(row[2] or "").lower() == "chargeback" else "refund"
                    events[str(row[0] or "")][kind].append(
                        (event_ts, max(to_float(row[3]), 0.0))
                    )
    except Exception:
        return [dict(order) for order in orders]

    adjusted: list[dict[str, Any]] = []
    for original in orders:
        order = dict(original)
        order_events = events.get(str(order.get("order_id") or ""), {})
        scalar_refunds = max(to_float(order.get("refunds")), 0.0)
        scalar_chargebacks = max(to_float(order.get("chargebacks")), 0.0)
        refunds = scalar_refunds
        chargebacks = scalar_chargebacks
        ledger_applied = False

        for kind, scalar in (
            ("refund", scalar_refunds),
            ("chargeback", scalar_chargebacks),
        ):
            component_events = order_events.get(kind, [])
            lifetime = sum(amount for _, amount in component_events)
            if component_events and lifetime + _MONEY_EPSILON >= scalar:
                as_of = round(
                    sum(amount for event_ts, amount in component_events if event_ts < cutoff),
                    2,
                )
                if kind == "refund":
                    refunds = as_of
                else:
                    chargebacks = as_of
                ledger_applied = True

        if ledger_applied:
            gross = to_float(order.get("gross"))
            if gross <= 0:
                gross = (
                    to_float(order.get("net"))
                    + scalar_refunds
                    + scalar_chargebacks
                )
            order["refunds"] = round(refunds, 2)
            order["chargebacks"] = round(chargebacks, 2)
            order["net"] = round(max(gross - refunds - chargebacks, 0.0), 2)

        adjusted.append(order)

    return adjusted
