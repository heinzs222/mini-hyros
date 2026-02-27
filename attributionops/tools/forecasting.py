from __future__ import annotations

from collections import defaultdict
from datetime import timedelta

from attributionops.db import query
from attributionops.util import parse_iso_ts, safe_div, to_float


def forecasting_run(
    db_path: str,
    *,
    start_date: str,
    end_date: str,
    cohort_window: int,
    method: str,
) -> dict[str, object]:
    # Minimal local stub: cohort customers by first order date, compute net revenue over cohort_window days.
    method = method.lower().strip()
    if method not in ("simple", "historical"):
        raise ValueError("method must be one of: simple, historical (local stub)")

    orders = query(
        db_path,
        """
        SELECT customer_key, ts, net
        FROM orders
        WHERE date(substr(ts,1,10)) BETWEEN :start_date AND :end_date
          AND COALESCE(customer_key,'') <> '';
        """,
        {"start_date": start_date, "end_date": end_date},
    ).rows

    orders_by_customer = defaultdict(list)
    for o in orders:
        orders_by_customer[str(o["customer_key"])].append(o)

    cohorts = defaultdict(list)
    for rows in orders_by_customer.values():
        rows.sort(key=lambda r: r["ts"])
        first_ts = parse_iso_ts(str(rows[0]["ts"]))
        cohort_key = first_ts.date().isoformat()
        cohorts[cohort_key].append((first_ts, rows))

    cohort_rows = []
    for cohort_date, entries in sorted(cohorts.items()):
        total_net = 0.0
        customer_count = len(entries)
        for first_ts, rows in entries:
            window_end = first_ts + timedelta(days=cohort_window)
            for r in rows:
                ts = parse_iso_ts(str(r["ts"]))
                if ts <= window_end:
                    total_net += to_float(r.get("net"))

        avg_ltv = safe_div(total_net, float(customer_count))
        cohort_rows.append(
            {
                "cohort_date": cohort_date,
                "customers": customer_count,
                "net_revenue_sum": float(f"{total_net:.2f}"),
                "avg_net_revenue_per_customer": float(f"{(avg_ltv or 0.0):.2f}") if avg_ltv is not None else None,
                "cohort_window_days": cohort_window,
            }
        )

    return {
        "start_date": start_date,
        "end_date": end_date,
        "cohort_window": cohort_window,
        "method": method,
        "rows": cohort_rows,
        "notes": ["Local stub: computes only net-revenue LTV over the cohort window from orders table."],
    }

