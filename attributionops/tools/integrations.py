from __future__ import annotations

from attributionops.db import db_label, is_postgres_dsn, query
from attributionops.util import parse_iso_ts, to_float, to_int


def integrations_status(db_path: str) -> dict[str, object]:
    # Infer health from presence of warehouse tables and last timestamps.
    tables = query(
        db_path,
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;",
    ).rows
    table_names = [t["name"] for t in tables]

    # These six MAX() lookups were six separate statements. Against a warehouse
    # on the other end of a network that is six round trips on the critical path
    # of every report, for six scalars — one UNION ALL fetches them together.
    wanted = (
        ("sessions_last_ts", "sessions", "ts"),
        ("touchpoints_last_ts", "touchpoints", "ts"),
        ("orders_last_ts", "orders", "ts"),
        ("conversions_last_ts", "conversions", "ts"),
        ("spend_last_date", "spend", "date"),
        ("reported_value_last_date", "reported_value", "date"),
    )
    available = [item for item in wanted if item[1] in table_names]
    maxima: dict[str, str | None] = {label: None for label, _, _ in wanted}
    if available:
        union = " UNION ALL ".join(
            f"SELECT '{label}' AS label, MAX({col}) AS value FROM {table}"
            for label, table, col in available
        )
        for row in query(db_path, union).rows:
            value = row.get("value")
            maxima[str(row.get("label") or "")] = str(value) if value else None

    def _max_ts(label: str) -> str | None:
        value = maxima.get(label)
        if not value:
            return None
        _ = parse_iso_ts(value)
        return value

    def _spend_platforms() -> dict[str, dict[str, object]]:
        if "spend" not in table_names:
            return {}
        rows = query(
            db_path,
            """
            SELECT LOWER(COALESCE(platform, 'unknown')) AS platform,
                   MAX(date) AS last_date,
                   COUNT(*) AS rows,
                   SUM(CAST(COALESCE(cost, '0') AS REAL)) AS cost
            FROM spend
            GROUP BY LOWER(COALESCE(platform, 'unknown'))
            ORDER BY platform
            """,
        ).rows
        return {
            str(row.get("platform") or "unknown"): {
                "last_date": str(row.get("last_date")) if row.get("last_date") else None,
                "rows": to_int(row.get("rows")),
                "cost": round(to_float(row.get("cost")), 2),
            }
            for row in rows
        }

    return {
        "connected": True,
        "mode": "local_warehouse",
        "warehouse": {
            "type": "postgres" if is_postgres_dsn(db_path) else "sqlite",
            "db_path": db_label(db_path),
            "tables": table_names,
        },
        "tracking": {
            "sessions_last_ts": _max_ts("sessions_last_ts"),
            "touchpoints_last_ts": _max_ts("touchpoints_last_ts"),
            "orders_last_ts": _max_ts("orders_last_ts"),
            "conversions_last_ts": _max_ts("conversions_last_ts"),
        },
        "ads": {
            "spend_last_date": maxima.get("spend_last_date"),
            "reported_value_last_date": maxima.get("reported_value_last_date"),
            "platforms": _spend_platforms(),
        },
        "notes": [
            "Warehouse status based on available database tables and timestamps.",
        ],
    }
