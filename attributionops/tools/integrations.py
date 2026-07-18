from __future__ import annotations

from attributionops.db import is_postgres, query
from attributionops.dbmeta import list_tables
from attributionops.util import parse_iso_ts, to_float, to_int


def integrations_status(db_path: str) -> dict[str, object]:
    # Local-only stub: infer "health" from presence of tables + last timestamps.
    table_names = list_tables(db_path)

    def _max_ts(table: str, col: str) -> str | None:
        if table not in table_names:
            return None
        rows = query(db_path, f"SELECT MAX({col}) AS max_ts FROM {table};").rows
        max_ts = (rows[0] or {}).get("max_ts")
        if not max_ts:
            return None
        _ = parse_iso_ts(str(max_ts))
        return str(max_ts)

    def _max_date(table: str, col: str) -> str | None:
        if table not in table_names:
            return None
        rows = query(db_path, f"SELECT MAX({col}) AS max_date FROM {table};").rows
        max_date = (rows[0] or {}).get("max_date")
        return str(max_date) if max_date else None

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
            "type": "postgres" if is_postgres() else "sqlite",
            "db_path": db_path,
            "tables": table_names,
        },
        "tracking": {
            "sessions_last_ts": _max_ts("sessions", "ts"),
            "touchpoints_last_ts": _max_ts("touchpoints", "ts"),
            "orders_last_ts": _max_ts("orders", "ts"),
            "conversions_last_ts": _max_ts("conversions", "ts"),
        },
        "ads": {
            "spend_last_date": _max_date("spend", "date"),
            "reported_value_last_date": _max_date("reported_value", "date"),
            "platforms": _spend_platforms(),
        },
        "notes": [
            "Local warehouse status based on available SQLite tables and timestamps.",
        ],
    }
