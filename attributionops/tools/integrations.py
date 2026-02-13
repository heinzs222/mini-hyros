from __future__ import annotations

from attributionops.db import query
from attributionops.util import parse_iso_ts


def integrations_status(db_path: str) -> dict[str, object]:
    # Local-only stub: infer "health" from presence of tables + last timestamps.
    tables = query(
        db_path,
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;",
    ).rows
    table_names = [t["name"] for t in tables]

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

    return {
        "connected": True,
        "mode": "local_dummy",
        "warehouse": {
            "type": "sqlite",
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
        },
        "notes": [
            "Local-only dummy integrations status (no external platform connections).",
        ],
    }

