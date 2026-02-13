from __future__ import annotations

from typing import Any

from attributionops.db import QueryResult, query


def warehouse_query(db_path: str, sql: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    result: QueryResult = query(db_path, sql, params)
    return {
        "rows": result.rows,
        "metadata": {
            "columns": result.columns,
            "row_count": result.row_count,
            "db_path": result.db_path,
            "sql": result.sql,
            "params": result.params,
        },
    }

