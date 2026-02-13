from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class QueryResult:
    rows: list[dict[str, Any]]
    columns: list[str]
    row_count: int
    sql: str
    params: dict[str, Any] | None
    db_path: str


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def query(db_path: str, sql: str, params: dict[str, Any] | None = None) -> QueryResult:
    if not Path(db_path).exists():
        raise FileNotFoundError(f"SQLite db not found: {db_path}")

    with connect(db_path) as conn:
        cur = conn.execute(sql, params or {})
        rows = [dict(r) for r in cur.fetchall()]
        columns = [d[0] for d in (cur.description or [])]
        return QueryResult(
            rows=rows,
            columns=columns,
            row_count=len(rows),
            sql=sql,
            params=params,
            db_path=db_path,
        )


def sql_rows(db_path: str, sql: str, params: Any = None) -> list[dict[str, Any]]:
    """Simple query returning just a list of row dicts. Accepts list or dict params."""
    if not Path(db_path).exists():
        raise FileNotFoundError(f"SQLite db not found: {db_path}")
    with connect(db_path) as conn:
        cur = conn.execute(sql, params or [])
        return [dict(r) for r in cur.fetchall()]


def query_iter(db_path: str, sql: str, params: dict[str, Any] | None = None) -> Iterable[dict[str, Any]]:
    if not Path(db_path).exists():
        raise FileNotFoundError(f"SQLite db not found: {db_path}")

    with connect(db_path) as conn:
        cur = conn.execute(sql, params or {})
        for r in cur:
            yield dict(r)

