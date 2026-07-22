from __future__ import annotations

import os
import re
import sqlite3
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class QueryResult:
    columns: list[str]
    rows: list[dict[str, Any]]
    db_path: str = ""
    sql: str = ""
    params: Any = None

    @property
    def row_count(self) -> int:
        return len(self.rows)

    def as_dicts(self) -> list[dict[str, Any]]:
        return self.rows


def database_url() -> str:
    return (os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL") or "").strip()


def using_postgres() -> bool:
    return database_url().lower().startswith(("postgres://", "postgresql://"))


def _sqlite_connect(db_path: str | os.PathLike[str]) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


_NAMED_PARAMETER = re.compile(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)")
_INSERT_REPLACE = re.compile(
    r"^\s*INSERT\s+OR\s+REPLACE\s+INTO\s+([^\s(]+)\s*\(([^)]+)\)\s*VALUES\s*",
    re.IGNORECASE | re.DOTALL,
)


def _translate_sql(sql: str, params: Any = None, connection: "PostgresConnection | None" = None) -> tuple[str, Any]:
    translated = sql
    translated = re.sub(r"\bIFNULL\s*\(", "COALESCE(", translated, flags=re.IGNORECASE)
    translated = re.sub(r"\bdatetime\s*\(\s*['\"]now['\"]\s*\)", "CURRENT_TIMESTAMP", translated, flags=re.IGNORECASE)
    translated = re.sub(r"\bdate\s*\(\s*['\"]now['\"]\s*\)", "CURRENT_DATE", translated, flags=re.IGNORECASE)
    translated = re.sub(
        r"CAST\s*\(\s*strftime\s*\(\s*['\"]%s['\"]\s*,\s*([^\)]+)\)\s+AS\s+INTEGER\s*\)",
        r"CAST(EXTRACT(EPOCH FROM \1) AS BIGINT)",
        translated,
        flags=re.IGNORECASE,
    )
    translated = re.sub(
        r"strftime\s*\(\s*['\"]%s['\"]\s*,\s*['\"]now['\"]\s*\)",
        "CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) AS BIGINT)",
        translated,
        flags=re.IGNORECASE,
    )
    translated = re.sub(r"\s+COLLATE\s+NOCASE\b", "", translated, flags=re.IGNORECASE)
    translated = re.sub(r"\bINSERT\s+OR\s+IGNORE\s+INTO\b", "INSERT INTO", translated, flags=re.IGNORECASE)

    ignore_insert = bool(re.search(r"\bINSERT\s+OR\s+IGNORE\s+INTO\b", sql, flags=re.IGNORECASE))
    replace_match = _INSERT_REPLACE.match(sql)
    if replace_match:
        table_token = replace_match.group(1)
        columns = [part.strip().strip('"') for part in replace_match.group(2).split(",")]
        translated = re.sub(r"^\s*INSERT\s+OR\s+REPLACE\s+INTO\b", "INSERT INTO", translated, count=1, flags=re.IGNORECASE)
        conflict_columns = connection.conflict_columns(table_token) if connection else []
        if not conflict_columns:
            conflict_columns = [columns[0]]
        updates = [column for column in columns if column not in conflict_columns]
        conflict = ", ".join(f'"{column}"' for column in conflict_columns)
        if updates:
            assignments = ", ".join(f'"{column}" = EXCLUDED."{column}"' for column in updates)
            translated = translated.rstrip().rstrip(";") + f" ON CONFLICT ({conflict}) DO UPDATE SET {assignments}"
        else:
            translated = translated.rstrip().rstrip(";") + f" ON CONFLICT ({conflict}) DO NOTHING"
    elif ignore_insert:
        translated = translated.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"

    if isinstance(params, Mapping):
        translated = _NAMED_PARAMETER.sub(r"%(\1)s", translated)
        return translated, dict(params)
    if params is None:
        return translated, None
    translated = translated.replace("?", "%s")
    return translated, params


class PostgresCursor:
    def __init__(self, connection: "PostgresConnection", cursor: Any):
        self.connection = connection
        self._cursor = cursor

    def execute(self, sql: str, params: Any = None) -> "PostgresCursor":
        translated, values = _translate_sql(sql, params, self.connection)
        if values is None:
            self._cursor.execute(translated)
        else:
            self._cursor.execute(translated, values)
        return self

    def executemany(self, sql: str, params: Sequence[Any]) -> "PostgresCursor":
        values = list(params)
        if not values:
            return self
        translated, _ = _translate_sql(sql, values[0], self.connection)
        self._cursor.executemany(translated, values)
        return self

    def fetchone(self) -> Any:
        return self._cursor.fetchone()

    def fetchall(self) -> list[Any]:
        return list(self._cursor.fetchall())

    @property
    def description(self) -> Any:
        return self._cursor.description

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    @property
    def lastrowid(self) -> int | None:
        try:
            with self.connection.raw.cursor() as cursor:
                cursor.execute("SELECT LASTVAL() AS value")
                row = cursor.fetchone()
                return int(row["value"] if isinstance(row, Mapping) else row[0])
        except Exception:
            return None

    def close(self) -> None:
        self._cursor.close()

    def __iter__(self):
        return iter(self._cursor)

    def __enter__(self) -> "PostgresCursor":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()


class PostgresConnection:
    def __init__(self, url: str):
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise RuntimeError("PostgreSQL requires psycopg. Install backend/requirements.txt.") from exc
        self.raw = psycopg.connect(url, row_factory=dict_row, prepare_threshold=None)
        with self.raw.cursor() as cursor:
            cursor.execute("SET statement_timeout = '55s'")
        self._conflicts: dict[str, list[str]] = {}

    @property
    def row_factory(self) -> Any:
        return None

    @row_factory.setter
    def row_factory(self, value: Any) -> None:
        return None

    def cursor(self) -> PostgresCursor:
        return PostgresCursor(self, self.raw.cursor())

    def execute(self, sql: str, params: Any = None) -> PostgresCursor:
        return self.cursor().execute(sql, params)

    def executemany(self, sql: str, params: Sequence[Any]) -> PostgresCursor:
        return self.cursor().executemany(sql, params)

    def executescript(self, script: str) -> None:
        with self.raw.cursor() as cursor:
            cursor.execute(script)

    def conflict_columns(self, table_token: str) -> list[str]:
        table = table_token.strip().strip('"').split(".")[-1]
        if table in self._conflicts:
            return self._conflicts[table]
        sql = """
            SELECT array_agg(attribute.attname ORDER BY key_position.ordinality) AS columns
            FROM pg_constraint constraint_row
            JOIN LATERAL unnest(constraint_row.conkey) WITH ORDINALITY AS key_position(attnum, ordinality) ON TRUE
            JOIN pg_attribute attribute
              ON attribute.attrelid = constraint_row.conrelid
             AND attribute.attnum = key_position.attnum
            WHERE constraint_row.conrelid = %s::regclass
              AND constraint_row.contype IN ('p', 'u')
            GROUP BY constraint_row.oid, constraint_row.contype
            ORDER BY (constraint_row.contype = 'p') DESC, constraint_row.oid
            LIMIT 1
        """
        try:
            with self.raw.cursor() as cursor:
                cursor.execute(sql, (table,))
                row = cursor.fetchone()
            columns = list(row.get("columns") or []) if row else []
        except Exception:
            self.raw.rollback()
            columns = []
        self._conflicts[table] = columns
        return columns

    def commit(self) -> None:
        self.raw.commit()

    def rollback(self) -> None:
        self.raw.rollback()

    def close(self) -> None:
        self.raw.close()

    def __enter__(self) -> "PostgresConnection":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.close()


def connect(db_path: str | os.PathLike[str]) -> sqlite3.Connection | PostgresConnection:
    if using_postgres():
        return PostgresConnection(database_url())
    return _sqlite_connect(db_path)


def _column_names(description: Any) -> list[str]:
    if not description:
        return []
    return [getattr(item, "name", None) or item[0] for item in description]


def _dict_rows(rows: Iterable[Any], columns: list[str]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, Mapping):
            output.append(dict(row))
        else:
            output.append(dict(zip(columns, row)))
    return output


def query(db_path: str, sql: str, params: dict[str, Any] | None = None) -> QueryResult:
    if not using_postgres() and not os.path.exists(db_path):
        raise FileNotFoundError(db_path)
    with connect(db_path) as conn:
        cursor = conn.execute(sql, params or {})
        columns = _column_names(cursor.description)
        rows = _dict_rows(cursor.fetchall(), columns)
        return QueryResult(
            columns=columns,
            rows=rows,
            db_path=db_path,
            sql=sql,
            params=params or {},
        )


def sql_rows(db_path: str, sql: str, params: Any = None) -> list[dict[str, Any]]:
    if not using_postgres() and not os.path.exists(db_path):
        raise FileNotFoundError(db_path)
    with connect(db_path) as conn:
        cursor = conn.execute(sql) if params is None else conn.execute(sql, params)
        columns = _column_names(cursor.description)
        return _dict_rows(cursor.fetchall(), columns)


def query_iter(db_path: str, sql: str, params: dict[str, Any] | None = None) -> Iterable[dict[str, Any]]:
    yield from query(db_path, sql, params).rows
