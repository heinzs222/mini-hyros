from __future__ import annotations

import os
import re
import sqlite3
import threading
from collections.abc import Iterable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit


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


POSTGRES_SCHEMES = ("postgres://", "postgresql://")

_INSERT_REPLACE_CONFLICT_TARGETS: dict[str, tuple[str, ...]] = {
    "ad_names": ("platform", "entity_type", "entity_id"),
    "campaign_settings": ("platform", "campaign_id"),
    "platform_tokens": ("platform",),
    "capi_log": ("id",),
    "email_sms_events": ("id",),
    "refund_log": ("id",),
    "stripe_sync_coverage": ("start_date", "end_date"),
    "webhook_log": ("id",),
}

_INSERT_REPLACE = re.compile(
    r"^\s*INSERT\s+OR\s+REPLACE\s+INTO\s+([^\s(]+)\s*\(([^)]+)\)\s*VALUES\s*",
    re.IGNORECASE | re.DOTALL,
)


def database_url() -> str:
    return (os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL") or "").strip()


def is_postgres_dsn(value: str | None) -> bool:
    return str(value or "").strip().lower().startswith(POSTGRES_SCHEMES)


def using_postgres() -> bool:
    return is_postgres_dsn(database_url())


def db_label(value: str | os.PathLike[str]) -> str:
    """Return a safe display label for a configured database location."""
    text = str(value)
    if not is_postgres_dsn(text):
        return text
    try:
        parsed = urlsplit(text)
        hostname = parsed.hostname or ""
        port = f":{parsed.port}" if parsed.port else ""
        username = parsed.username or "postgres"
        return urlunsplit((parsed.scheme, f"{username}:***@{hostname}{port}", parsed.path, "", ""))
    except ValueError:
        return "<redacted postgres url>"


def _active_db_label(db_path: str | os.PathLike[str]) -> str:
    if is_postgres_dsn(str(db_path)):
        return db_label(db_path)
    if using_postgres():
        return db_label(database_url())
    return str(db_path)


# journal_mode is a persistent property of the database file, not of a
# connection, but it was re-applied on all ~50 connections a single report
# opens — the most expensive of the three PRAGMAs, and a write each time.
_wal_lock = threading.Lock()
_wal_applied: set[str] = set()


def _sqlite_connect(db_path: str | os.PathLike[str]) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")

    key = str(path)
    with _wal_lock:
        needs_wal = key not in _wal_applied
    if needs_wal:
        conn.execute("PRAGMA journal_mode = WAL")
        with _wal_lock:
            _wal_applied.add(key)
    return conn


def _append_clause(sql: str, clause: str) -> str:
    stripped = sql.rstrip()
    terminator = ";" if stripped.endswith(";") else ""
    body = stripped[:-1].rstrip() if terminator else stripped
    return f"{body} {clause}{terminator}"


def _sqlite_master_name(sql: str, params: Any) -> str | None:
    literal = re.search(r"\bname\s*=\s*['\"]([^'\"]+)['\"]", sql, flags=re.IGNORECASE)
    if literal:
        return literal.group(1)
    named = re.search(r"\bname\s*=\s*:([A-Za-z_][A-Za-z0-9_]*)", sql, flags=re.IGNORECASE)
    if named and isinstance(params, Mapping):
        value = params.get(named.group(1))
        return str(value) if value is not None else None
    if re.search(r"\bname\s*=\s*\?", sql, flags=re.IGNORECASE):
        if isinstance(params, Sequence) and not isinstance(params, (str, bytes)) and params:
            return str(params[0])
    return None


def _postgres_special_sql(sql: str, params: Any = None) -> tuple[str, Any] | None:
    pragma_match = re.fullmatch(
        r"\s*PRAGMA\s+table_info\(\s*(?:\"([^\"]+)\"|'([^']+)'|([A-Za-z_][\w.]*))\s*\)\s*;?\s*",
        sql,
        flags=re.IGNORECASE,
    )
    if pragma_match:
        raw_table = next(group for group in pragma_match.groups() if group)
        table = raw_table.split(".")[-1].strip('"')
        return (
            """
            SELECT
                ordinal_position - 1 AS cid,
                column_name AS name,
                data_type AS type,
                CASE WHEN is_nullable = 'NO' THEN 1 ELSE 0 END AS notnull,
                column_default AS dflt_value,
                0 AS pk
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table,),
        )

    if not re.search(r"\bsqlite_master\b", sql, flags=re.IGNORECASE):
        return None
    object_type = re.search(r"\btype\s*=\s*['\"]([^'\"]+)['\"]", sql, flags=re.IGNORECASE)
    if object_type and object_type.group(1).lower() != "table":
        return None

    table_name = _sqlite_master_name(sql, params)
    values: list[Any] = []
    if re.search(r"\bSELECT\s+1\b", sql, flags=re.IGNORECASE):
        query_sql = """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        """
        if table_name:
            query_sql += " AND table_name = %s"
            values.append(table_name)
        query_sql += " LIMIT 1"
        return query_sql, tuple(values)

    query_sql = """
        SELECT table_name AS name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
    """
    if table_name:
        query_sql += " AND table_name = %s"
        values.append(table_name)
    if re.search(r"\bORDER\s+BY\s+name\b", sql, flags=re.IGNORECASE):
        query_sql += " ORDER BY table_name"
    return query_sql, tuple(values)


def _replace_insert_or_ignore(sql: str) -> str:
    replaced, count = re.subn(
        r"\bINSERT\s+OR\s+IGNORE\s+INTO\b",
        "INSERT INTO",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )
    if count == 0 or re.search(r"\bON\s+CONFLICT\b", replaced, flags=re.IGNORECASE):
        return replaced
    return _append_clause(replaced, "ON CONFLICT DO NOTHING")


def _replace_insert_or_replace(sql: str, connection: "PostgresConnection | None" = None) -> str:
    match = _INSERT_REPLACE.match(sql)
    if not match:
        return sql

    table_token = match.group(1)
    table = table_token.strip().strip('"').split(".")[-1].lower()
    columns = [part.strip().strip('"') for part in match.group(2).replace("\n", " ").split(",")]
    replaced = re.sub(
        r"^\s*INSERT\s+OR\s+REPLACE\s+INTO\b",
        "INSERT INTO",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )

    conflict_columns = connection.conflict_columns(table_token) if connection else []
    if not conflict_columns:
        conflict_columns = list(_INSERT_REPLACE_CONFLICT_TARGETS.get(table, (columns[0],)))

    updates = [column for column in columns if column not in conflict_columns]
    conflict = ", ".join(conflict_columns)
    if not updates:
        return _append_clause(replaced, f"ON CONFLICT ({conflict}) DO NOTHING")

    assignments = ", ".join(f"{column} = EXCLUDED.{column}" for column in updates)
    return _append_clause(replaced, f"ON CONFLICT ({conflict}) DO UPDATE SET {assignments}")


def _replace_date_functions(sql: str) -> str:
    sql = re.sub(r"\bIFNULL\s*\(", "COALESCE(", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bdatetime\s*\(\s*['\"]now['\"]\s*\)", "CURRENT_TIMESTAMP", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bdate\s*\(\s*['\"]now['\"]\s*\)", "CURRENT_DATE", sql, flags=re.IGNORECASE)
    sql = re.sub(
        r"date\s*\(\s*substr\s*\(\s*([A-Za-z_][\w.]*)\s*,\s*1\s*,\s*10\s*\)\s*\)",
        r"substr(\1, 1, 10)",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(r"date\s*\(\s*(\?)\s*\)", r"\1", sql, flags=re.IGNORECASE)
    sql = re.sub(
        r"date\s*\(\s*(:[A-Za-z_][A-Za-z0-9_]*)\s*\)",
        r"\1",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"CAST\s*\(\s*strftime\s*\(\s*['\"]%s['\"]\s*,\s*([^\)]+)\)\s+AS\s+INTEGER\s*\)",
        r"CAST(EXTRACT(EPOCH FROM \1) AS BIGINT)",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"strftime\s*\(\s*['\"]%s['\"]\s*,\s*['\"]now['\"]\s*\)",
        "CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) AS BIGINT)",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"strftime\(\s*'%Y-%m-%dT%H:%M:%SZ'\s*,\s*([^,]+?)\s*,\s*(:[A-Za-z_][A-Za-z0-9_]*|\?)\s*\)",
        r"""to_char((\1::timestamptz + \2::interval) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')""",
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def _replace_rowid(sql: str) -> str:
    sql = re.sub(
        r"\b([A-Za-z_][A-Za-z0-9_]*)\.rowid\b",
        r"\1.ctid::text",
        sql,
        flags=re.IGNORECASE,
    )
    return re.sub(r"\browid\b", "ctid::text", sql, flags=re.IGNORECASE)


def _replace_cast_real(sql: str) -> str:
    out: list[str] = []
    index = 0
    lower = sql.lower()
    while True:
        start = lower.find("cast(", index)
        if start < 0:
            out.append(sql[index:])
            return "".join(out)
        out.append(sql[index:start])
        inner_start = start + len("cast(")
        close = _matching_paren(sql, inner_start - 1)
        if close < 0:
            out.append(sql[start:])
            return "".join(out)
        inner = sql[inner_start:close]
        split = _split_cast_as_real(inner)
        if split is None:
            out.append("CAST(")
            out.append(_replace_cast_real(inner))
            out.append(")")
        else:
            out.append(f"CAST(COALESCE(NULLIF(({split})::text, ''), '0') AS DOUBLE PRECISION)")
        index = close + 1


def _matching_paren(sql: str, open_index: int) -> int:
    depth = 0
    in_single_quote = False
    index = open_index
    while index < len(sql):
        char = sql[index]
        if char == "'":
            if in_single_quote and index + 1 < len(sql) and sql[index + 1] == "'":
                index += 2
                continue
            in_single_quote = not in_single_quote
        elif not in_single_quote:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    return index
        index += 1
    return -1


def _split_cast_as_real(inner: str) -> str | None:
    in_single_quote = False
    depth = 0
    index = 0
    lower = inner.lower()
    while index < len(inner):
        char = inner[index]
        if char == "'":
            if in_single_quote and index + 1 < len(inner) and inner[index + 1] == "'":
                index += 2
                continue
            in_single_quote = not in_single_quote
        elif not in_single_quote:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif depth == 0 and lower.startswith(" as real", index):
                tail = inner[index + len(" as real") :].strip()
                if not tail:
                    return inner[:index].strip()
        index += 1
    return None


def _translate_placeholders(sql: str, params: Any) -> tuple[str, Any]:
    out: list[str] = []
    in_single_quote = False
    index = 0
    while index < len(sql):
        char = sql[index]
        if char == "'":
            out.append(char)
            if in_single_quote and index + 1 < len(sql) and sql[index + 1] == "'":
                out.append("'")
                index += 2
                continue
            in_single_quote = not in_single_quote
            index += 1
            continue
        if in_single_quote:
            out.append(char)
            index += 1
            continue
        if char == "?":
            out.append("%s")
            index += 1
            continue
        if (
            char == ":"
            and index + 1 < len(sql)
            and re.match(r"[A-Za-z_]", sql[index + 1])
            and (index == 0 or sql[index - 1] != ":")
        ):
            end = index + 2
            while end < len(sql) and re.match(r"[A-Za-z0-9_]", sql[end]):
                end += 1
            out.append(f"%({sql[index + 1:end]})s")
            index = end
            continue
        out.append(char)
        index += 1
    if isinstance(params, Mapping):
        return "".join(out), dict(params)
    return "".join(out), params


def _translate_sql(sql: str, params: Any = None, connection: "PostgresConnection | None" = None) -> tuple[str, Any]:
    special = _postgres_special_sql(sql, params)
    if special is not None:
        return special

    translated = _replace_insert_or_ignore(sql)
    translated = _replace_insert_or_replace(translated, connection)
    translated = _replace_date_functions(translated)
    translated = _replace_rowid(translated)
    translated = _replace_cast_real(translated)
    translated = re.sub(r"\s+COLLATE\s+NOCASE\b", "", translated, flags=re.IGNORECASE)
    return _translate_placeholders(translated, params)


def _translate_postgres_sql(sql: str, params: Any = None) -> tuple[str, Any]:
    return _translate_sql(sql, params, None)


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
        return _postgres_compat_row(self._cursor.fetchone())

    def fetchall(self) -> list[Any]:
        return [_postgres_compat_row(row) for row in self._cursor.fetchall()]

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
        return (_postgres_compat_row(row) for row in self._cursor)

    def __enter__(self) -> "PostgresCursor":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()


class PostgresCompatRow(dict[str, Any]):
    """Mapping row that also supports SQLite-style numeric column access."""

    def __getitem__(self, key: str | int) -> Any:
        if isinstance(key, int):
            return tuple(self.values())[key]
        return super().__getitem__(key)


def _postgres_compat_row(row: Any) -> Any:
    if row is None or isinstance(row, PostgresCompatRow):
        return row
    if isinstance(row, Mapping):
        return PostgresCompatRow(row)
    return row


class PostgresConnection:
    def __init__(self, url: str, *, autocommit: bool = False):
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise RuntimeError("PostgreSQL requires psycopg. Install backend/requirements.txt.") from exc
        self.raw = psycopg.connect(
            url,
            row_factory=dict_row,
            prepare_threshold=None,
            autocommit=autocommit,
        )
        self.url = url
        self.autocommit = autocommit
        # Set by the idle pool; a pooled connection's close() parks it for reuse
        # instead of tearing down the socket.
        self.pooled = False
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
        if self.pooled:
            _release_pooled_connection(self)
            return
        self.raw.close()

    def hard_close(self) -> None:
        try:
            self.raw.close()
        except Exception:
            pass

    def __enter__(self) -> "PostgresConnection":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.close()


# ── Idle connection pool for connect() ────────────────────────────────────────
# Assembling one report calls connect() ~60 times (schema probes, spend reads,
# attribution loads, integration status). Against SQLite that is a cheap file
# open; against Supabase every one of them was a fresh TCP + TLS + auth
# handshake plus a statement_timeout round trip, so the connection setup alone
# dominated the response. Connections are now parked per thread and reused.
_POOL_SIZE = max(0, int(os.environ.get("POSTGRES_IDLE_POOL_SIZE", "4") or 0))
_pool_state = threading.local()


def _idle_pool(url: str) -> list[PostgresConnection]:
    pools = getattr(_pool_state, "pools", None)
    if pools is None:
        pools = {}
        _pool_state.pools = pools
    return pools.setdefault(url, [])


def _acquire_pooled_connection(url: str) -> PostgresConnection:
    pool = _idle_pool(url)
    while pool:
        connection = pool.pop()
        if not connection.raw.closed:
            return connection
    connection = PostgresConnection(url)
    connection.pooled = _POOL_SIZE > 0
    return connection


def _release_pooled_connection(connection: PostgresConnection) -> None:
    """Park a finished connection for reuse, or drop it if it isn't reusable."""
    if connection.raw.closed:
        return
    try:
        # Never hand back a connection mid-transaction or in an aborted state.
        # TransactionStatus.IDLE is 0; an unreadable status is treated as dirty.
        status = getattr(getattr(connection.raw, "info", None), "transaction_status", None)
        if status != 0:
            connection.raw.rollback()
    except Exception:
        connection.hard_close()
        return

    pool = _idle_pool(connection.url)
    if len(pool) >= _POOL_SIZE:
        connection.hard_close()
        return
    pool.append(connection)


def close_pooled_connections() -> None:
    """Drop this thread's idle connections (used by tests and shutdown)."""
    pools = getattr(_pool_state, "pools", None)
    if not pools:
        return
    for pool in pools.values():
        while pool:
            pool.pop().hard_close()
    pools.clear()


def connect(db_path: str | os.PathLike[str]) -> sqlite3.Connection | PostgresConnection:
    path_text = str(db_path)
    if is_postgres_dsn(path_text):
        return _acquire_pooled_connection(path_text)
    if using_postgres():
        return _acquire_pooled_connection(database_url())
    return _sqlite_connect(db_path)


_query_connection_state = threading.local()


def _cached_postgres_connection(url: str) -> PostgresConnection:
    state = getattr(_query_connection_state, "postgres", None)
    if state is not None:
        cached_url, connection = state
        if cached_url == url and not connection.raw.closed:
            return connection
        try:
            connection.close()
        except Exception:
            pass

    connection = PostgresConnection(url, autocommit=True)
    _query_connection_state.postgres = (url, connection)
    return connection


@contextmanager
def _query_connection(
    db_path: str | os.PathLike[str],
) -> Iterable[sqlite3.Connection | PostgresConnection]:
    path_text = str(db_path)
    url = path_text if is_postgres_dsn(path_text) else database_url()
    if not is_postgres_dsn(url):
        with _sqlite_connect(db_path) as connection:
            yield connection
        return

    connection = _cached_postgres_connection(url)
    try:
        yield connection
        if not connection.autocommit:
            connection.commit()
    except Exception:
        try:
            if not connection.autocommit:
                connection.rollback()
        except Exception:
            try:
                connection.close()
            finally:
                _query_connection_state.postgres = None
        raise


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


def _requires_existing_sqlite_file(db_path: str | os.PathLike[str]) -> bool:
    return not is_postgres_dsn(str(db_path)) and not using_postgres()


def query(db_path: str, sql: str, params: dict[str, Any] | None = None) -> QueryResult:
    if _requires_existing_sqlite_file(db_path) and not os.path.exists(db_path):
        raise FileNotFoundError(db_path)
    with _query_connection(db_path) as conn:
        cursor = conn.execute(sql, params or {})
        try:
            columns = _column_names(cursor.description)
            rows = _dict_rows(cursor.fetchall(), columns)
        finally:
            cursor.close()
        return QueryResult(
            columns=columns,
            rows=rows,
            db_path=_active_db_label(db_path),
            sql=sql,
            params=params or {},
        )


def sql_rows(db_path: str, sql: str, params: Any = None) -> list[dict[str, Any]]:
    if _requires_existing_sqlite_file(db_path) and not os.path.exists(db_path):
        raise FileNotFoundError(db_path)
    with _query_connection(db_path) as conn:
        cursor = conn.execute(sql) if params is None else conn.execute(sql, params)
        try:
            columns = _column_names(cursor.description)
            return _dict_rows(cursor.fetchall(), columns)
        finally:
            cursor.close()


def query_iter(db_path: str, sql: str, params: dict[str, Any] | None = None) -> Iterable[dict[str, Any]]:
    yield from query(db_path, sql, params).rows
