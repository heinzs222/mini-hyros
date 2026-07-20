"""psycopg-backed connection adapters that stand in for sqlite3 connections.

The warehouse code was written for the stdlib ``sqlite3`` API: ``connect()``
objects with ``.execute()/.executemany()/.cursor()/.commit()/.rollback()``, used
both directly and as context managers, whose cursors expose
``.fetchone()/.fetchall()/.rowcount/.description`` and yield rows that support
*both* positional (``row[0]``) and column-name (``row["x"]``) access.

These adapters reproduce exactly that surface on top of psycopg 3 so the ~30
modules that talk to the database do not need to care which backend they are on.
Every SQL string is passed through :func:`attributionops.pg_dialect.translate`
before execution. Connections are configured for the Supabase transaction pooler
(prepared statements disabled) so they are safe to open per serverless
invocation.
"""

from __future__ import annotations

import threading
from typing import Any

import psycopg

from attributionops.pg_dialect import translate


# ── Hybrid row (positional + mapping, like sqlite3.Row) ──────────────────────
class Row:
    __slots__ = ("_cols", "_vals", "_idx")

    def __init__(self, cols: list[str], vals: tuple[Any, ...]):
        self._cols = cols
        self._vals = tuple(vals)
        self._idx: dict[str, int] | None = None

    def _index(self) -> dict[str, int]:
        if self._idx is None:
            self._idx = {c: i for i, c in enumerate(self._cols)}
        return self._idx

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, (int, slice)):
            return self._vals[key]
        return self._vals[self._index()[key]]

    def get(self, key: str, default: Any = None) -> Any:
        i = self._index().get(key)
        return self._vals[i] if i is not None else default

    def keys(self) -> list[str]:
        return list(self._cols)

    def __iter__(self):
        # sqlite3.Row iterates over the *values*; match that so any code doing
        # ``list(row)`` or tuple-unpacking behaves identically.
        return iter(self._vals)

    def __len__(self) -> int:
        return len(self._vals)

    def __repr__(self) -> str:
        return f"Row({dict(zip(self._cols, self._vals))!r})"


def _hybrid_row_factory(cursor: Any):
    cols = [c.name for c in (cursor.description or [])]

    def make(values: Any) -> Row:
        return Row(cols, values)

    return make


def _norm_params(params: Any) -> Any:
    # psycopg only processes ``%%`` escapes / ``%s`` placeholders when a params
    # container is supplied, so never pass None — an empty tuple still triggers
    # the escape handling for SQL that has literal percent signs.
    if params is None:
        return ()
    return params


# ── Cursor adapter ───────────────────────────────────────────────────────────
class _Cursor:
    def __init__(self, cur: Any):
        self._cur = cur

    def execute(self, sql: str, params: Any = None) -> "_Cursor":
        self._cur.execute(translate(sql), _norm_params(params))
        return self

    def executemany(self, sql: str, seq_of_params: Any) -> "_Cursor":
        self._cur.executemany(translate(sql), [_norm_params(p) for p in seq_of_params])
        return self

    def fetchone(self) -> Any:
        return self._cur.fetchone()

    def fetchall(self) -> list[Any]:
        return self._cur.fetchall()

    def fetchmany(self, size: int | None = None) -> list[Any]:
        return self._cur.fetchmany(size) if size is not None else self._cur.fetchmany()

    @property
    def rowcount(self) -> int:
        return self._cur.rowcount

    @property
    def lastrowid(self) -> Any:  # rarely used; psycopg has no direct equivalent
        return None

    @property
    def description(self) -> Any:
        return self._cur.description

    def __iter__(self):
        return iter(self._cur)

    def close(self) -> None:
        self._cur.close()


# ── Connection adapter ───────────────────────────────────────────────────────
class _Connection:
    def __init__(self, conn: psycopg.Connection):
        self._conn = conn

    # sqlite3-style convenience: connection.execute() opens a cursor for you.
    def execute(self, sql: str, params: Any = None) -> _Cursor:
        cur = _Cursor(self._conn.cursor())
        cur.execute(sql, params)
        return cur

    def executemany(self, sql: str, seq_of_params: Any) -> _Cursor:
        cur = _Cursor(self._conn.cursor())
        cur.executemany(sql, seq_of_params)
        return cur

    def cursor(self) -> _Cursor:
        return _Cursor(self._conn.cursor())

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()

    @property
    def raw(self) -> psycopg.Connection:
        return self._conn

    def __enter__(self) -> "_Connection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        # Mirror sqlite3's context manager: commit on success, roll back on
        # error. Unlike sqlite3 we also close, because every ``with connect()``
        # block in this codebase opens a fresh connection scoped to the block —
        # closing keeps serverless connection counts bounded.
        try:
            if exc_type is None:
                self._conn.commit()
            else:
                self._conn.rollback()
        finally:
            self._conn.close()


def _open(dsn: str, *, autocommit: bool) -> psycopg.Connection:
    conn = psycopg.connect(
        dsn,
        autocommit=autocommit,
        # Disable prepared statements: the Supabase transaction pooler
        # (pgBouncer) does not support them across pooled sessions.
        prepare_threshold=None,
        connect_timeout=15,
    )
    conn.row_factory = _hybrid_row_factory
    return conn


def connect(dsn: str) -> _Connection:
    """Fresh write connection (transactional; commits/closes via context manager)."""
    return _Connection(_open(dsn, autocommit=False))


# ── Cached autocommit read connections (thread-local, mirrors the SQLite path) ─
_local = threading.local()


def read_conn(dsn: str) -> _Connection:
    cache: dict[str, _Connection] | None = getattr(_local, "pg_conns", None)
    if cache is None:
        cache = {}
        _local.pg_conns = cache

    conn = cache.get(dsn)
    if conn is not None:
        try:
            conn.execute("SELECT 1;").fetchone()
            return conn
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            cache.pop(dsn, None)

    conn = _Connection(_open(dsn, autocommit=True))
    cache[dsn] = conn
    return conn
