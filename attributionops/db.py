from __future__ import annotations

import sqlite3
import threading
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


def _tune(conn: sqlite3.Connection) -> None:
    """Apply read/write tuning PRAGMAs to a connection.

    WAL + ``synchronous=NORMAL`` gives concurrent readers alongside a single
    writer; a large page cache and memory temp store keep the warehouse
    aggregation/group-by work off disk; ``busy_timeout`` avoids spurious
    "database is locked" errors when a writer and readers overlap.

    These are connection-scoped settings (apart from WAL, which is persisted in
    the file), so they must be re-applied to every new connection — the original
    code applied none of them, leaving every connection at ``synchronous=FULL``
    with a tiny cache.
    """
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA temp_store=MEMORY;")
        cur.execute("PRAGMA cache_size=-65536;")    # ~64 MB page cache
        cur.execute("PRAGMA mmap_size=134217728;")  # 128 MB memory-mapped I/O
        cur.execute("PRAGMA busy_timeout=5000;")
    finally:
        cur.close()


def connect(db_path: str) -> sqlite3.Connection:
    """Open a fresh, tuned connection.

    Callers that write should use this with a context manager
    (``with connect(...) as conn: ...``) so the transaction commits/rolls back.
    """
    conn = sqlite3.connect(db_path)
    _tune(conn)
    return conn


# ── Cached read connections ──────────────────────────────────────────────────
# A single report fans out to ~20 read queries; opening (and cold-starting the
# page cache of) a brand-new connection for each one is pure overhead. We keep
# one tuned, autocommit read connection per (thread, db_path) and reuse it so
# the page cache stays warm across the whole request.
#
# isolation_level=None (autocommit) means SELECTs never open a transaction, so
# readers always observe the latest committed data and never block the writer;
# the rare write accidentally routed through these helpers still commits
# immediately. The cache is thread-local, so each FastAPI threadpool worker gets
# its own connection (SQLite connections are not shared across threads).
_local = threading.local()


def _read_conn(db_path: str) -> sqlite3.Connection:
    cache: dict[str, sqlite3.Connection] | None = getattr(_local, "conns", None)
    if cache is None:
        cache = {}
        _local.conns = cache

    conn = cache.get(db_path)
    if conn is not None:
        try:
            conn.execute("SELECT 1;")
            return conn
        except sqlite3.Error:
            try:
                conn.close()
            except sqlite3.Error:
                pass
            cache.pop(db_path, None)

    if not Path(db_path).exists():
        raise FileNotFoundError(f"SQLite db not found: {db_path}")
    conn = sqlite3.connect(db_path, isolation_level=None)
    _tune(conn)
    cache[db_path] = conn
    return conn


def query(db_path: str, sql: str, params: dict[str, Any] | None = None) -> QueryResult:
    conn = _read_conn(db_path)
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
    conn = _read_conn(db_path)
    cur = conn.execute(sql, params or [])
    return [dict(r) for r in cur.fetchall()]


def query_iter(db_path: str, sql: str, params: dict[str, Any] | None = None) -> Iterable[dict[str, Any]]:
    conn = _read_conn(db_path)
    cur = conn.execute(sql, params or {})
    for r in cur:
        yield dict(r)
