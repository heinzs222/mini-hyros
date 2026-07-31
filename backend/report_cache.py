"""Warehouse-backed report cache shared by every server instance.

The in-process cache in ``main`` only helps a process that already built the
report. On serverless that is rarely the same process twice: instances are
recycled between clicks and a single page load can fan out across several of
them, so almost every request paid a full rebuild. Persisting finished reports
in the warehouse lets any instance serve one that any other instance built.

Only used when the warehouse is Postgres. A SQLite deployment is a single
long-lived process whose in-memory cache already hits, and adding a write per
report there would just contend for the same file lock the report reads from.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from attributionops.db import connect, is_postgres_dsn, using_postgres

logger = logging.getLogger("vigil.report_cache")

_TABLE = "public.report_cache"

# A report big enough to be worth several MB of round trip on every read is
# cheaper to rebuild than to ship; skip persisting those.
_MAX_PAYLOAD_BYTES = int(os.environ.get("REPORT_CACHE_SHARED_MAX_BYTES", "4000000") or 0)


def enabled(db_path: str) -> bool:
    if str(os.environ.get("REPORT_CACHE_SHARED", "1")).strip().lower() in {"0", "false", "off"}:
        return False
    return is_postgres_dsn(str(db_path)) or using_postgres()


def _create_table(conn: Any) -> None:
    """Create the table on a warehouse that predates it.

    The schema migration owns this table; this is only the recovery path for a
    warehouse that has not had the migration applied yet. Creating it eagerly on
    every cold start would spend the DDL round trips the migration fingerprint
    check exists to avoid.
    """
    conn.execute(
        f"""CREATE TABLE IF NOT EXISTS {_TABLE} (
            key TEXT PRIMARY KEY,
            payload TEXT NOT NULL,
            built_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            expires_at TIMESTAMPTZ NOT NULL
        )"""
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_report_cache_expires_at ON {_TABLE} (expires_at)"
    )


def get(db_path: str, key: str) -> Any | None:
    """Return a cached report built by any instance, or None."""
    if not enabled(db_path) or not key:
        return None
    try:
        with connect(db_path) as conn:
            row = conn.execute(
                f"SELECT payload FROM {_TABLE} WHERE key = %s AND expires_at > now()",
                (key,),
            ).fetchone()
        if not row:
            return None
        payload = row["payload"] if isinstance(row, dict) else row[0]
        return json.loads(payload)
    except Exception:
        # A cache is never worth failing a request over — a missing table just
        # means the next write creates it.
        logger.debug("shared report cache read failed", exc_info=True)
        return None


def put(db_path: str, key: str, value: Any, ttl_seconds: float) -> None:
    """Store a finished report for other instances; best effort."""
    if not enabled(db_path) or not key or ttl_seconds <= 0:
        return
    try:
        payload = json.dumps(value, separators=(",", ":"), default=str)
        if _MAX_PAYLOAD_BYTES and len(payload) > _MAX_PAYLOAD_BYTES:
            return
        with connect(db_path) as conn:
            _write(conn, key, payload, ttl_seconds)
            conn.commit()
    except Exception:
        logger.warning("shared report cache write failed", exc_info=True)


def _write(conn: Any, key: str, payload: str, ttl_seconds: float) -> None:
    try:
        _upsert(conn, key, payload, ttl_seconds)
    except Exception:
        # First write against a warehouse that predates the table.
        conn.rollback()
        _create_table(conn)
        _upsert(conn, key, payload, ttl_seconds)


def _upsert(conn: Any, key: str, payload: str, ttl_seconds: float) -> None:
    conn.execute(
        f"""INSERT INTO {_TABLE} (key, payload, built_at, expires_at)
            VALUES (%s, %s, now(), now() + make_interval(secs => %s))
            ON CONFLICT (key) DO UPDATE
                SET payload = EXCLUDED.payload,
                    built_at = EXCLUDED.built_at,
                    expires_at = EXCLUDED.expires_at""",
        (key, payload, float(ttl_seconds)),
    )
    # Opportunistic cleanup so expired rows cannot accumulate; bounded so it
    # never turns into a long delete on the request path.
    conn.execute(
        f"DELETE FROM {_TABLE} WHERE key IN ("
        f"  SELECT key FROM {_TABLE} WHERE expires_at < now() LIMIT 50)"
    )


def clear(db_path: str) -> None:
    """Drop every shared entry (used when a sync invalidates the warehouse)."""
    if not enabled(db_path):
        return
    try:
        with connect(db_path) as conn:
            conn.execute(f"DELETE FROM {_TABLE}")
            conn.commit()
    except Exception:
        logger.debug("shared report cache clear failed", exc_info=True)
