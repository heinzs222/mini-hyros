"""Per-campaign tracked/excluded flag — read-time filtering helpers.

A campaign is excluded from tracking by inserting a ``campaign_settings`` row
with ``tracked = 0`` (keyed by ``platform`` + ``campaign_id``). Everything is
filtered at read time — no stored rows are deleted or recomputed — so toggling
is instant and retroactive for every historical date range.

Two shapes are provided:
  * ``excluded_campaign_keys(db_path)`` → a ``set[(platform_lower, campaign_id)]``
    for Python-side aggregation loops (ads/attribution).
  * ``not_excluded_sql(...)`` → a ``NOT EXISTS (...)`` SQL fragment for raw-SQL
    call sites (platform comparison, drill-down children, AI recs).

The table is created lazily (memoized per path) so old databases and test
fixtures that predate the feature keep working.
"""

from __future__ import annotations

import sqlite3
import threading

from ..db import sql_rows
from ..schema import ensure_campaign_settings
from attributionops.db import is_postgres

_lock = threading.Lock()
_ensured: set[str] = set()


def ensure_campaign_settings_table(db_path: str) -> None:
    if is_postgres():
        # Postgres schema is provisioned once by migrations/postgres/0001_schema.sql;
        # the SQLite-style CREATE/ALTER/PRAGMA below never runs against Postgres.
        return
    with _lock:
        if db_path in _ensured:
            return
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA busy_timeout=5000;")
            ensure_campaign_settings(conn)
            conn.commit()
    except sqlite3.Error:
        return
    with _lock:
        _ensured.add(db_path)


def excluded_campaign_keys(db_path: str) -> set[tuple[str, str]]:
    """Return {(platform_lower, campaign_id)} for campaigns flagged tracked=0."""
    ensure_campaign_settings_table(db_path)
    try:
        rows = sql_rows(
            db_path,
            "SELECT platform, campaign_id FROM campaign_settings WHERE tracked = 0",
        )
    except sqlite3.Error:
        return set()
    return {
        (str(r.get("platform", "")).lower(), str(r.get("campaign_id", "")))
        for r in rows
    }


def is_excluded(
    excluded: set[tuple[str, str]], platform: str | None, campaign_id: str | None
) -> bool:
    if not excluded:
        return False
    return (str(platform or "").lower(), str(campaign_id or "")) in excluded


def not_excluded_sql(platform_col: str, campaign_col: str) -> str:
    """SQL fragment (no leading AND) excluding rows whose campaign is tracked=0.

    Pass fully-qualified column expressions, e.g.
    ``not_excluded_sql("s.platform", "s.campaign_id")``. Callers must ensure the
    ``campaign_settings`` table exists first (``ensure_campaign_settings_table``).
    """
    return (
        "NOT EXISTS (SELECT 1 FROM campaign_settings cs "
        f"WHERE cs.tracked = 0 AND LOWER(cs.platform) = LOWER({platform_col}) "
        f"AND cs.campaign_id = {campaign_col})"
    )
