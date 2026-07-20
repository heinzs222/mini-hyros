"""Backend-agnostic schema introspection.

SQLite exposes catalog data through ``sqlite_master`` and ``PRAGMA
table_info``; Postgres through ``information_schema`` / ``to_regclass``. These
helpers hide that difference so the ~10 call sites that need "does this table
exist" / "what columns does it have" / "list the tables" work on both backends.
"""

from __future__ import annotations

from attributionops.db import is_postgres, query


def list_tables(db_path: str) -> list[str]:
    if is_postgres():
        rows = query(
            db_path,
            "SELECT table_name AS name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' "
            "ORDER BY table_name",
        ).rows
    else:
        rows = query(
            db_path,
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
        ).rows
    return [str(r.get("name") or "") for r in rows]


def table_exists(db_path: str, table: str) -> bool:
    if is_postgres():
        rows = query(
            db_path,
            "SELECT to_regclass('public.' || :t) IS NOT NULL AS present",
            {"t": table},
        ).rows
        return bool(rows and rows[0].get("present"))
    rows = query(
        db_path,
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = :t",
        {"t": table},
    ).rows
    return bool(rows)


def table_columns(db_path: str, table: str) -> set[str]:
    if is_postgres():
        rows = query(
            db_path,
            "SELECT column_name AS name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = :t",
            {"t": table},
        ).rows
        return {str(r.get("name") or "") for r in rows}
    rows = query(db_path, f"PRAGMA table_info({table})").rows
    # PRAGMA table_info columns: cid, name, type, notnull, dflt_value, pk
    return {str(r.get("name") or "") for r in rows}
