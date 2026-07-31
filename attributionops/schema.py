"""Idempotent schema migrations shared by the DB initializer and app startup.

These run against both freshly-created databases (via ``scripts/init_empty_db.py``)
and any pre-existing database at process start (via ``ensure_schema``), so the
integrity constraints the ingestion layer relies on are guaranteed to exist:

* UNIQUE indexes on ``orders.order_id`` / ``conversions.conversion_id`` — the
  webhook/sync/pixel writers all use ``INSERT OR IGNORE`` for idempotency, which
  is a silent no-op without a UNIQUE constraint (duplicate deliveries would
  otherwise double-count revenue).
* the ``campaign_settings`` table backing the per-campaign tracked/excluded flag.
* recurring Stripe orders keep their latest known acquisition source instead of
  becoming unattributed when a later API sync returns less tracking metadata.

Everything here is idempotent and cheap to re-run; ``ensure_schema`` is memoized
per process+path so the app pays the cost at most once per database.
"""

from __future__ import annotations

import hashlib
import logging
import sqlite3
import threading
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from attributionops.db import connect, is_postgres_dsn

logger = logging.getLogger(__name__)

_ensured_lock = threading.Lock()
_ensured_paths: set[str] = set()
# Last migration failure per DB path, surfaced via /api/health so a persistently
# broken migration (the root cause of downstream all-zero reports) is visible in
# monitoring rather than only in logs that may not be tailed.
_last_migration_error: dict[str, str] = {}
_POSTGRES_SCHEMA = Path(__file__).resolve().parents[1] / "supabase" / "migrations" / "202607210001_initial_schema.sql"


ORDER_SEMANTIC_COLUMNS: dict[str, str] = {
    "currency": "TEXT DEFAULT ''",
    "processor": "TEXT DEFAULT ''",
    "processor_customer_id": "TEXT DEFAULT ''",
    "payment_fingerprint": "TEXT DEFAULT ''",
    "product_key": "TEXT DEFAULT ''",
    "is_recurring": "INTEGER DEFAULT 0",
    "sale_group_id": "TEXT DEFAULT ''",
}

ORDER_ATTRIBUTION_COLUMNS: tuple[str, ...] = (
    "session_id",
    "visitor_id",
    "channel",
    "platform",
    "campaign_id",
    "adset_id",
    "ad_id",
    "creative_id",
    "gclid",
    "fbclid",
    "ttclid",
)


# These account-specific exclusions mirror the campaigns omitted by the source
# Hyros account. INSERT OR IGNORE preserves any later choice made in Vigil.
DEFAULT_CAMPAIGN_SETTINGS: tuple[tuple[str, str, int, str], ...] = (
    ("google", "21892266666", 0, "Default Hyros parity exclusion"),
    ("google", "23351447961", 0, "Default Hyros parity exclusion"),
    ("meta", "120237922106660149", 0, "Default Hyros parity exclusion"),
)


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def ensure_order_semantics(conn: sqlite3.Connection) -> None:
    """Add the order identity fields required by customer and AOV metrics."""
    columns = _table_columns(conn, "orders")
    for name, definition in ORDER_SEMANTIC_COLUMNS.items():
        if name not in columns:
            conn.execute(f"ALTER TABLE orders ADD COLUMN {name} {definition}")

    # Legacy rows represent one sale each until an ingestion source can provide
    # a stronger grouping key. This keeps every existing order count stable.
    conn.execute(
        "UPDATE orders SET sale_group_id = order_id "
        "WHERE COALESCE(sale_group_id, '') = ''"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_sale_group_id ON orders(sale_group_id)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_orders_processor_customer_id "
        "ON orders(processor_customer_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_orders_payment_fingerprint "
        "ON orders(payment_fingerprint)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_is_recurring ON orders(is_recurring)")


def _dedupe_and_unique_index(
    conn: sqlite3.Connection, table: str, key: str, index_name: str, legacy_index: str
) -> None:
    """Collapse duplicate rows on ``key`` then enforce uniqueness.

    A UNIQUE index cannot be created while duplicates exist, so we first delete
    all but the lowest-rowid row per key. On a fresh DB this deletes nothing.
    """
    # Does a unique index on this key already exist? If so, nothing to do.
    for row in conn.execute(f"PRAGMA index_list({table})").fetchall():
        # row: (seq, name, unique, origin, partial)
        if row[2]:  # unique
            cols = [c[2] for c in conn.execute(f"PRAGMA index_info({row[1]})").fetchall()]
            if cols == [key]:
                return

    conn.execute(
        f"DELETE FROM {table} WHERE rowid NOT IN "
        f"(SELECT MIN(rowid) FROM {table} GROUP BY {key})"
    )
    # Drop the redundant non-unique index (superseded by the unique one).
    conn.execute(f"DROP INDEX IF EXISTS {legacy_index}")
    conn.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {table}({key})")


def _backfill_linked_purchase_identities(conn: sqlite3.Connection) -> None:
    """Fill blank order/conversion identities from the matching purchase row."""
    tables = {
        str(row[0])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    if not {"orders", "conversions"}.issubset(tables):
        return

    order_columns = _table_columns(conn, "orders")
    conversion_columns = _table_columns(conn, "conversions")
    if "order_id" not in order_columns or "order_id" not in conversion_columns:
        return

    for column in ("customer_key", "session_id", "visitor_id"):
        if column not in order_columns or column not in conversion_columns:
            continue
        conn.execute(
            f"""
            UPDATE orders
               SET {column} = COALESCE(
                   NULLIF({column}, ''),
                   (
                       SELECT NULLIF(c.{column}, '')
                         FROM conversions c
                        WHERE c.order_id = orders.order_id
                          AND COALESCE(c.{column}, '') != ''
                        ORDER BY c.ts DESC, c.rowid DESC
                        LIMIT 1
                   ),
                   ''
               )
             WHERE COALESCE({column}, '') = ''
               AND COALESCE(order_id, '') != ''
            """
        )
        conn.execute(
            f"""
            UPDATE conversions
               SET {column} = COALESCE(
                   NULLIF({column}, ''),
                   (
                       SELECT NULLIF(o.{column}, '')
                         FROM orders o
                        WHERE o.order_id = conversions.order_id
                          AND COALESCE(o.{column}, '') != ''
                        ORDER BY o.ts DESC, o.rowid DESC
                        LIMIT 1
                   ),
                   ''
               )
             WHERE COALESCE({column}, '') = ''
               AND COALESCE(order_id, '') != ''
            """
        )


def _touchpoint_signal_sql(alias: str, columns: set[str]) -> str:
    clauses: list[str] = []
    if "channel" in columns:
        clauses.append(
            f"LOWER(COALESCE({alias}.channel, '')) NOT IN ('', 'organic', 'direct')"
        )
    for column in (
        "platform",
        "campaign_id",
        "adset_id",
        "ad_id",
        "creative_id",
        "gclid",
        "fbclid",
        "ttclid",
    ):
        if column in columns:
            clauses.append(f"COALESCE({alias}.{column}, '') != ''")
    return "(" + " OR ".join(clauses) + ")" if clauses else "0"


def _recurring_attribution_assignments(
    order_ref: str,
    columns: tuple[str, ...],
    touchpoint_columns: set[str],
) -> str:
    signal_sql = _touchpoint_signal_sql("t", touchpoint_columns)
    assignments: list[str] = []
    for column in columns:
        assignments.append(
            f"""{column} = COALESCE(
                NULLIF({column}, ''),
                (
                    SELECT t.{column}
                      FROM touchpoints t
                     WHERE COALESCE(t.customer_key, '') = COALESCE({order_ref}.customer_key, '')
                       AND COALESCE({order_ref}.customer_key, '') != ''
                       AND t.ts <= {order_ref}.ts
                       AND {signal_sql}
                     ORDER BY t.ts DESC, t.rowid DESC
                     LIMIT 1
                ),
                ''
            )"""
        )
    return ",\n".join(assignments)


def ensure_order_attribution_integrity(conn: sqlite3.Connection) -> None:
    """Repair and preserve source data used by the reporting-gap calculation.

    Stripe's charge API can return less metadata on a later sync than the realtime
    purchase event originally carried. The old update path replaced populated
    session/click/source columns with empty strings, making already-attributed
    orders fall back into the yellow reporting-gap banner. Recurring charges also
    legitimately happen long after the normal click lookback window, so they keep
    the buyer's latest known source from before the charge.
    """
    tables = {
        str(row[0])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    if "orders" not in tables:
        return

    _backfill_linked_purchase_identities(conn)

    if "touchpoints" not in tables:
        return

    order_columns = _table_columns(conn, "orders")
    touchpoint_columns = _table_columns(conn, "touchpoints")
    required_order_columns = {"customer_key", "ts", "is_recurring"}
    required_touchpoint_columns = {"customer_key", "ts"}
    if not required_order_columns.issubset(order_columns):
        return
    if not required_touchpoint_columns.issubset(touchpoint_columns):
        return

    shared_columns = tuple(
        column
        for column in ORDER_ATTRIBUTION_COLUMNS
        if column in order_columns and column in touchpoint_columns
    )
    if not shared_columns:
        return

    # Repair existing recurring orders. This is lifetime carry-forward only for
    # recurring charges; normal first-time purchases continue to obey the report's
    # configured lookback window inside the attribution engine.
    conn.execute(
        f"""
        UPDATE orders
           SET {_recurring_attribution_assignments('orders', shared_columns, touchpoint_columns)}
         WHERE COALESCE(is_recurring, 0) = 1
           AND COALESCE(customer_key, '') != ''
        """
    )

    # Never let a sparse re-sync erase attribution already captured by the pixel,
    # webhook, or an earlier richer Stripe response.
    preserve_when = " OR ".join(
        f"(COALESCE(NEW.{column}, '') = '' AND COALESCE(OLD.{column}, '') != '')"
        for column in shared_columns
    )
    preserve_set = ",\n".join(
        f"{column} = CASE WHEN COALESCE(NEW.{column}, '') = '' "
        f"THEN OLD.{column} ELSE NEW.{column} END"
        for column in shared_columns
    )
    conn.execute("DROP TRIGGER IF EXISTS trg_orders_preserve_attribution_after_update")
    conn.execute(
        f"""
        CREATE TRIGGER trg_orders_preserve_attribution_after_update
        AFTER UPDATE OF {', '.join(shared_columns)} ON orders
        WHEN {preserve_when}
        BEGIN
            UPDATE orders
               SET {preserve_set}
             WHERE rowid = NEW.rowid;
        END
        """
    )

    trigger_assignments = _recurring_attribution_assignments(
        "NEW", shared_columns, touchpoint_columns
    )
    conn.execute("DROP TRIGGER IF EXISTS trg_orders_backfill_recurring_after_insert")
    conn.execute(
        f"""
        CREATE TRIGGER trg_orders_backfill_recurring_after_insert
        AFTER INSERT ON orders
        WHEN COALESCE(NEW.is_recurring, 0) = 1
         AND COALESCE(NEW.customer_key, '') != ''
        BEGIN
            UPDATE orders
               SET {trigger_assignments}
             WHERE rowid = NEW.rowid;
        END
        """
    )

    conn.execute("DROP TRIGGER IF EXISTS trg_orders_backfill_recurring_after_update")
    conn.execute(
        f"""
        CREATE TRIGGER trg_orders_backfill_recurring_after_update
        AFTER UPDATE OF is_recurring, customer_key, ts ON orders
        WHEN COALESCE(NEW.is_recurring, 0) = 1
         AND COALESCE(NEW.customer_key, '') != ''
        BEGIN
            UPDATE orders
               SET {trigger_assignments}
             WHERE rowid = NEW.rowid;
        END
        """
    )


def ensure_campaign_settings(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS campaign_settings (
            platform    TEXT NOT NULL,
            campaign_id TEXT NOT NULL,
            tracked     INTEGER NOT NULL DEFAULT 1,
            note        TEXT DEFAULT '',
            updated_at  TEXT,
            PRIMARY KEY (platform, campaign_id)
        );
        """
    )
    conn.executemany(
        """
        INSERT OR IGNORE INTO campaign_settings
            (platform, campaign_id, tracked, note, updated_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        DEFAULT_CAMPAIGN_SETTINGS,
    )


def ensure_customer_identities(conn: sqlite3.Connection) -> None:
    """Create the contact book that maps a customer_key back to a person.

    ``customer_key`` is a hash by design, so every ingestion path threw the
    email and name away after computing it and the CRM could only show the
    hash. This table keeps the human-readable identity beside the key.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS customer_identities (
            customer_key TEXT PRIMARY KEY,
            email TEXT DEFAULT '',
            name TEXT DEFAULT '',
            phone TEXT DEFAULT '',
            source TEXT DEFAULT '',
            updated_at TEXT
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_customer_identities_email "
        "ON customer_identities(email)"
    )


def upsert_customer_identity(
    conn: sqlite3.Connection,
    customer_key: str,
    *,
    email: str = "",
    name: str = "",
    phone: str = "",
    source: str = "",
    updated_at: str | None = None,
) -> bool:
    """Record what we know about a customer, never unlearning a known field.

    Sources disagree about how much they carry — a Stripe charge may have an
    email but no name while the GHL contact has both — so each write fills the
    gaps it can and leaves populated fields alone. Callers run
    ``ensure_customer_identities`` once per connection first; this is called
    per row and must stay free of DDL.
    """
    key = str(customer_key or "").strip()
    if not key:
        return False

    email = str(email or "").strip().lower()
    name = " ".join(str(name or "").split())
    phone = str(phone or "").strip()
    if not (email or name or phone):
        return False

    stamp = updated_at or datetime.now(timezone.utc).replace(microsecond=0).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    conn.execute(
        """
        INSERT INTO customer_identities (customer_key, email, name, phone, source, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(customer_key) DO UPDATE SET
            email = CASE WHEN COALESCE(excluded.email, '') != '' THEN excluded.email
                         ELSE customer_identities.email END,
            name = CASE WHEN COALESCE(excluded.name, '') != '' THEN excluded.name
                        ELSE customer_identities.name END,
            phone = CASE WHEN COALESCE(excluded.phone, '') != '' THEN excluded.phone
                         ELSE customer_identities.phone END,
            source = CASE WHEN COALESCE(excluded.source, '') != '' THEN excluded.source
                          ELSE customer_identities.source END,
            updated_at = excluded.updated_at
        """,
        (key, email, name, phone, str(source or ""), stamp),
    )
    return True


def ensure_refund_log(conn: sqlite3.Connection) -> None:
    """Create the timestamped refund ledger used by historical reports."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS refund_log (
            id TEXT PRIMARY KEY,
            ts TEXT,
            order_id TEXT,
            customer_key TEXT,
            type TEXT,
            amount TEXT,
            reason TEXT,
            source TEXT
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_refund_log_order_id_ts "
        "ON refund_log(order_id, ts)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_refund_log_ts ON refund_log(ts)")


def ensure_report_indexes(conn: sqlite3.Connection, tables: set[str] | None = None) -> None:
    """Cover the report's two heaviest scans with composite indexes.

    Both count distinct sessions over a date window and group by a second
    column; with only the single-column ``ts`` index they read every matching
    row from the table. Ordering the index ts-first keeps the range scan and
    makes the grouping column and session id available without touching the
    table.
    """
    if tables is None:
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    if "touchpoints" in tables:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_touchpoints_ts_platform_session "
            "ON touchpoints(ts, platform, session_id)"
        )
    if "sessions" in tables:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sessions_ts_landing_session "
            "ON sessions(ts, landing_page, session_id)"
        )


def apply_migrations(conn: sqlite3.Connection) -> None:
    """Apply all idempotent schema upgrades on an open connection."""
    tables = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if "orders" in tables:
        ensure_order_semantics(conn)
        _dedupe_and_unique_index(
            conn, "orders", "order_id", "uq_orders_order_id", "idx_orders_order_id"
        )
    if "conversions" in tables:
        # No pre-existing conversion_id index to supersede (pass a no-op legacy
        # name); the order_id lookup index is left untouched.
        _dedupe_and_unique_index(
            conn,
            "conversions",
            "conversion_id",
            "uq_conversions_conversion_id",
            "idx_conversions_conversion_id_legacy",
        )
    if "orders" in tables:
        ensure_order_attribution_integrity(conn)
    ensure_campaign_settings(conn)
    ensure_refund_log(conn)
    ensure_customer_identities(conn)
    ensure_report_indexes(conn, tables)


def _split_sql_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_single_quote = False
    index = 0
    while index < len(sql_text):
        char = sql_text[index]
        if char == "'":
            current.append(char)
            if in_single_quote and index + 1 < len(sql_text) and sql_text[index + 1] == "'":
                current.append("'")
                index += 2
                continue
            in_single_quote = not in_single_quote
        elif char == ";" and not in_single_quote:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
        else:
            current.append(char)
        index += 1
    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


_SCHEMA_STATE_TABLE = "public.schema_state"


def _schema_fingerprint(schema_text: str) -> str:
    return hashlib.sha256(schema_text.encode("utf-8")).hexdigest()[:32]


def _applied_schema_fingerprint(conn: Any) -> str:
    """Return the fingerprint of the schema already applied, or ''.

    Uses ``to_regclass`` rather than a plain SELECT so a warehouse that predates
    the marker table answers with NULL instead of raising and poisoning the
    transaction.
    """
    try:
        row = conn.execute(
            f"SELECT to_regclass('{_SCHEMA_STATE_TABLE}') IS NOT NULL AS present"
        ).fetchone()
        present = bool(row["present"] if isinstance(row, Mapping) else row[0])
        if not present:
            return ""
        row = conn.execute(
            f"SELECT value FROM {_SCHEMA_STATE_TABLE} WHERE key = 'initial_schema'"
        ).fetchone()
        if row is None:
            return ""
        return str((row["value"] if isinstance(row, Mapping) else row[0]) or "")
    except Exception:
        return ""


def _ensure_postgres_schema(database_url: str) -> None:
    """Apply the Postgres schema, skipping the replay when it is already current.

    Every statement in the schema file is `IF NOT EXISTS`, so replaying it was
    harmless — but it is 40-odd round trips to the warehouse, and it ran on
    every cold start of a serverless instance, delaying the first request of
    each one. The fingerprint check costs two queries instead.
    """
    schema_text = _POSTGRES_SCHEMA.read_text(encoding="utf-8")
    fingerprint = _schema_fingerprint(schema_text)

    with connect(database_url) as conn:
        if _applied_schema_fingerprint(conn) == fingerprint:
            return

        for statement in _split_sql_statements(schema_text):
            conn.execute(statement)
        conn.execute(
            f"""CREATE TABLE IF NOT EXISTS {_SCHEMA_STATE_TABLE} (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )"""
        )
        conn.execute(
            f"""INSERT INTO {_SCHEMA_STATE_TABLE} (key, value, updated_at)
                VALUES ('initial_schema', %s, now())
                ON CONFLICT (key) DO UPDATE
                    SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at""",
            (fingerprint,),
        )
        conn.commit()


def ensure_schema(db_path: str) -> None:
    """Idempotently upgrade an existing database; memoized per process+path."""
    with _ensured_lock:
        if db_path in _ensured_paths:
            return
    try:
        if is_postgres_dsn(db_path):
            _ensure_postgres_schema(db_path)
            _last_migration_error.pop(db_path, None)
            with _ensured_lock:
                _ensured_paths.add(db_path)
            return
        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA busy_timeout=5000;")
            apply_migrations(conn)
            conn.commit()
    except Exception as exc:
        # Never let a migration hiccup crash startup; the individual lazy
        # ensure_* helpers will retry on first use. But DO record it — a failed
        # migration leaves report queries referencing columns that never got
        # added, which then silently zeroes the whole report downstream.
        logger.exception("ensure_schema migration failed for %s", db_path)
        _last_migration_error[db_path] = str(exc)
        return
    _last_migration_error.pop(db_path, None)
    with _ensured_lock:
        _ensured_paths.add(db_path)


def last_migration_error(db_path: str) -> str | None:
    """Return the most recent migration failure message for ``db_path`` (or None)."""
    return _last_migration_error.get(db_path)
