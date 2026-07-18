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

import logging
import sqlite3
import threading
from attributionops.db import is_postgres

logger = logging.getLogger(__name__)

_ensured_lock = threading.Lock()
_ensured_paths: set[str] = set()
# Last migration failure per DB path, surfaced via /api/health so a persistently
# broken migration (the root cause of downstream all-zero reports) is visible in
# monitoring rather than only in logs that may not be tailed.
_last_migration_error: dict[str, str] = {}


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


def ensure_schema(db_path: str) -> None:
    """Idempotently upgrade an existing database; memoized per process+path."""
    if is_postgres():
        # Postgres schema is provisioned once by migrations/postgres/0001_schema.sql;
        # the SQLite-style CREATE/ALTER/PRAGMA below never runs against Postgres.
        return
    with _ensured_lock:
        if db_path in _ensured_paths:
            return
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA busy_timeout=5000;")
            apply_migrations(conn)
            conn.commit()
    except sqlite3.Error as exc:
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
