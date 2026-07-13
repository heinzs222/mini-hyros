"""Idempotent schema migrations shared by the DB initializer and app startup.

These run against both freshly-created databases (via ``scripts/init_empty_db.py``)
and any pre-existing database at process start (via ``ensure_schema``), so the
integrity constraints the ingestion layer relies on are guaranteed to exist:

* UNIQUE indexes on ``orders.order_id`` / ``conversions.conversion_id`` — the
  webhook/sync/pixel writers all use ``INSERT OR IGNORE`` for idempotency, which
  is a silent no-op without a UNIQUE constraint (duplicate deliveries would
  otherwise double-count revenue).
* the ``campaign_settings`` table backing the per-campaign tracked/excluded flag.

Everything here is idempotent and cheap to re-run; ``ensure_schema`` is memoized
per process+path so the app pays the cost at most once per database.
"""

from __future__ import annotations

import sqlite3
import threading

_ensured_lock = threading.Lock()
_ensured_paths: set[str] = set()


ORDER_SEMANTIC_COLUMNS: dict[str, str] = {
    "currency": "TEXT DEFAULT ''",
    "processor": "TEXT DEFAULT ''",
    "processor_customer_id": "TEXT DEFAULT ''",
    "payment_fingerprint": "TEXT DEFAULT ''",
    "product_key": "TEXT DEFAULT ''",
    "is_recurring": "INTEGER DEFAULT 0",
    "sale_group_id": "TEXT DEFAULT ''",
}


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
    ensure_campaign_settings(conn)


def ensure_schema(db_path: str) -> None:
    """Idempotently upgrade an existing database; memoized per process+path."""
    with _ensured_lock:
        if db_path in _ensured_paths:
            return
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA busy_timeout=5000;")
            apply_migrations(conn)
            conn.commit()
    except sqlite3.Error:
        # Never let a migration hiccup crash startup; the individual lazy
        # ensure_* helpers will retry on first use.
        return
    with _ensured_lock:
        _ensured_paths.add(db_path)
