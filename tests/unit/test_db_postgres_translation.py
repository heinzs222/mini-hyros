"""Unit tests for the SQLite->Postgres SQL translation in attributionops.db.

The warehouse's SQL is written in SQLite's dialect and translated at runtime when
a Postgres (Supabase) DSN is configured. That translator is the highest-risk code
in the data layer — every report, sync and webhook query passes through it — so it
is exercised here directly as pure string functions (no live Postgres needed).
"""

from __future__ import annotations

import pytest

from attributionops.db import (
    _postgres_special_sql,
    _replace_cast_real,
    _replace_date_functions,
    _replace_insert_or_ignore,
    _replace_insert_or_replace,
    _replace_rowid,
    _translate_placeholders,
    _translate_postgres_sql,
    db_label,
    is_postgres_dsn,
    using_postgres,
)


# ── DSN detection / redaction ────────────────────────────────────────────────
@pytest.mark.parametrize(
    "value,expected",
    [
        ("postgres://u:p@h:5432/db", True),
        ("postgresql://u:p@h:5432/db", True),
        ("POSTGRESQL://u:p@h/db", True),
        ("data/live/attributionops.sqlite", False),
        ("", False),
        (None, False),
    ],
)
def test_is_postgres_dsn(value, expected):
    assert is_postgres_dsn(value) is expected


def test_using_postgres_reads_env(monkeypatch):
    for var in ("SUPABASE_DB_URL", "DATABASE_URL"):
        monkeypatch.delenv(var, raising=False)
    assert using_postgres() is False
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h:5432/db")
    assert using_postgres() is True


def test_db_label_redacts_password():
    label = db_label("postgresql://postgres:sup3rsecret@db.example.co:6543/postgres")
    assert "sup3rsecret" not in label
    assert "***" in label
    assert "db.example.co:6543" in label


def test_db_label_passes_through_sqlite_path():
    assert db_label("data/live/attributionops.sqlite") == "data/live/attributionops.sqlite"


# ── INSERT OR IGNORE / REPLACE -> ON CONFLICT ────────────────────────────────
def test_insert_or_ignore_becomes_on_conflict_do_nothing():
    out = _replace_insert_or_ignore("INSERT OR IGNORE INTO orders (order_id) VALUES (?)")
    assert out.startswith("INSERT INTO orders")
    assert out.rstrip().endswith("ON CONFLICT DO NOTHING")


def test_insert_or_ignore_preserves_trailing_semicolon():
    out = _replace_insert_or_ignore("INSERT OR IGNORE INTO orders (order_id) VALUES (?);")
    assert out.rstrip().endswith(";")
    assert "ON CONFLICT DO NOTHING" in out


def test_insert_or_ignore_leaves_existing_on_conflict_alone():
    sql = "INSERT OR IGNORE INTO orders (order_id) VALUES (?) ON CONFLICT DO NOTHING"
    assert _replace_insert_or_ignore(sql).count("ON CONFLICT") == 1


def test_insert_or_replace_uses_registered_conflict_target():
    out = _replace_insert_or_replace(
        "INSERT OR REPLACE INTO ad_names (platform, entity_type, entity_id, name) VALUES (?,?,?,?)"
    )
    assert "ON CONFLICT (platform, entity_type, entity_id) DO UPDATE SET" in out
    assert "name = EXCLUDED.name" in out
    # conflict-key columns are never self-assigned
    assert "platform = EXCLUDED.platform" not in out


def test_insert_or_replace_single_key_table():
    out = _replace_insert_or_replace(
        "INSERT OR REPLACE INTO platform_tokens (platform, access_token) VALUES (?,?)"
    )
    assert "ON CONFLICT (platform) DO UPDATE SET access_token = EXCLUDED.access_token" in out


def test_insert_or_replace_all_columns_are_keys_becomes_do_nothing():
    out = _replace_insert_or_replace("INSERT OR REPLACE INTO platform_tokens (platform) VALUES (?)")
    assert out.rstrip().endswith("ON CONFLICT (platform) DO NOTHING")


def test_insert_or_replace_unknown_table_falls_back_to_first_column():
    out = _replace_insert_or_replace("INSERT OR REPLACE INTO widgets (id, label) VALUES (?,?)")
    assert "ON CONFLICT (id) DO UPDATE SET label = EXCLUDED.label" in out


def test_non_upsert_sql_is_untouched():
    sql = "SELECT 1 FROM orders"
    assert _replace_insert_or_replace(sql) == sql
    assert _replace_insert_or_ignore(sql) == sql


# ── date/time function translation ───────────────────────────────────────────
def test_ifnull_becomes_coalesce():
    assert "COALESCE(a, 0)" in _replace_date_functions("SELECT IFNULL(a, 0) FROM t")


def test_datetime_and_date_now():
    assert "CURRENT_TIMESTAMP" in _replace_date_functions("SELECT datetime('now')")
    assert "CURRENT_DATE" in _replace_date_functions("SELECT date('now')")


def test_date_substr_collapses_to_substr():
    out = _replace_date_functions("WHERE date(substr(c.ts, 1, 10)) >= ?")
    assert "substr(c.ts, 1, 10)" in out
    assert "date(substr" not in out.lower()


def test_date_around_placeholder_is_dropped():
    assert _replace_date_functions("WHERE d >= date(?)").rstrip().endswith(">= ?")
    assert _replace_date_functions("WHERE d >= date(:start)").rstrip().endswith(">= :start")


def test_strftime_epoch_forms():
    assert "EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)" in _replace_date_functions(
        "SELECT strftime('%s','now')"
    )
    assert "EXTRACT(EPOCH FROM t.ts)" in _replace_date_functions(
        "SELECT CAST(strftime('%s', t.ts) AS INTEGER)"
    )


def test_strftime_with_interval_modifier():
    """The tracking source-lookback window: strftime(fmt, ts, '-N days')."""
    out = _replace_date_functions(
        "WHERE t.ts >= strftime('%Y-%m-%dT%H:%M:%SZ', :start_utc, :lookback)"
    )
    assert "to_char(" in out
    assert "::timestamptz" in out and "::interval" in out
    assert "strftime" not in out


# ── rowid ────────────────────────────────────────────────────────────────────
def test_rowid_translation_bare_and_qualified():
    assert _replace_rowid("ORDER BY ts, rowid DESC") == "ORDER BY ts, ctid::text DESC"
    assert _replace_rowid("ORDER BY t.rowid") == "ORDER BY t.ctid::text"


# ── lenient CAST(x AS REAL) ──────────────────────────────────────────────────
def test_cast_as_real_is_made_empty_string_safe():
    """SQLite yields 0 for CAST('' AS REAL); bare Postgres raises. The rewrite
    must coalesce empty text to '0' so text money columns stay summable."""
    out = _replace_cast_real("SELECT CAST(cost AS REAL) FROM spend")
    assert "NULLIF((cost)::text, '')" in out
    assert "DOUBLE PRECISION" in out


def test_cast_as_real_with_coalesce_inner_expression():
    out = _replace_cast_real("SELECT CAST(COALESCE(s.cost, '0') AS REAL) FROM spend s")
    assert "COALESCE(s.cost, '0')" in out
    assert "DOUBLE PRECISION" in out


def test_non_real_cast_is_preserved():
    out = _replace_cast_real("SELECT CAST(id AS TEXT) FROM t")
    assert out == "SELECT CAST(id AS TEXT) FROM t"


# ── placeholders ─────────────────────────────────────────────────────────────
def test_qmark_and_named_placeholders():
    sql, params = _translate_placeholders("SELECT * FROM t WHERE a = ? AND b = :b", {"b": 1})
    assert sql == "SELECT * FROM t WHERE a = %s AND b = %(b)s"
    assert params == {"b": 1}


def test_placeholders_inside_string_literals_are_not_touched():
    sql, _ = _translate_placeholders("SELECT '?' AS q, ':x' AS n WHERE a = ?", None)
    assert sql == "SELECT '?' AS q, ':x' AS n WHERE a = %s"


def test_postgres_cast_colons_are_not_mistaken_for_params():
    sql, _ = _translate_placeholders("SELECT ctid::text FROM t", None)
    assert sql == "SELECT ctid::text FROM t"


# ── sqlite_master / PRAGMA interception ──────────────────────────────────────
def test_pragma_table_info_maps_to_information_schema():
    out = _postgres_special_sql("PRAGMA table_info(spend)")
    assert out is not None
    sql, params = out
    assert "information_schema.columns" in sql
    assert params == ("spend",)
    assert "AS name" in sql  # callers read row["name"] / row[1]


def test_sqlite_master_table_listing_maps_to_information_schema():
    out = _postgres_special_sql("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    assert out is not None
    sql, _ = out
    assert "information_schema.tables" in sql
    assert "ORDER BY table_name" in sql


def test_sqlite_master_existence_probe_maps_to_select_1():
    out = _postgres_special_sql(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='refund_log'"
    )
    assert out is not None
    sql, params = out
    assert "SELECT 1" in sql and "information_schema.tables" in sql
    assert params == ("refund_log",)


def test_ordinary_sql_is_not_intercepted():
    assert _postgres_special_sql("SELECT * FROM orders") is None


# ── end-to-end ───────────────────────────────────────────────────────────────
def test_translate_end_to_end_combines_rules():
    sql, params = _translate_postgres_sql(
        "INSERT OR IGNORE INTO orders (order_id, gross) VALUES (?, ?)", ["o1", "5"]
    )
    assert sql.startswith("INSERT INTO orders")
    assert "%s" in sql
    assert "ON CONFLICT DO NOTHING" in sql
    assert params == ["o1", "5"]


def test_translate_end_to_end_select_with_cast_and_rowid():
    sql, _ = _translate_postgres_sql(
        "SELECT CAST(gross AS REAL) AS g, rowid FROM orders WHERE ts > :since", {"since": "x"}
    )
    assert "DOUBLE PRECISION" in sql
    assert "ctid::text" in sql
    assert "%(since)s" in sql


def test_collate_nocase_is_stripped():
    sql, _ = _translate_postgres_sql("SELECT * FROM t ORDER BY name COLLATE NOCASE", None)
    assert "COLLATE" not in sql.upper()
