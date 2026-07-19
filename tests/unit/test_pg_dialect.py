"""Unit tests for the SQLite→Postgres SQL translator."""

from __future__ import annotations

from attributionops.pg_dialect import translate


def test_qmark_placeholders_become_percent_s():
    assert translate("SELECT * FROM t WHERE a = ? AND b = ?") == (
        "SELECT * FROM t WHERE a = %s AND b = %s"
    )


def test_named_placeholders_become_pyformat():
    assert translate("SELECT * FROM t WHERE a = :start AND b = :end_x") == (
        "SELECT * FROM t WHERE a = %(start)s AND b = %(end_x)s"
    )


def test_literal_percent_is_escaped():
    out = translate("SELECT * FROM t WHERE name LIKE '%foo%'")
    assert out == "SELECT * FROM t WHERE name LIKE '%%foo%%'"


def test_percent_inside_string_with_placeholder_outside():
    out = translate("SELECT * FROM t WHERE name LIKE '%x%' AND id = ?")
    assert out == "SELECT * FROM t WHERE name LIKE '%%x%%' AND id = %s"


def test_question_mark_inside_string_is_preserved():
    out = translate("SELECT '?' AS lit WHERE a = ?")
    assert out == "SELECT '?' AS lit WHERE a = %s"


def test_insert_or_ignore_becomes_do_nothing():
    out = translate("INSERT OR IGNORE INTO orders (order_id) VALUES (?)")
    assert out.startswith("INSERT INTO orders (order_id) VALUES (%s)")
    assert out.rstrip().endswith("ON CONFLICT DO NOTHING")


def test_replace_into_becomes_on_conflict_do_update():
    out = translate(
        "INSERT OR REPLACE INTO ad_names (platform, entity_type, entity_id, name) "
        "VALUES (?, ?, ?, ?)"
    )
    assert out.startswith(
        "INSERT INTO ad_names (platform, entity_type, entity_id, name) VALUES"
    )
    assert "ON CONFLICT (platform, entity_type, entity_id) DO UPDATE SET" in out
    assert "name = EXCLUDED.name" in out
    # Conflict-key columns are never in the SET list.
    assert "platform = EXCLUDED.platform" not in out


def test_replace_into_platform_tokens():
    out = translate(
        "REPLACE INTO platform_tokens (platform, access_token, refresh_token) "
        "VALUES (?, ?, ?)"
    )
    assert "ON CONFLICT (platform) DO UPDATE SET" in out
    assert "access_token = EXCLUDED.access_token" in out
    assert "refresh_token = EXCLUDED.refresh_token" in out


def test_cast_as_real_becomes_lenient_helper():
    out = translate("SELECT CAST(cost AS REAL) FROM spend")
    assert out == "SELECT sqlite_real(cost) FROM spend"


def test_cast_as_real_with_coalesce():
    out = translate("SELECT CAST(COALESCE(s.cost, '0') AS REAL) FROM spend s")
    assert out == "SELECT sqlite_real(COALESCE(s.cost, '0')) FROM spend s"


def test_nested_cast():
    out = translate("SELECT CAST(CAST(net AS REAL) AS INTEGER) FROM orders")
    assert out == "SELECT sqlite_int(sqlite_real(net)) FROM orders"


def test_cast_as_text_is_left_untouched():
    out = translate("SELECT CAST(id AS TEXT) FROM t")
    assert out == "SELECT CAST(id AS TEXT) FROM t"


def test_rowid_becomes_ctid():
    out = translate("SELECT rowid FROM t ORDER BY ts, rowid")
    assert out == "SELECT ctid FROM t ORDER BY ts, ctid"


def test_rowid_substring_in_identifier_is_untouched():
    out = translate("SELECT crowid_x, t.rowid FROM t")
    assert "crowid_x" in out
    assert "t.ctid" in out


def test_cast_as_integer_becomes_sqlite_int():
    assert translate("SELECT CAST(x AS INTEGER) FROM t") == "SELECT sqlite_int(x) FROM t"


def test_replace_into_unknown_table_falls_back_to_do_nothing():
    out = translate("REPLACE INTO unknown_tbl (a, b) VALUES (?, ?)")
    assert out.startswith("INSERT INTO unknown_tbl (a, b) VALUES")
    assert out.rstrip().endswith("ON CONFLICT DO NOTHING")


def test_line_comment_percent_escaped_and_preserved():
    out = translate("SELECT 1 -- 50% done\nFROM t WHERE a = ?")
    assert "-- 50%% done" in out
    assert "a = %s" in out


def test_double_quoted_identifier_preserves_question_mark():
    out = translate('SELECT "weird?col" FROM t WHERE a = ?')
    assert '"weird?col"' in out
    assert out.rstrip().endswith("a = %s")


def test_escaped_single_quote_and_percent_in_string():
    out = translate("SELECT 'it''s a %' AS x FROM t WHERE a = ?")
    assert "'it''s a %%'" in out
    assert out.rstrip().endswith("a = %s")
