"""Unit tests for the psycopg-backed connection adapters in attributionops.db.

PostgresConnection/PostgresCursor stand in for a sqlite3 connection so the rest
of the codebase can stay dialect-agnostic. They are exercised here against a fake
psycopg driver, so no live Postgres is required.
"""

from __future__ import annotations

import sys
import types

import pytest

from attributionops import db as dbmod
from attributionops.db import PostgresConnection, PostgresCursor


# ── Fake psycopg ─────────────────────────────────────────────────────────────
class _FakeCursor:
    def __init__(self, conn):
        self._conn = conn
        self.rows: list = []
        self.description = None
        self.rowcount = -1
        self.closed = False

    def execute(self, sql, params=None):
        self._conn.executed.append((sql, params))
        if "LASTVAL" in sql.upper():
            self.rows = [{"value": 42}]
        elif "pg_constraint" in sql:
            self.rows = [{"columns": self._conn.conflict_result}]
        return self

    def executemany(self, sql, seq):
        self._conn.executemany_calls.append((sql, list(seq)))
        return self

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)

    def close(self):
        self.closed = True

    def __iter__(self):
        return iter(self.rows)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


class _FakeRawConn:
    def __init__(self, url, row_factory=None, prepare_threshold=None):
        self.url = url
        self.row_factory = row_factory
        self.prepare_threshold = prepare_threshold
        self.executed: list = []
        self.executemany_calls: list = []
        self.committed = 0
        self.rolledback = 0
        self.closed = False
        self.conflict_result = ["platform"]
        self.last_cursor: _FakeCursor | None = None

    def cursor(self):
        cur = _FakeCursor(self)
        self.last_cursor = cur
        return cur

    def commit(self):
        self.committed += 1

    def rollback(self):
        self.rolledback += 1

    def close(self):
        self.closed = True


@pytest.fixture
def fake_psycopg(monkeypatch):
    made: list[_FakeRawConn] = []

    def _connect(url, row_factory=None, prepare_threshold=None):
        conn = _FakeRawConn(url, row_factory, prepare_threshold)
        made.append(conn)
        return conn

    psycopg_mod = types.ModuleType("psycopg")
    psycopg_mod.connect = _connect
    rows_mod = types.ModuleType("psycopg.rows")
    rows_mod.dict_row = object()
    monkeypatch.setitem(sys.modules, "psycopg", psycopg_mod)
    monkeypatch.setitem(sys.modules, "psycopg.rows", rows_mod)
    return made


def test_connection_sets_pooler_safe_options_and_timeout(fake_psycopg):
    conn = PostgresConnection("postgresql://u:p@h/db")
    raw = fake_psycopg[0]
    # prepared statements disabled -> safe behind the Supabase transaction pooler
    assert raw.prepare_threshold is None
    assert raw.row_factory is not None
    assert any("statement_timeout" in sql for sql, _ in raw.executed)
    assert isinstance(conn.cursor(), PostgresCursor)


def test_execute_translates_sql_before_sending(fake_psycopg):
    conn = PostgresConnection("postgresql://u:p@h/db")
    conn.execute("INSERT OR IGNORE INTO orders (order_id) VALUES (?)", ["o1"])
    sql, params = fake_psycopg[0].executed[-1]
    assert sql.startswith("INSERT INTO orders")
    assert "ON CONFLICT DO NOTHING" in sql
    assert "%s" in sql
    assert params == ["o1"]


def test_execute_without_params_omits_values(fake_psycopg):
    conn = PostgresConnection("postgresql://u:p@h/db")
    conn.execute("SELECT 1 FROM orders")
    sql, params = fake_psycopg[0].executed[-1]
    assert params is None


def test_executemany_translates_once_and_forwards_all_rows(fake_psycopg):
    conn = PostgresConnection("postgresql://u:p@h/db")
    conn.executemany("INSERT OR IGNORE INTO orders (order_id) VALUES (?)", [["a"], ["b"]])
    sql, seq = fake_psycopg[0].executemany_calls[-1]
    assert "ON CONFLICT DO NOTHING" in sql
    assert seq == [["a"], ["b"]]


def test_executemany_with_no_rows_is_a_noop(fake_psycopg):
    conn = PostgresConnection("postgresql://u:p@h/db")
    conn.executemany("INSERT INTO orders (order_id) VALUES (?)", [])
    assert fake_psycopg[0].executemany_calls == []


def test_cursor_proxies_fetch_and_metadata(fake_psycopg):
    conn = PostgresConnection("postgresql://u:p@h/db")
    cur = conn.execute("SELECT 1 FROM orders")
    cur._cursor.rows = [{"a": 1}, {"a": 2}]
    assert cur.fetchone() == {"a": 1}
    assert cur.fetchall() == [{"a": 1}, {"a": 2}]
    assert list(cur) == [{"a": 1}, {"a": 2}]
    assert cur.rowcount == -1
    assert cur.description is None
    cur.close()
    assert cur._cursor.closed


def test_cursor_context_manager_closes(fake_psycopg):
    conn = PostgresConnection("postgresql://u:p@h/db")
    with conn.cursor() as cur:
        inner = cur._cursor
    assert inner.closed


def test_lastrowid_uses_lastval(fake_psycopg):
    conn = PostgresConnection("postgresql://u:p@h/db")
    assert conn.cursor().lastrowid == 42


def test_conflict_columns_queries_and_caches(fake_psycopg):
    conn = PostgresConnection("postgresql://u:p@h/db")
    raw = fake_psycopg[0]
    assert conn.conflict_columns("public.platform_tokens") == ["platform"]
    calls = len([1 for sql, _ in raw.executed if "pg_constraint" in sql])
    # second call is served from cache
    assert conn.conflict_columns("platform_tokens") == ["platform"]
    assert len([1 for sql, _ in raw.executed if "pg_constraint" in sql]) == calls


def test_conflict_columns_rolls_back_and_returns_empty_on_error(fake_psycopg, monkeypatch):
    conn = PostgresConnection("postgresql://u:p@h/db")
    raw = fake_psycopg[0]

    def boom():
        raise RuntimeError("no such table")

    monkeypatch.setattr(raw, "cursor", boom)
    assert conn.conflict_columns("missing") == []
    assert raw.rolledback == 1


def test_context_manager_commits_on_success(fake_psycopg):
    with PostgresConnection("postgresql://u:p@h/db"):
        pass
    raw = fake_psycopg[0]
    assert raw.committed == 1 and raw.closed and raw.rolledback == 0


def test_context_manager_rolls_back_on_error(fake_psycopg):
    with pytest.raises(ValueError):
        with PostgresConnection("postgresql://u:p@h/db"):
            raise ValueError("boom")
    raw = fake_psycopg[0]
    assert raw.rolledback == 1 and raw.committed == 0 and raw.closed


def test_commit_rollback_close_and_executescript(fake_psycopg):
    conn = PostgresConnection("postgresql://u:p@h/db")
    conn.executescript("CREATE TABLE t (a text)")
    conn.commit()
    conn.rollback()
    conn.close()
    raw = fake_psycopg[0]
    assert any("CREATE TABLE t" in sql for sql, _ in raw.executed)
    assert raw.committed == 1 and raw.rolledback == 1 and raw.closed


def test_row_factory_setter_is_inert(fake_psycopg):
    """Callers copied from the sqlite3 path may assign row_factory; it must not blow up."""
    conn = PostgresConnection("postgresql://u:p@h/db")
    conn.row_factory = object()
    assert conn.row_factory is None


def test_connect_routes_dsn_to_postgres(fake_psycopg, monkeypatch):
    for var in ("SUPABASE_DB_URL", "DATABASE_URL"):
        monkeypatch.delenv(var, raising=False)
    assert isinstance(dbmod.connect("postgresql://u:p@h/db"), PostgresConnection)


def test_connect_routes_to_postgres_when_env_configured(fake_psycopg, monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h/db")
    assert isinstance(dbmod.connect(str(tmp_path / "ignored.sqlite")), PostgresConnection)


def test_connect_uses_sqlite_without_postgres_env(monkeypatch, tmp_path):
    import sqlite3

    for var in ("SUPABASE_DB_URL", "DATABASE_URL"):
        monkeypatch.delenv(var, raising=False)
    conn = dbmod.connect(str(tmp_path / "w.sqlite"))
    assert isinstance(conn, sqlite3.Connection)
    conn.close()


# ── read path row mapping ────────────────────────────────────────────────────
class _Col:
    def __init__(self, name):
        self.name = name


def test_column_names_supports_named_and_tuple_descriptions():
    assert dbmod._column_names([_Col("a"), _Col("b")]) == ["a", "b"]
    assert dbmod._column_names([("a", None), ("b", None)]) == ["a", "b"]
    assert dbmod._column_names(None) == []


def test_dict_rows_handles_mappings_and_tuples():
    assert dbmod._dict_rows([{"a": 1}], ["a"]) == [{"a": 1}]
    assert dbmod._dict_rows([(1, 2)], ["a", "b"]) == [{"a": 1, "b": 2}]


def test_requires_existing_sqlite_file_only_for_sqlite(monkeypatch):
    for var in ("SUPABASE_DB_URL", "DATABASE_URL"):
        monkeypatch.delenv(var, raising=False)
    assert dbmod._requires_existing_sqlite_file("x.sqlite") is True
    assert dbmod._requires_existing_sqlite_file("postgresql://u@h/db") is False
    monkeypatch.setenv("DATABASE_URL", "postgresql://u@h/db")
    assert dbmod._requires_existing_sqlite_file("x.sqlite") is False


def test_query_missing_sqlite_file_raises(monkeypatch, tmp_path):
    for var in ("SUPABASE_DB_URL", "DATABASE_URL"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(FileNotFoundError):
        dbmod.query(str(tmp_path / "absent.sqlite"), "SELECT 1")
    with pytest.raises(FileNotFoundError):
        dbmod.sql_rows(str(tmp_path / "absent.sqlite"), "SELECT 1")


def test_query_over_postgres_maps_rows_and_redacts_label(fake_psycopg, monkeypatch):
    dsn = "postgresql://postgres:hunter2@db.example.co:6543/postgres"
    monkeypatch.setenv("DATABASE_URL", dsn)

    real_cursor = _FakeCursor.execute

    def _execute(self, sql, params=None):
        real_cursor(self, sql, params)
        self.description = [_Col("n")]
        self.rows = [{"n": 7}]
        return self

    monkeypatch.setattr(_FakeCursor, "execute", _execute)

    result = dbmod.query(dsn, "SELECT COUNT(*) AS n FROM orders")
    assert result.columns == ["n"]
    assert result.rows == [{"n": 7}]
    assert result.row_count == 1
    assert result.as_dicts() == [{"n": 7}]
    # the label must never leak the password
    assert "hunter2" not in result.db_path
    assert dbmod.sql_rows(dsn, "SELECT COUNT(*) AS n FROM orders") == [{"n": 7}]
    assert list(dbmod.query_iter(dsn, "SELECT COUNT(*) AS n FROM orders")) == [{"n": 7}]
