"""Tests for the backend-agnostic schema introspection helpers."""

from __future__ import annotations

import sqlite3

import pytest

from attributionops import dbmeta


@pytest.fixture
def sqlite_db(tmp_path, monkeypatch):
    for var in ("DATABASE_URL", "POSTGRES_URL", "ATTRIBUTIONOPS_DATABASE_URL", "SUPABASE_DB_URL"):
        monkeypatch.delenv(var, raising=False)
    p = tmp_path / "meta.sqlite"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE orders (order_id TEXT, gross TEXT)")
    con.execute("CREATE TABLE spend (platform TEXT)")
    con.commit()
    con.close()
    return str(p)


def test_list_tables_sqlite(sqlite_db):
    tables = dbmeta.list_tables(sqlite_db)
    assert "orders" in tables and "spend" in tables


def test_table_exists_sqlite(sqlite_db):
    assert dbmeta.table_exists(sqlite_db, "orders") is True
    assert dbmeta.table_exists(sqlite_db, "does_not_exist") is False


def test_table_columns_sqlite(sqlite_db):
    assert dbmeta.table_columns(sqlite_db, "orders") == {"order_id", "gross"}


class _R:
    def __init__(self, rows):
        self.rows = rows


def test_dbmeta_postgres_branches(monkeypatch):
    monkeypatch.setattr(dbmeta, "is_postgres", lambda: True)

    def fake_query(db, sql, params=None):
        if "information_schema.tables" in sql:
            return _R([{"name": "orders"}, {"name": "spend"}])
        if "to_regclass" in sql:
            return _R([{"present": True}])
        if "information_schema.columns" in sql:
            return _R([{"name": "a"}, {"name": "b"}])
        return _R([])

    monkeypatch.setattr(dbmeta, "query", fake_query)
    assert dbmeta.list_tables("x") == ["orders", "spend"]
    assert dbmeta.table_exists("x", "orders") is True
    assert dbmeta.table_columns("x", "t") == {"a", "b"}

    monkeypatch.setattr(dbmeta, "query", lambda *a, **k: _R([{"present": False}]))
    assert dbmeta.table_exists("x", "orders") is False
