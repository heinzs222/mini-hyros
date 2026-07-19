"""Unit tests for the psycopg connection adapters (attributionops.pg_conn).

These exercise the sqlite3-compatible adapter surface — hybrid rows, cursor/
connection delegation, SQL translation on execute, and the connect()/read_conn()
factories — against a *fake* psycopg connection, so no live Postgres is needed.
"""

from __future__ import annotations

import threading

import pytest

from attributionops import pg_conn
from attributionops.pg_conn import (
    Row,
    _Connection,
    _Cursor,
    _hybrid_row_factory,
    _norm_params,
)


# ── Row (positional + mapping, like sqlite3.Row) ─────────────────────────────
def test_row_positional_and_mapping_access():
    r = Row(["a", "b"], (1, 2))
    assert r[0] == 1 and r[1] == 2
    assert r["a"] == 1 and r["b"] == 2
    assert r[0:2] == (1, 2)
    assert r.get("a") == 1
    assert r.get("missing") is None
    assert r.get("missing", 9) == 9
    assert r.keys() == ["a", "b"]
    assert list(r) == [1, 2]  # iterates VALUES, matching sqlite3.Row
    assert len(r) == 2
    assert dict(r) == {"a": 1, "b": 2}
    assert "Row(" in repr(r)


# ── Fakes ────────────────────────────────────────────────────────────────────
class _Col:
    def __init__(self, name: str):
        self.name = name


class _FakeCursor:
    def __init__(self, rows=None, description=None, rowcount=0):
        self._rows = list(rows or [])
        self.description = description
        self.rowcount = rowcount
        self.executed: list = []
        self.executemany_calls: list = []
        self.closed = False

    def execute(self, sql, params):
        self.executed.append((sql, params))

    def executemany(self, sql, seq):
        self.executemany_calls.append((sql, list(seq)))

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def fetchmany(self, size=None):
        return list(self._rows[: (size or 1)])

    def __iter__(self):
        return iter(self._rows)

    def close(self):
        self.closed = True


class _FakeConn:
    def __init__(self, cursor=None):
        self._cursor = cursor if cursor is not None else _FakeCursor()
        self.row_factory = None
        self.committed = 0
        self.rolledback = 0
        self.closed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed += 1

    def rollback(self):
        self.rolledback += 1

    def close(self):
        self.closed = True


def test_norm_params():
    assert _norm_params(None) == ()
    lst = [1, 2]
    assert _norm_params(lst) is lst
    d = {"a": 1}
    assert _norm_params(d) is d


def test_hybrid_row_factory_builds_hybrid_rows():
    cur = _FakeCursor(description=[_Col("x"), _Col("y")])
    make = _hybrid_row_factory(cur)
    row = make((10, 20))
    assert isinstance(row, Row)
    assert row["x"] == 10 and row[1] == 20


# ── _Cursor ──────────────────────────────────────────────────────────────────
def test_cursor_translates_sql_and_delegates():
    fc = _FakeCursor(rows=[Row(["n"], (1,))], rowcount=3)
    cur = _Cursor(fc)
    assert cur.execute("SELECT * FROM t WHERE a = ? AND b = :b", [1]) is cur
    sql, params = fc.executed[-1]
    assert sql == "SELECT * FROM t WHERE a = %s AND b = %(b)s"
    assert params == [1]
    assert cur.fetchone()["n"] == 1
    assert cur.fetchall()[0]["n"] == 1
    assert cur.fetchmany(1)[0]["n"] == 1
    assert cur.rowcount == 3
    assert cur.lastrowid is None
    assert cur.description is None
    assert [r["n"] for r in cur] == [1]
    cur.close()
    assert fc.closed


def test_cursor_executemany_translates_upsert():
    fc = _FakeCursor()
    cur = _Cursor(fc)
    cur.executemany("INSERT OR IGNORE INTO t (a) VALUES (?)", [[1], [2]])
    sql, seq = fc.executemany_calls[-1]
    assert sql.startswith("INSERT INTO t (a) VALUES (%s)")
    assert "ON CONFLICT DO NOTHING" in sql
    assert seq == [[1], [2]]


# ── _Connection ──────────────────────────────────────────────────────────────
def test_connection_execute_returns_cursor():
    fconn = _FakeConn(_FakeCursor(rows=[Row(["x"], (5,))], rowcount=1))
    conn = _Connection(fconn)
    cur = conn.execute("SELECT ?", [1])
    assert isinstance(cur, _Cursor)
    assert cur.rowcount == 1
    assert conn.raw is fconn


def test_connection_context_manager_commits_on_success():
    fconn = _FakeConn()
    with _Connection(fconn) as c:
        c.execute("SELECT 1", None)
    assert fconn.committed == 1
    assert fconn.closed
    assert fconn.rolledback == 0


def test_connection_context_manager_rolls_back_on_error():
    fconn = _FakeConn()
    with pytest.raises(ValueError):
        with _Connection(fconn) as c:
            c.execute("SELECT 1", None)
            raise ValueError("boom")
    assert fconn.rolledback == 1
    assert fconn.committed == 0
    assert fconn.closed


def test_connection_commit_rollback_close_cursor_executemany():
    fconn = _FakeConn()
    conn = _Connection(fconn)
    conn.commit()
    conn.rollback()
    assert isinstance(conn.cursor(), _Cursor)
    conn.executemany("INSERT INTO t (a) VALUES (?)", [[1]])
    conn.close()
    assert fconn.committed == 1 and fconn.rolledback == 1 and fconn.closed


# ── connect() / read_conn() with a fake psycopg module ───────────────────────
class _FakePsycopg:
    def __init__(self):
        self.created: list = []

    def connect(self, dsn, **kw):
        conn = _FakeConn()
        self.created.append((dsn, kw))
        return conn


def test_connect_opens_transactional_pooler_safe_connection(monkeypatch):
    fake = _FakePsycopg()
    monkeypatch.setattr(pg_conn, "psycopg", fake)
    monkeypatch.setattr(pg_conn, "_local", threading.local())
    c = pg_conn.connect("dsn://x")
    assert isinstance(c, _Connection)
    dsn, kw = fake.created[-1]
    assert dsn == "dsn://x"
    assert kw["autocommit"] is False
    assert kw["prepare_threshold"] is None  # pooler-safe


def test_read_conn_caches_and_uses_autocommit(monkeypatch):
    fake = _FakePsycopg()
    monkeypatch.setattr(pg_conn, "psycopg", fake)
    monkeypatch.setattr(pg_conn, "_local", threading.local())
    r1 = pg_conn.read_conn("dsn://y")
    r2 = pg_conn.read_conn("dsn://y")
    assert r1 is r2  # cached per (thread, dsn)
    assert fake.created[-1][1]["autocommit"] is True
    assert len(fake.created) == 1


def test_read_conn_replaces_dead_cached_connection(monkeypatch):
    monkeypatch.setattr(pg_conn, "_local", threading.local())

    class _DeadCursor:
        def execute(self, *a):
            raise RuntimeError("connection dead")

        def close(self):
            pass

    made: list = []

    class _FakePsycopgFlaky:
        def connect(self, dsn, **kw):
            # First connection validates-dead; the second is healthy.
            cur = _DeadCursor() if not made else _FakeCursor(rows=[Row(["n"], (1,))])
            conn = _FakeConn(cursor=cur)
            made.append(conn)
            return conn

    monkeypatch.setattr(pg_conn, "psycopg", _FakePsycopgFlaky())
    first = pg_conn.read_conn("d")  # creates conn#0 (no validation on fresh insert)
    second = pg_conn.read_conn("d")  # validates conn#0 -> raises -> drop + recreate
    assert first is not second
    assert len(made) == 2
    assert made[0].closed  # the dead one was closed
