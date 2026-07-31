"""Reports cached in the warehouse are reusable by any server instance.

A serverless instance is rarely the same process twice — instances are recycled
between clicks and one page load fans out across several — so the in-memory
cache alone left almost every request rebuilding a report another instance had
already built.
"""

from __future__ import annotations

import pytest

from backend import report_cache as store


class _FakeConn:
    def __init__(self, rows: dict[str, str], fail_first_write: bool = False):
        self.rows = rows
        self.fail_first_write = fail_first_write
        self.executed: list[str] = []
        self.committed = 0
        self.rolled_back = 0

    def execute(self, sql, params=None):
        flat = " ".join(str(sql).split())
        self.executed.append(flat)
        if flat.startswith("SELECT payload"):
            key = params[0]
            value = self.rows.get(key)
            return _FakeCursor([{"payload": value}] if value is not None else [])
        if flat.startswith("INSERT INTO public.report_cache"):
            if self.fail_first_write:
                self.fail_first_write = False
                raise RuntimeError('relation "public.report_cache" does not exist')
            self.rows[params[0]] = params[1]
        if flat.startswith("DELETE FROM public.report_cache WHERE key IN"):
            return _FakeCursor([])
        if flat.startswith("DELETE FROM public.report_cache"):
            self.rows.clear()
        return _FakeCursor([])

    def commit(self):
        self.committed += 1

    def rollback(self):
        self.rolled_back += 1

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


@pytest.fixture
def pg(monkeypatch):
    """Present a Postgres warehouse backed by an in-memory fake."""
    conn = _FakeConn({})
    monkeypatch.setattr(store, "connect", lambda db: conn)
    monkeypatch.setattr(store, "is_postgres_dsn", lambda db: True)
    monkeypatch.delenv("REPORT_CACHE_SHARED", raising=False)
    return conn


DSN = "postgresql://u:p@h/db"


def test_a_report_stored_by_one_instance_is_read_by_another(pg):
    store.put(DSN, "k1", {"rows": [{"name": "meta"}]}, ttl_seconds=45)
    assert store.get(DSN, "k1") == {"rows": [{"name": "meta"}]}
    assert pg.committed == 1


def test_sqlite_warehouses_are_left_alone(monkeypatch):
    """A single long-lived process already hits its in-memory cache; writing
    here would only contend for the file lock the report reads through."""
    monkeypatch.setattr(store, "is_postgres_dsn", lambda db: False)
    monkeypatch.setattr(store, "using_postgres", lambda: False)
    monkeypatch.setattr(store, "connect", lambda db: pytest.fail("must not touch the DB"))

    store.put("/tmp/warehouse.sqlite", "k", {"rows": []}, ttl_seconds=45)
    assert store.get("/tmp/warehouse.sqlite", "k") is None


def test_it_can_be_switched_off(pg, monkeypatch):
    monkeypatch.setenv("REPORT_CACHE_SHARED", "0")
    store.put(DSN, "k", {"rows": []}, ttl_seconds=45)
    assert store.get(DSN, "k") is None
    assert pg.executed == []


def test_an_oversized_report_is_not_persisted(pg, monkeypatch):
    monkeypatch.setattr(store, "_MAX_PAYLOAD_BYTES", 50)
    store.put(DSN, "k", {"rows": [{"name": "x" * 500}]}, ttl_seconds=45)
    assert store.get(DSN, "k") is None


def test_a_warehouse_predating_the_table_gets_it_created(monkeypatch):
    conn = _FakeConn({}, fail_first_write=True)
    monkeypatch.setattr(store, "connect", lambda db: conn)
    monkeypatch.setattr(store, "is_postgres_dsn", lambda db: True)

    store.put(DSN, "k", {"rows": []}, ttl_seconds=45)

    assert conn.rolled_back == 1
    assert any("CREATE TABLE IF NOT EXISTS public.report_cache" in sql for sql in conn.executed)
    assert store.get(DSN, "k") == {"rows": []}


def test_a_read_failure_never_fails_the_request(monkeypatch):
    def _boom(db):
        raise RuntimeError("warehouse unreachable")

    monkeypatch.setattr(store, "connect", _boom)
    monkeypatch.setattr(store, "is_postgres_dsn", lambda db: True)

    assert store.get(DSN, "k") is None
    store.put(DSN, "k", {"rows": []}, ttl_seconds=45)  # must not raise


def test_clear_drops_every_entry(pg):
    store.put(DSN, "k1", {"a": 1}, ttl_seconds=45)
    store.put(DSN, "k2", {"b": 2}, ttl_seconds=45)
    store.clear(DSN)
    assert store.get(DSN, "k1") is None
    assert store.get(DSN, "k2") is None
