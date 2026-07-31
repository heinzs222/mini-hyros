"""The Postgres schema replay is skipped once it is already applied.

Every statement in the schema file is IF NOT EXISTS, so replaying it was
harmless — but it is 40-odd round trips to the warehouse, and ``ensure_schema``
runs on every process start. On serverless that is every cold instance, delaying
the first request each one serves.
"""

from __future__ import annotations

import pytest

from attributionops import schema as schema_mod


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeConn:
    """Stands in for a Postgres connection, recording what it was asked to run."""

    def __init__(self, *, has_state_table: bool, applied: str = ""):
        self.has_state_table = has_state_table
        self.applied = applied
        self.executed: list[str] = []
        self.committed = 0

    def execute(self, sql, params=None):
        self.executed.append(" ".join(str(sql).split()))
        if "to_regclass" in sql:
            return _FakeCursor([{"present": self.has_state_table}])
        if "SELECT value FROM" in sql:
            return _FakeCursor([{"value": self.applied}] if self.applied else [])
        return _FakeCursor([])

    def commit(self):
        self.committed += 1

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None


@pytest.fixture
def fingerprint():
    return schema_mod._schema_fingerprint(
        schema_mod._POSTGRES_SCHEMA.read_text(encoding="utf-8")
    )


def test_current_schema_costs_two_queries_not_a_full_replay(monkeypatch, fingerprint):
    conn = _FakeConn(has_state_table=True, applied=fingerprint)
    monkeypatch.setattr(schema_mod, "connect", lambda url: conn)

    schema_mod._ensure_postgres_schema("postgresql://u:p@h/db")

    assert len(conn.executed) == 2
    assert not any("create table" in sql.lower() for sql in conn.executed)
    assert conn.committed == 0


def test_a_warehouse_without_the_marker_gets_the_full_schema(monkeypatch, fingerprint):
    conn = _FakeConn(has_state_table=False)
    monkeypatch.setattr(schema_mod, "connect", lambda url: conn)

    schema_mod._ensure_postgres_schema("postgresql://u:p@h/db")

    creates = [sql for sql in conn.executed if sql.lower().startswith("create table")]
    assert len(creates) > 5
    # The fingerprint is recorded so the next boot skips the replay.
    assert any("INSERT INTO public.schema_state" in sql for sql in conn.executed)
    assert conn.committed == 1


def test_a_stale_fingerprint_reapplies_the_schema(monkeypatch, fingerprint):
    conn = _FakeConn(has_state_table=True, applied="an-older-schema")
    monkeypatch.setattr(schema_mod, "connect", lambda url: conn)

    schema_mod._ensure_postgres_schema("postgresql://u:p@h/db")

    assert any(sql.lower().startswith("create table") for sql in conn.executed)
    assert conn.committed == 1


def test_editing_the_schema_changes_the_fingerprint():
    base = schema_mod._schema_fingerprint("create table a();")
    assert base != schema_mod._schema_fingerprint("create table a(); create table b();")
    assert base == schema_mod._schema_fingerprint("create table a();")
