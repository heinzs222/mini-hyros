"""Coverage for ensure_schema's migration-failure recording."""

from __future__ import annotations

import sqlite3

import attributionops.schema as schema


def test_migration_failure_is_recorded(tmp_path, monkeypatch):
    db = str(tmp_path / "broken.sqlite")
    sqlite3.connect(db).close()

    def boom(conn):
        raise sqlite3.OperationalError("simulated migration failure")

    monkeypatch.setattr(schema, "apply_migrations", boom)
    schema._ensured_paths.discard(db)
    schema._last_migration_error.pop(db, None)

    # Must not raise even though the migration failed.
    schema.ensure_schema(db)
    assert schema.last_migration_error(db) == "simulated migration failure"


def test_successful_migration_clears_error(tmp_path):
    db = str(tmp_path / "ok.sqlite")
    sqlite3.connect(db).close()
    schema._ensured_paths.discard(db)
    schema._last_migration_error[db] = "stale error"

    schema.ensure_schema(db)
    assert schema.last_migration_error(db) is None
