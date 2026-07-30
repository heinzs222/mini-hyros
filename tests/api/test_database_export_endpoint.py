"""Tests for the /api/admin/database-export endpoint and its snapshot helper.

The endpoint hands out a full copy of the warehouse, so its auth gate is the
security-critical part: it must stay closed unless DATABASE_EXPORT_TOKEN is set
AND the caller presents exactly that token.
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import closing

import pytest

from backend.database_export import create_sqlite_snapshot, remove_file


# ── endpoint auth gate ───────────────────────────────────────────────────────
def test_export_disabled_when_token_unset(client, monkeypatch):
    monkeypatch.delenv("DATABASE_EXPORT_TOKEN", raising=False)
    resp = client.get("/api/admin/database-export")
    assert resp.status_code == 503


def test_export_requires_token_header(client, monkeypatch):
    monkeypatch.setenv("DATABASE_EXPORT_TOKEN", "s3cret")
    assert client.get("/api/admin/database-export").status_code == 401


def test_export_rejects_wrong_token(client, monkeypatch):
    monkeypatch.setenv("DATABASE_EXPORT_TOKEN", "s3cret")
    resp = client.get(
        "/api/admin/database-export", headers={"X-Mini-Hyros-Export-Token": "wrong"}
    )
    assert resp.status_code == 401


def test_export_succeeds_with_correct_token(client, monkeypatch):
    monkeypatch.setenv("DATABASE_EXPORT_TOKEN", "s3cret")
    resp = client.get(
        "/api/admin/database-export", headers={"X-Mini-Hyros-Export-Token": "s3cret"}
    )
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/vnd.sqlite3"
    assert "attachment" in resp.headers.get("content-disposition", "")
    # A real SQLite file starts with this magic header.
    assert resp.content[:15] == b"SQLite format 3"


def test_export_404_when_database_file_missing(client, monkeypatch, tmp_path):
    monkeypatch.setenv("DATABASE_EXPORT_TOKEN", "s3cret")
    monkeypatch.setenv("ATTRIBUTIONOPS_DB_PATH", str(tmp_path / "nope.sqlite"))
    resp = client.get(
        "/api/admin/database-export", headers={"X-Mini-Hyros-Export-Token": "s3cret"}
    )
    assert resp.status_code == 404


# ── snapshot helper ──────────────────────────────────────────────────────────
def test_create_snapshot_copies_rows(tmp_path):
    src = tmp_path / "src.sqlite"
    con = sqlite3.connect(src)
    con.execute("CREATE TABLE t (a TEXT)")
    con.execute("INSERT INTO t VALUES ('x')")
    con.commit()
    con.close()

    snapshot = create_sqlite_snapshot(str(src))
    try:
        assert os.path.isfile(snapshot)
        with closing(sqlite3.connect(snapshot)) as snap:
            assert snap.execute("SELECT a FROM t").fetchall() == [("x",)]
    finally:
        remove_file(snapshot)
    assert not os.path.exists(snapshot)


def test_create_snapshot_missing_source_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        create_sqlite_snapshot(str(tmp_path / "absent.sqlite"))


def test_remove_file_is_idempotent(tmp_path):
    target = tmp_path / "gone.sqlite"
    remove_file(str(target))  # must not raise when already absent
    target.write_text("x")
    remove_file(str(target))
    assert not target.exists()
