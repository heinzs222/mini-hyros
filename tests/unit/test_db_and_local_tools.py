"""Unit tests for the db access layer and the local file-writing / query tools."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

import attributionops.db as db_module
from attributionops.db import (
    PostgresCompatRow,
    _translate_postgres_sql,
    query,
    query_iter,
    sql_rows,
)
from attributionops.schema import DEFAULT_CAMPAIGN_SETTINGS, ensure_campaign_settings
from attributionops.tools.audiences import audiences_sync
from attributionops.tools.conversions import conversions_push
from attributionops.tools.logs import logs_search
from attributionops.tools.warehouse import warehouse_query
from tests.helpers import insert_rows, spend


# ── db layer ──────────────────────────────────────────────────────────────────
def test_query_missing_db_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        query(str(tmp_path / "does_not_exist.sqlite"), "SELECT 1;")


def test_query_returns_rows_columns_and_metadata(empty_db):
    insert_rows(empty_db, "spend", [spend(date="2026-01-10", clicks=3)])
    result = query(empty_db, "SELECT date, clicks FROM spend;")
    assert result.rows == [{"date": "2026-01-10", "clicks": "3"}]
    assert result.columns == ["date", "clicks"]
    assert result.row_count == 1
    assert result.db_path == empty_db


def test_sql_rows_accepts_list_params(empty_db):
    assert sql_rows(empty_db, "SELECT ? AS x", [7]) == [{"x": 7}]


def test_query_iter_yields_each_row(empty_db):
    insert_rows(empty_db, "spend", [spend(date="2026-01-10"), spend(date="2026-01-11")])
    dates = sorted(r["date"] for r in query_iter(empty_db, "SELECT date FROM spend;"))
    assert dates == ["2026-01-10", "2026-01-11"]


def test_sql_rows_missing_db_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        sql_rows(str(tmp_path / "nope.sqlite"), "SELECT 1;")


def test_postgres_translation_handles_sqlite_placeholders_and_idempotent_insert():
    sql, params = _translate_postgres_sql(
        "INSERT OR IGNORE INTO orders (order_id, gross) VALUES (?, ?)",
        ["o1", "10"],
    )
    assert sql == "INSERT INTO orders (order_id, gross) VALUES (%s, %s) ON CONFLICT DO NOTHING"
    assert params == ["o1", "10"]


def test_postgres_translation_handles_replace_upsert_and_sqlite_functions():
    sql, _params = _translate_postgres_sql(
        """
        INSERT OR REPLACE INTO platform_tokens
            (platform, access_token, updated_at)
        VALUES ('tiktok', ?, ?)
        """
    )
    assert "ON CONFLICT (platform) DO UPDATE SET" in sql
    assert "access_token = EXCLUDED.access_token" in sql

    sql, _params = _translate_postgres_sql(
        """
        SELECT t.rowid AS marker,
               strftime('%Y-%m-%dT%H:%M:%SZ', o.ts, :lookback) AS win_start,
               CAST(CAST(o.net AS REAL) - ? AS TEXT) AS adjusted
        FROM orders o JOIN touchpoints t ON t.customer_key = o.customer_key
        WHERE date(substr(o.ts,1,10)) >= date(:start_date)
        """
    )
    assert "t.ctid::text AS marker" in sql
    assert "to_char((o.ts::timestamptz + %(lookback)s::interval)" in sql
    assert "CAST(COALESCE(NULLIF((o.net)::text, ''), '0') AS DOUBLE PRECISION)" in sql
    assert "substr(o.ts, 1, 10) >= %(start_date)s" in sql


def test_postgres_compat_row_supports_named_and_numeric_access():
    row = PostgresCompatRow({"cid": 0, "name": "spend", "type": "text"})

    assert row["name"] == "spend"
    assert row[1] == "spend"
    assert row.get("type") == "text"


def test_postgres_queries_reuse_one_autocommit_connection(monkeypatch):
    class FakeCursor:
        description = [("value",)]

        def fetchall(self):
            return [(1,)]

        def close(self):
            return None

    class FakeRaw:
        closed = False

    class FakeConnection:
        def __init__(self, url, *, autocommit=False):
            self.url = url
            self.autocommit = autocommit
            self.raw = FakeRaw()
            self.execute_count = 0

        def execute(self, _sql, _params=None):
            self.execute_count += 1
            return FakeCursor()

        def commit(self):
            raise AssertionError("autocommit query connections must not commit")

        def rollback(self):
            raise AssertionError("successful query connections must not roll back")

        def close(self):
            self.raw.closed = True

    created = []

    def create_connection(url, *, autocommit=False):
        connection = FakeConnection(url, autocommit=autocommit)
        created.append(connection)
        return connection

    monkeypatch.setattr(db_module, "PostgresConnection", create_connection)
    monkeypatch.setattr(db_module._query_connection_state, "postgres", None, raising=False)
    dsn = "postgresql://user:password@example.test/postgres"

    assert query(dsn, "SELECT 1 AS value").rows == [{"value": 1}]
    assert query(dsn, "SELECT 1 AS value").rows == [{"value": 1}]

    assert len(created) == 1
    assert created[0].autocommit is True
    assert created[0].execute_count == 2
    monkeypatch.setattr(db_module._query_connection_state, "postgres", None, raising=False)


# ── warehouse.query tool ──────────────────────────────────────────────────────
def test_default_campaign_settings_seed_without_overwriting_manual_override(empty_db):
    with sqlite3.connect(empty_db) as conn:
        seeded = conn.execute(
            "SELECT platform, campaign_id, tracked FROM campaign_settings"
        ).fetchall()
        assert set(seeded) >= {
            (platform, campaign_id, tracked)
            for platform, campaign_id, tracked, _note in DEFAULT_CAMPAIGN_SETTINGS
        }

        platform, campaign_id, _tracked, _note = DEFAULT_CAMPAIGN_SETTINGS[0]
        conn.execute(
            """
            UPDATE campaign_settings
            SET tracked = 1, note = 'Manual override'
            WHERE platform = ? AND campaign_id = ?
            """,
            (platform, campaign_id),
        )
        ensure_campaign_settings(conn)
        override = conn.execute(
            """
            SELECT tracked, note
            FROM campaign_settings
            WHERE platform = ? AND campaign_id = ?
            """,
            (platform, campaign_id),
        ).fetchone()

    assert override == (1, "Manual override")


def test_warehouse_query_wraps_result(empty_db):
    insert_rows(empty_db, "spend", [spend(date="2026-01-10"), spend(date="2026-01-11")])
    out = warehouse_query(empty_db, "SELECT COUNT(*) AS n FROM spend;")
    assert out["rows"] == [{"n": 2}]
    assert out["metadata"]["row_count"] == 1
    assert out["metadata"]["columns"] == ["n"]


# ── conversions.push / audiences.sync (write JSON to outbox) ───────────────────
def test_conversions_push_writes_payload(tmp_path):
    events = [{"event": "Purchase", "value": 10}]
    out = conversions_push(platform="meta", events=events, out_dir=str(tmp_path))
    assert out["ok"] is True
    assert out["event_count"] == 1
    written = Path(out["written_to"])
    assert written.exists()
    payload = json.loads(written.read_text())
    assert payload == {"platform": "meta", "events": events}


def test_audiences_sync_writes_definition(tmp_path):
    segment = {"name": "vip", "rule": "ltv>1000"}
    out = audiences_sync(platform="google", segment_definition=segment, out_dir=str(tmp_path))
    assert out["ok"] is True
    payload = json.loads(Path(out["written_to"]).read_text())
    assert payload == {"platform": "google", "segment_definition": segment}


# ── logs.search ───────────────────────────────────────────────────────────────
def test_logs_search_missing_file_returns_empty(tmp_path):
    out = logs_search(query="x", start_date="2026-01-01", end_date="2026-01-31",
                      log_path=str(tmp_path / "absent.jsonl"))
    assert out["rows"] == []


def test_logs_search_filters_by_substring(tmp_path):
    log_file = tmp_path / "logs.jsonl"
    log_file.write_text(
        '{"level":"error","msg":"boom"}\n'
        '{"level":"info","msg":"ok"}\n'
        "\n"  # blank line ignored
        "not-json\n"  # undecodable line ignored
    )
    out = logs_search(query="error", start_date="2026-01-01", end_date="2026-01-31", log_path=str(log_file))
    assert out["row_count"] == 1
    assert out["rows"][0]["msg"] == "boom"
