"""Unit tests for the db access layer and the local file-writing / query tools."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from attributionops.db import query, query_iter, sql_rows
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
