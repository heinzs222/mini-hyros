"""Tests for the serverless-oriented endpoints: /api/cron/sync and
/api/live/recent (the Vercel Cron sync trigger and the polling live feed that
replaced the /ws WebSocket)."""

from __future__ import annotations

import main
import pytest

from attributionops.db import connect


# ── /api/cron/sync ───────────────────────────────────────────────────────────
@pytest.fixture
def stub_sync(monkeypatch):
    calls = {"n": 0}

    async def _fake():
        calls["n"] += 1
        return {"spend": {"ok": True}}

    monkeypatch.setattr(main, "_run_scheduled_sync", _fake)
    return calls


def test_cron_sync_runs_without_secret(client, stub_sync, monkeypatch):
    monkeypatch.delenv("CRON_SECRET", raising=False)
    r = client.get("/api/cron/sync")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["result"] == {"spend": {"ok": True}}
    assert stub_sync["n"] == 1


def test_cron_sync_accepts_post(client, stub_sync, monkeypatch):
    monkeypatch.delenv("CRON_SECRET", raising=False)
    assert client.post("/api/cron/sync").status_code == 200


def test_cron_sync_secret_enforced(client, stub_sync, monkeypatch):
    monkeypatch.setenv("CRON_SECRET", "topsecret")
    assert client.get("/api/cron/sync").status_code == 401  # no header
    assert (
        client.get("/api/cron/sync", headers={"Authorization": "Bearer topsecret"}).status_code
        == 200
    )
    assert (
        client.get("/api/cron/sync", headers={"X-Cron-Secret": "topsecret"}).status_code == 200
    )
    assert (
        client.get("/api/cron/sync", headers={"Authorization": "Bearer wrong"}).status_code
        == 401
    )


def test_cron_sync_reports_error(client, monkeypatch):
    monkeypatch.delenv("CRON_SECRET", raising=False)

    async def _boom():
        raise RuntimeError("sync exploded")

    monkeypatch.setattr(main, "_run_scheduled_sync", _boom)
    r = client.get("/api/cron/sync")
    assert r.status_code == 500
    assert r.json()["status"] == "error"


# ── /api/live/recent ─────────────────────────────────────────────────────────
def _seed_live_rows(db_path: str) -> None:
    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO orders (order_id, ts, gross, customer_key) VALUES (?, ?, ?, ?)",
            ["o1", "2026-07-01T10:00:00Z", "42.50", "cust-1"],
        )
        conn.execute(
            "INSERT INTO conversions (conversion_id, ts, type, customer_key) VALUES (?, ?, ?, ?)",
            ["c1", "2026-07-01T10:01:00Z", "lead", "cust-2"],
        )
        conn.execute(
            "INSERT INTO conversions (conversion_id, ts, type, customer_key) VALUES (?, ?, ?, ?)",
            ["c2", "2026-07-01T10:02:00Z", "appointmentbooked", "cust-3"],
        )
        conn.execute(
            "INSERT INTO sessions (session_id, ts, utm_source, landing_page) VALUES (?, ?, ?, ?)",
            ["s1", "2026-07-01T10:03:00Z", "google", "/home"],
        )
        # A purchase-type conversion must NOT appear (orders already cover sales).
        conn.execute(
            "INSERT INTO conversions (conversion_id, ts, type, customer_key) VALUES (?, ?, ?, ?)",
            ["c3", "2026-07-01T10:04:00Z", "purchase", "cust-4"],
        )
        conn.commit()


def test_live_recent_empty_on_fresh_db(client):
    r = client.get("/api/live/recent?limit=10")
    assert r.status_code == 200
    body = r.json()
    assert body["events"] == []
    assert "cursor" in body


def test_live_recent_returns_typed_events(client, api_db):
    _seed_live_rows(api_db)
    body = client.get("/api/live/recent?limit=50").json()
    events = body["events"]
    by_type = {e["type"]: e for e in events}
    # order, lead, booking, session present; purchase-conversion excluded.
    assert by_type["new_order"]["gross"] == 42.5
    assert by_type["new_order"]["order_id"] == "o1"
    assert by_type["new_lead"]["conversion_type"] == "lead"
    assert by_type["new_booking"]["conversion_type"] == "appointmentbooked"
    assert by_type["session"]["utm_source"] == "google"
    assert by_type["session"]["landing_page"] == "/home"
    assert "new_order" in by_type and all(e["type"] != "purchase" for e in events)
    # oldest-first ordering (append semantics) → cursor is the newest ts.
    assert body["cursor"] == events[-1]["ts"]
    ts_list = [e["ts"] for e in events]
    assert ts_list == sorted(ts_list)


def test_live_recent_cursor_is_incremental(client, api_db):
    _seed_live_rows(api_db)
    first = client.get("/api/live/recent?limit=50").json()
    cursor = first["cursor"]
    # Polling again with the cursor returns nothing new.
    second = client.get(f"/api/live/recent?since={cursor}&limit=50").json()
    assert second["events"] == []
    assert second["cursor"] == cursor
