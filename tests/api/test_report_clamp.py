"""Coverage for the future-window clamp and health timezone exposure."""

from __future__ import annotations


def test_health_exposes_reporting_timezone(client):
    body = client.get("/api/health").json()
    assert isinstance(body.get("timezone"), str) and body["timezone"]


def test_report_clamps_future_window(client, api_db):
    r = client.get(
        "/api/report",
        params={
            "start_date": "2099-01-01",
            "end_date": "2099-01-31",
            "active_tab": "traffic_source",
        },
    )
    assert r.status_code == 200
    body = r.json()
    # A window entirely in the future is clamped to today and flagged so the UI
    # can explain the adjustment instead of silently showing an empty report.
    assert body["report_meta"].get("future_window_clamped") is True


def test_report_clamps_only_future_end(client, api_db):
    # A window that starts in the past but ends in the future is still clamped.
    r = client.get(
        "/api/report",
        params={"start_date": "2026-01-01", "end_date": "2099-12-31", "active_tab": "traffic_source"},
    )
    assert r.status_code == 200
    assert r.json()["report_meta"].get("future_window_clamped") is True


def test_valid_window_is_not_flagged(client, api_db):
    r = client.get(
        "/api/report",
        params={"start_date": "2026-01-01", "end_date": "2026-01-31", "active_tab": "traffic_source"},
    )
    assert r.status_code == 200
    assert "future_window_clamped" not in r.json()["report_meta"]


def test_report_rejects_inverted_range(client, api_db):
    r = client.get(
        "/api/report",
        params={"start_date": "2026-02-01", "end_date": "2026-01-01", "active_tab": "traffic_source"},
    )
    assert r.status_code == 400


def test_report_rejects_range_inverted_by_clamp(client, api_db):
    # start=2099 clamps to today, which lands after the past end date — that must
    # be a 400, not an empty report labeled as clamped.
    r = client.get(
        "/api/report",
        params={"start_date": "2099-01-01", "end_date": "2020-01-01", "active_tab": "traffic_source"},
    )
    assert r.status_code == 400


def test_clamp_flag_is_per_request_not_cached(client, api_db):
    from datetime import datetime

    from attributionops.util import report_timezone

    today = datetime.now(report_timezone()).date().isoformat()
    future = {"start_date": "2099-01-01", "end_date": "2099-12-31", "active_tab": "traffic_source"}
    genuine = {"start_date": today, "end_date": today, "active_tab": "traffic_source"}

    # Both requests resolve to the same (today, today) window and share a cache
    # entry, but the flag reflects each request's own dates, not whoever built
    # the cache entry first.
    r1 = client.get("/api/report", params=future)
    assert r1.json()["report_meta"].get("future_window_clamped") is True

    r2 = client.get("/api/report", params=genuine)
    assert "future_window_clamped" not in r2.json()["report_meta"]

    r3 = client.get("/api/report", params=future)
    assert r3.json()["report_meta"].get("future_window_clamped") is True


def test_ad_tab_report_injects_thumbnail_fields(client, api_db):
    from tests.helpers import insert_rows, order, spend, touchpoint

    insert_rows(api_db, "spend", [spend(date="2026-01-14", clicks=50, cost=120, impressions=5000)])
    insert_rows(api_db, "touchpoints", [touchpoint("2026-01-14T09:00:00Z", "c1")])
    insert_rows(api_db, "orders", [order("o1", "2026-01-15T12:00:00Z", "c1", gross=500, net=400)])

    r = client.get(
        "/api/report",
        params={"start_date": "2026-01-01", "end_date": "2026-01-31", "active_tab": "ad"},
    )
    assert r.status_code == 200
    rows = r.json()["table"]["rows"]
    # Every ad-level row carries the creative fields (empty when no creative synced).
    for row in rows:
        assert "thumbnail_url" in row
        assert "creative_type" in row
