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
