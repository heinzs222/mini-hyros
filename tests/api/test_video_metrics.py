"""Tests for the video metrics API (prefix /api/video).

  GET /api/video/metrics  — per-breakdown video metrics with derived VTR/completion
  GET /api/video/summary  — platform-level rollups

IMPORTANT SCHEMA NOTE
---------------------
The shared ``api_db`` fixture initialises the warehouse from
``scripts/init_empty_db.py``, whose ``video_metrics`` table uses a *different*
column set (e.g. ``views_25``/``ctr`` and no ``account_id``/``impressions``)
than the one ``backend/api/video_metrics.py`` queries (``views_25pct``,
``account_id``, ``impressions``, ...). Because ``_ensure_video_table`` only
creates the table ``IF NOT EXISTS``, the module never gets its own schema and
its queries fail with ``no such column`` against the stock fixture DB. See
``test_metrics_500_against_stock_fixture_schema`` which pins that behaviour.

To exercise the happy path we drop the stock table and let the endpoint's
``_ensure_video_table`` recreate it with the module's expected schema, then seed
via ``insert_rows``. This only touches the per-test temp DB (no source/conftest
changes). The module makes no outbound HTTP, so respx is not required.
"""

from __future__ import annotations

import sqlite3

import api.video_metrics as video_metrics
from tests.helpers import insert_rows

START = "2026-02-01"
END = "2026-02-28"


def _reset_to_module_schema(api_db: str) -> None:
    """Drop the stock fixture table; the module recreates its own on first call."""
    with sqlite3.connect(api_db) as conn:
        conn.execute("DROP TABLE IF EXISTS video_metrics")
        conn.commit()
    # Touching the table creation directly mirrors what the endpoint does.
    video_metrics._ensure_video_table(api_db)


def _vrow(**overrides):
    """A full video_metrics row (module schema) with sensible defaults."""
    row = {
        "platform": "meta",
        "date": "2026-02-10",
        "account_id": "acc1",
        "campaign_id": "cmp1",
        "adset_id": "set1",
        "ad_id": "ad1",
        "video_id": "vid1",
        "video_name": "Hook Test A",
        "views": 0,
        "views_3s": 0,
        "views_25pct": 0,
        "views_50pct": 0,
        "views_75pct": 0,
        "views_100pct": 0,
        "avg_watch_time_sec": 0.0,
        "video_length_sec": 0.0,
        "thumb_stop_rate": 0.0,
        "hook_rate": 0.0,
        "hold_rate": 0.0,
        "click_through_rate": 0.0,
        "cost_per_view": 0.0,
        "impressions": 0,
        "clicks": 0,
        "cost": 0.0,
    }
    row.update(overrides)
    return row


# ── Stock-fixture schema mismatch (documents a real bug) ────────────────────────


def test_metrics_500_against_stock_fixture_schema(api_db, monkeypatch):
    """The stock fixture's video_metrics schema is incompatible with the module's
    queries, so /metrics raises and FastAPI returns 500."""
    from fastapi.testclient import TestClient
    import main

    client = TestClient(main.app, raise_server_exceptions=False)
    r = client.get("/api/video/metrics", params={"start_date": START, "end_date": END})
    assert r.status_code == 500


def test_summary_500_against_stock_fixture_schema(api_db):
    from fastapi.testclient import TestClient
    import main

    client = TestClient(main.app, raise_server_exceptions=False)
    r = client.get("/api/video/summary", params={"start_date": START, "end_date": END})
    assert r.status_code == 500


# ── /metrics happy path (module schema) ─────────────────────────────────────────


def test_metrics_empty_returns_empty_rows(client, api_db):
    _reset_to_module_schema(api_db)
    r = client.get("/api/video/metrics", params={"start_date": START, "end_date": END})
    assert r.status_code == 200
    body = r.json()
    assert body["start_date"] == START
    assert body["end_date"] == END
    assert body["platform"] == "all"
    assert body["breakdown"] == "ad"
    assert body["rows"] == []


def test_metrics_ad_breakdown_aggregates_and_derives(client, api_db):
    _reset_to_module_schema(api_db)
    # Two rows for the same ad/video — they aggregate. SUM views/impressions,
    # so vtr = views/impressions and completion_rate = views_100pct/views.
    insert_rows(
        api_db,
        "video_metrics",
        [
            _vrow(date="2026-02-05", views=600, views_100pct=150, impressions=2000, clicks=40, cost=30.0),
            _vrow(date="2026-02-06", views=400, views_100pct=100, impressions=2000, clicks=10, cost=10.0),
        ],
    )
    r = client.get("/api/video/metrics", params={"start_date": START, "end_date": END, "breakdown": "ad"})
    assert r.status_code == 200
    rows = r.json()["rows"]
    assert len(rows) == 1
    row = rows[0]
    assert row["platform"] == "meta"
    assert row["video_name"] == "Hook Test A"
    assert row["views"] == 1000
    assert row["views_100pct"] == 250
    assert row["impressions"] == 4000
    assert row["clicks"] == 50
    assert row["cost"] == 40.0
    # cost_per_view = SUM(cost)/SUM(views) = 40/1000
    assert row["cost_per_view"] == 0.04
    # Derived in Python: vtr = 1000/4000, completion = 250/1000
    assert row["vtr"] == 0.25
    assert row["completion_rate"] == 0.25


def test_metrics_zero_views_yields_zero_rates(client, api_db):
    _reset_to_module_schema(api_db)
    insert_rows(
        api_db,
        "video_metrics",
        [_vrow(date="2026-02-07", views=0, views_100pct=0, impressions=0, cost=5.0)],
    )
    rows = client.get(
        "/api/video/metrics", params={"start_date": START, "end_date": END}
    ).json()["rows"]
    assert len(rows) == 1
    assert rows[0]["cost_per_view"] == 0
    assert rows[0]["vtr"] == 0
    assert rows[0]["completion_rate"] == 0


def test_metrics_platform_breakdown(client, api_db):
    _reset_to_module_schema(api_db)
    insert_rows(
        api_db,
        "video_metrics",
        [
            _vrow(platform="meta", date="2026-02-08", ad_id="ad1", views=300, impressions=1000),
            _vrow(platform="meta", date="2026-02-09", ad_id="ad2", views=200, impressions=1000),
            _vrow(platform="tiktok", date="2026-02-09", ad_id="ad3", views=500, impressions=1000),
        ],
    )
    rows = client.get(
        "/api/video/metrics",
        params={"start_date": START, "end_date": END, "breakdown": "platform"},
    ).json()["rows"]
    by_platform = {r["platform"]: r for r in rows}
    assert set(by_platform) == {"meta", "tiktok"}
    # meta rolls both ads up: 300 + 200.
    assert by_platform["meta"]["views"] == 500
    assert by_platform["tiktok"]["views"] == 500
    # Ordered by SUM(views) DESC — ties allowed, but both present.
    assert "campaign_id" not in by_platform["meta"]  # platform breakdown only groups by platform


def test_metrics_campaign_breakdown(client, api_db):
    _reset_to_module_schema(api_db)
    insert_rows(
        api_db,
        "video_metrics",
        [
            _vrow(campaign_id="cmpA", ad_id="ad1", views=100),
            _vrow(campaign_id="cmpA", ad_id="ad2", views=50),
            _vrow(campaign_id="cmpB", ad_id="ad3", views=70),
        ],
    )
    rows = client.get(
        "/api/video/metrics",
        params={"start_date": START, "end_date": END, "breakdown": "campaign"},
    ).json()["rows"]
    by_cmp = {r["campaign_id"]: r for r in rows}
    assert set(by_cmp) == {"cmpA", "cmpB"}
    assert by_cmp["cmpA"]["views"] == 150
    assert by_cmp["cmpB"]["views"] == 70
    # ORDER BY views DESC.
    assert rows[0]["campaign_id"] == "cmpA"


def test_metrics_platform_filter(client, api_db):
    _reset_to_module_schema(api_db)
    insert_rows(
        api_db,
        "video_metrics",
        [
            _vrow(platform="meta", ad_id="ad1", views=100),
            _vrow(platform="tiktok", ad_id="ad2", views=200),
        ],
    )
    rows = client.get(
        "/api/video/metrics",
        params={"start_date": START, "end_date": END, "platform": "tiktok"},
    ).json()["rows"]
    assert len(rows) == 1
    assert rows[0]["platform"] == "tiktok"
    assert rows[0]["views"] == 200


def test_metrics_date_range_excludes_out_of_window(client, api_db):
    _reset_to_module_schema(api_db)
    insert_rows(
        api_db,
        "video_metrics",
        [
            _vrow(date="2026-02-10", ad_id="in", views=100),
            _vrow(date="2026-01-01", ad_id="before", views=999),
            _vrow(date="2026-03-15", ad_id="after", views=999),
        ],
    )
    rows = client.get(
        "/api/video/metrics", params={"start_date": START, "end_date": END}
    ).json()["rows"]
    assert len(rows) == 1
    assert rows[0]["ad_id"] == "in"
    assert rows[0]["views"] == 100


def test_metrics_unknown_breakdown_falls_back_to_ad(client, api_db):
    _reset_to_module_schema(api_db)
    insert_rows(api_db, "video_metrics", [_vrow(views=42)])
    body = client.get(
        "/api/video/metrics",
        params={"start_date": START, "end_date": END, "breakdown": "bogus"},
    ).json()
    assert body["breakdown"] == "bogus"
    rows = body["rows"]
    # Fallback group is the same as the ad breakdown (per-video columns present).
    assert len(rows) == 1
    assert rows[0]["video_id"] == "vid1"
    assert rows[0]["views"] == 42


def test_metrics_default_dates_when_omitted(client, api_db):
    _reset_to_module_schema(api_db)
    body = client.get("/api/video/metrics").json()
    # Defaults are filled in (today and today-30d); just assert they are present.
    assert body["start_date"] and body["end_date"]
    assert body["start_date"] <= body["end_date"]


# ── /summary happy path (module schema) ─────────────────────────────────────────


def test_summary_empty(client, api_db):
    _reset_to_module_schema(api_db)
    r = client.get("/api/video/summary", params={"start_date": START, "end_date": END})
    assert r.status_code == 200
    body = r.json()
    assert body["start_date"] == START
    assert body["end_date"] == END
    assert body["platforms"] == []


def test_summary_rolls_up_per_platform(client, api_db):
    _reset_to_module_schema(api_db)
    insert_rows(
        api_db,
        "video_metrics",
        [
            _vrow(platform="meta", date="2026-02-05", ad_id="ad1", views=600, views_3s=500,
                  views_100pct=150, cost=30.0, clicks=20, impressions=2000),
            _vrow(platform="meta", date="2026-02-06", ad_id="ad2", views=400, views_3s=300,
                  views_100pct=100, cost=10.0, clicks=5, impressions=1000),
            _vrow(platform="tiktok", date="2026-02-07", ad_id="ad3", views=200, views_3s=150,
                  views_100pct=50, cost=4.0, clicks=2, impressions=800),
        ],
    )
    r = client.get("/api/video/summary", params={"start_date": START, "end_date": END})
    assert r.status_code == 200
    platforms = {p["platform"]: p for p in r.json()["platforms"]}
    assert set(platforms) == {"meta", "tiktok"}

    meta = platforms["meta"]
    assert meta["total_views"] == 1000
    assert meta["total_3s_views"] == 800
    assert meta["total_completions"] == 250
    assert meta["total_cost"] == 40.0
    assert meta["total_clicks"] == 25
    assert meta["total_impressions"] == 3000
    # cpv = SUM(cost)/SUM(views) = 40/1000
    assert meta["cpv"] == 0.04

    tiktok = platforms["tiktok"]
    assert tiktok["total_views"] == 200
    assert tiktok["cpv"] == 0.02

    # Ordered by SUM(views) DESC: meta (1000) before tiktok (200).
    ordered = [p["platform"] for p in r.json()["platforms"]]
    assert ordered == ["meta", "tiktok"]


def test_summary_default_dates_when_omitted(client, api_db):
    _reset_to_module_schema(api_db)
    body = client.get("/api/video/summary").json()
    # Defaults are filled in (today and today-30d).
    assert body["start_date"] and body["end_date"]
    assert body["start_date"] <= body["end_date"]


def test_summary_zero_views_cpv_is_zero(client, api_db):
    _reset_to_module_schema(api_db)
    insert_rows(api_db, "video_metrics", [_vrow(views=0, cost=9.0)])
    platforms = client.get(
        "/api/video/summary", params={"start_date": START, "end_date": END}
    ).json()["platforms"]
    assert len(platforms) == 1
    assert platforms[0]["cpv"] == 0
