"""Smoke + light golden tests for the analytics GET endpoints over a seeded DB."""

from __future__ import annotations

import pytest

from tests.helpers import insert_rows, order, touchpoint


@pytest.fixture
def seeded(client, api_db):
    insert_rows(
        api_db,
        "touchpoints",
        [touchpoint("2026-01-14T09:00:00Z", "c1", platform="meta", campaign_id="cmp1", ad_id="ad1")],
    )
    insert_rows(api_db, "orders", [order("o1", "2026-01-15T12:00:00Z", "c1", gross=1000, net=800)])
    insert_rows(
        api_db,
        "sessions",
        [{"session_id": "s1", "ts": "2026-01-14T08:00:00Z", "customer_key": "c1", "landing_page": "/checkout"}],
    )
    insert_rows(
        api_db,
        "conversions",
        [{"conversion_id": "k1", "ts": "2026-01-15T12:00:00Z", "type": "purchase", "value": "800", "customer_key": "c1"}],
    )
    return client


def test_ltv_by_source(seeded):
    r = seeded.get("/api/ltv/by-source")
    assert r.status_code == 200
    body = r.json()
    assert "rows" in body and "windows" in body


def test_ltv_summary(seeded):
    assert seeded.get("/api/ltv/summary").status_code == 200


def test_funnel_report(seeded):
    r = seeded.get("/api/funnel/report")
    assert r.status_code == 200
    body = r.json()
    assert "stages" in body
    assert "overall_conversion_rate" in body


def test_cohort_analysis(seeded):
    r = seeded.get("/api/cohort/analysis")
    assert r.status_code == 200
    assert "cohorts" in r.json()


def test_journey_stats_counts_one_journey(seeded):
    r = seeded.get("/api/journey/stats")
    assert r.status_code == 200
    assert r.json()["total_journeys"] == 1


def test_refunds_summary_counts_orders(seeded):
    r = seeded.get("/api/refunds/summary")
    assert r.status_code == 200
    assert r.json()["total_orders"] == 1
