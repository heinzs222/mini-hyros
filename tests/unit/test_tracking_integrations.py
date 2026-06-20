"""Unit tests for tracking.health_check and integrations.status."""

from __future__ import annotations

from attributionops.tools.integrations import integrations_status
from attributionops.tools.tracking import tracking_health_check
from tests.helpers import insert_rows, order, spend, touchpoint


# ── tracking.health_check ─────────────────────────────────────────────────────
def test_health_check_empty_db_warns(empty_db):
    health = tracking_health_check(empty_db)
    assert health["status"] == "warn"  # no orders / no sessions
    assert health["coverage"]["orders_total"] == 0
    assert health["coverage"]["sessions_total"] == 0


def test_health_check_coverage_counts(empty_db):
    insert_rows(
        empty_db,
        "sessions",
        [
            {"session_id": "s1", "ts": "2026-01-14T00:00:00Z", "gclid": "g1", "customer_key": "c1"},
            {"session_id": "s2", "ts": "2026-01-14T00:00:00Z", "customer_key": "c2"},  # no click id
        ],
    )
    insert_rows(empty_db, "touchpoints", [touchpoint("2026-01-14T09:00:00Z", "c1", ad_id="ad1")])
    insert_rows(
        empty_db,
        "orders",
        [
            order("o1", "2026-01-15T12:00:00Z", "c1", net=100),  # has touchpoint in window
            order("o2", "2026-01-15T12:00:00Z", "c2", net=80),   # no touchpoint -> no source
        ],
    )

    cov = tracking_health_check(empty_db)["coverage"]
    assert cov["sessions_total"] == 2
    assert cov["sessions_with_click_id"] == 1
    assert cov["orders_total"] == 2
    assert cov["orders_with_source"] == 1


def test_health_check_reports_gaps(empty_db):
    insert_rows(empty_db, "sessions", [{"session_id": "s1", "ts": "2026-01-14T00:00:00Z", "customer_key": "c1"}])
    insert_rows(empty_db, "orders", [order("o1", "2026-01-15T12:00:00Z", "c1", net=100)])
    health = tracking_health_check(empty_db)
    issues = {g["issue"] for g in health["top_tracking_gaps"]}
    assert "High session click-id loss" in issues
    assert "Orders missing attributable source" in issues


# ── integrations.status ───────────────────────────────────────────────────────
def test_integrations_status_lists_schema_tables(empty_db):
    status = integrations_status(empty_db)
    assert status["connected"] is True
    assert status["warehouse"]["type"] == "sqlite"
    tables = set(status["warehouse"]["tables"])
    assert {"spend", "sessions", "touchpoints", "orders", "conversions"} <= tables


def test_integrations_status_surfaces_last_timestamps(empty_db):
    insert_rows(empty_db, "spend", [spend(date="2026-01-10")])
    insert_rows(empty_db, "sessions", [{"session_id": "s1", "ts": "2026-01-12T00:00:00Z"}])
    status = integrations_status(empty_db)
    assert status["ads"]["spend_last_date"] == "2026-01-10"
    assert status["tracking"]["sessions_last_ts"] == "2026-01-12T00:00:00Z"
