"""Resilience + new-signal coverage for build_hyros_like_report.

These exercise the graceful-degradation paths added so that a partial/broken
warehouse produces an explained, non-crashing report instead of a silently
all-zero one, plus the per-day leads series and the timezone helper.
"""

from __future__ import annotations

import sqlite3

import pytest

from attributionops.report import ReportInputs, build_hyros_like_report
from attributionops.util import report_timezone_name
from tests.helpers import insert_rows, order, spend, touchpoint


def _inputs(active_tab: str = "traffic_source", **overrides) -> ReportInputs:
    base = dict(
        report_name="Test",
        start_date="2026-01-01",
        end_date="2026-01-31",
        preset=None,
        currency="USD",
        attribution_model="last_click",
        lookback_days=30,
        conversion_type="Purchase",
        use_date_of_click_attribution=False,
        active_tab=active_tab,
    )
    base.update(overrides)
    return ReportInputs(**base)


def _seed_basic(db: str) -> None:
    insert_rows(db, "spend", [spend(date="2026-01-14", clicks=100, cost=200, impressions=10_000)])
    insert_rows(db, "touchpoints", [touchpoint("2026-01-14T09:00:00Z", "c1")])
    insert_rows(db, "orders", [order("o1", "2026-01-15T12:00:00Z", "c1", gross=1000, net=800, cogs=100, fees=50)])


def test_healthy_report_has_no_data_errors(empty_db):
    _seed_basic(empty_db)
    report = build_hyros_like_report(empty_db, _inputs())
    assert report["diagnostics"]["data_errors"] == []
    assert report["summary_totals"]["all_orders_count"] == 1


def test_order_query_failure_is_reported_not_silently_zeroed(empty_db):
    _seed_basic(empty_db)
    # Simulate a warehouse whose orders table is missing a column the metrics
    # query selects (the exact condition that used to silently zero the report).
    with sqlite3.connect(empty_db) as conn:
        conn.execute("ALTER TABLE orders DROP COLUMN processor")
        conn.commit()

    report = build_hyros_like_report(empty_db, _inputs())
    # It must NOT crash, and the failure must be surfaced (not hidden as "empty").
    errors = report["diagnostics"]["data_errors"]
    assert any(e["component"] == "orders" for e in errors)
    # Attribution-side metrics are computed independently and still populate.
    assert "summary_totals" in report


def test_leads_series_present_in_time_series(empty_db):
    _seed_basic(empty_db)
    insert_rows(
        empty_db,
        "conversions",
        [
            {
                "conversion_id": "lead1",
                "ts": "2026-01-10T08:00:00Z",
                "type": "lead",
                "value": "0",
                "order_id": "",
                "customer_key": "lead_cust_1",
                "session_id": "s_lead_1",
                "visitor_id": "",
            }
        ],
    )
    report = build_hyros_like_report(empty_db, _inputs())
    assert report["summary_totals"]["leads"] == 1
    # A first-ever opt-in inside the window counts as a net-new lead (stage 2).
    assert report["summary_totals"]["new_leads"] == 1
    # The per-day series must carry a `leads` field on every day.
    series = report["charts"]["time_series"]
    assert all("leads" in row for row in series)
    assert sum(int(row["leads"]) for row in series) == 1


def test_lead_net_new_scan_failure_is_reported(empty_db, monkeypatch):
    _seed_basic(empty_db)
    insert_rows(
        empty_db,
        "conversions",
        [
            {
                "conversion_id": "lead1",
                "ts": "2026-01-10T08:00:00Z",
                "type": "lead",
                "value": "0",
                "order_id": "",
                "customer_key": "lk1",
                "session_id": "s1",
                "visitor_id": "",
            }
        ],
    )
    # Force ONLY the net-new-lead scan (stage 2) to fail, and assert the stage-1
    # counts survive and the failure is surfaced rather than zeroing everything.
    import attributionops.report as report_mod

    real_query = report_mod.query

    def flaky_query(db_path, sql, params=None):
        if "MIN(ts) AS first_ts" in sql and "identity" in sql:
            raise sqlite3.OperationalError("simulated net-new scan failure")
        return real_query(db_path, sql, params)

    monkeypatch.setattr(report_mod, "query", flaky_query)
    report = build_hyros_like_report(empty_db, _inputs())
    errors = report["diagnostics"]["data_errors"]
    assert any(e["component"] == "leads_net_new" for e in errors)
    # Stage-1 lead count still computed despite the stage-2 failure.
    assert report["summary_totals"]["leads"] == 1
    assert report["summary_totals"]["new_leads"] == 0


def test_report_ad_account_tab_on_resilience_db(empty_db):
    _seed_basic(empty_db)
    report = build_hyros_like_report(empty_db, _inputs("ad_account"))
    assert report["table"]["active_tab"] == "ad_account"


def test_report_survives_sessions_without_visitor_id(empty_db):
    _seed_basic(empty_db)
    # A sessions table lacking visitor_id previously raised
    # "misuse of aggregate: MAX()" and crashed the entire build.
    with sqlite3.connect(empty_db) as conn:
        for table in ("sessions", "touchpoints"):
            cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
            if "visitor_id" not in cols:
                continue
            # Drop any index referencing visitor_id first (DROP COLUMN fails otherwise).
            for (idx_name,) in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=?",
                (table,),
            ).fetchall():
                info = conn.execute(f"PRAGMA index_info({idx_name})").fetchall()
                if any(str(row[2]) == "visitor_id" for row in info):
                    conn.execute(f"DROP INDEX IF EXISTS {idx_name}")
            conn.execute(f"ALTER TABLE {table} DROP COLUMN visitor_id")
        conn.commit()

    report = build_hyros_like_report(empty_db, _inputs())
    assert "tracking" in report
    assert isinstance(report["tracking"].get("tracking_percentage"), (int, float))


def test_report_timezone_name_reflects_env(monkeypatch):
    monkeypatch.setenv("REPORT_TIMEZONE", "America/Toronto")
    assert report_timezone_name() == "America/Toronto"
    monkeypatch.delenv("REPORT_TIMEZONE", raising=False)
    assert report_timezone_name() == "Etc/GMT+6"
