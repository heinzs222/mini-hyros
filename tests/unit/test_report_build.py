"""End-to-end smoke test for build_hyros_like_report over a seeded warehouse.

Asserts the report's overall shape and the headline computed numbers so that a
regression anywhere in the orchestration (spend + attribution + reported + chart
series + summary math) is caught.
"""

from __future__ import annotations

import json

import pytest

from attributionops.report import ReportInputs, build_hyros_like_report
from tests.helpers import insert_rows, order, spend, touchpoint


@pytest.fixture
def report_db(empty_db):
    insert_rows(
        empty_db,
        "spend",
        [
            spend(
                date="2026-01-14",
                platform="meta",
                account_id="acc_meta",
                campaign_id="cmp1",
                adset_id="set1",
                ad_id="ad1",
                clicks=100,
                cost=200,
                impressions=10_000,
                metadata=json.dumps(
                    {"campaign_name": "Camp 1", "adset_name": "Set 1", "ad_name": "Ad 1"}
                ),
            )
        ],
    )
    insert_rows(
        empty_db,
        "touchpoints",
        [touchpoint("2026-01-14T09:00:00Z", "c1", platform="meta", campaign_id="cmp1", adset_id="set1", ad_id="ad1")],
    )
    insert_rows(
        empty_db,
        "orders",
        [order("o1", "2026-01-15T12:00:00Z", "c1", gross=1000, net=800, cogs=100, fees=50)],
    )
    return empty_db


def _inputs(active_tab="campaign"):
    return ReportInputs(
        report_name="Test Report",
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


def test_report_top_level_structure(report_db):
    report = build_hyros_like_report(report_db, _inputs())
    for key in (
        "report_meta",
        "tracking",
        "summary_totals",
        "tabs",
        "table",
        "charts",
        "platform_comparison",
        "funnels",
        "diagnostics",
        "action_plan",
    ):
        assert key in report

    assert report["report_meta"]["attribution_model"] == "Last Click"
    assert [t["key"] for t in report["tabs"]] == [
        "traffic_source", "ad_account", "campaign", "ad_set", "ad",
    ]
    assert report["table"]["active_tab"] == "campaign"


def test_report_time_series_spans_every_day_in_range(report_db):
    report = build_hyros_like_report(report_db, _inputs())
    series = report["charts"]["time_series"]
    assert len(series) == 31  # Jan 1 .. Jan 31 inclusive
    assert series[0]["date"] == "2026-01-01"
    assert series[-1]["date"] == "2026-01-31"
    # Spend landed on Jan 14, revenue is credited on the Jan 15 conversion day.
    jan14 = next(r for r in series if r["date"] == "2026-01-14")
    jan15 = next(r for r in series if r["date"] == "2026-01-15")
    assert jan14["cost"] == 200.0
    assert jan15["revenue"] == 800.0


def test_report_campaign_row_metrics(report_db):
    report = build_hyros_like_report(report_db, _inputs("campaign"))
    rows = report["table"]["rows"]
    assert len(rows) == 1
    metrics = rows[0]["metrics"]
    assert metrics["cost"] == 200.0
    assert metrics["revenue"] == 800.0
    assert metrics["roas"] == 4.0
    assert metrics["orders"] == 1.0
    assert metrics["profit"] == 450.0  # 800 - 200 - 100 - 50


def test_report_summary_totals(report_db):
    report = build_hyros_like_report(report_db, _inputs())
    s = report["summary_totals"]
    assert s["all_orders_count"] == 1
    assert s["all_orders_revenue"] == 800.0
    assert s["attributed_orders"] == 1.0
    assert s["attributed_revenue"] == 800.0
    assert s["attribution_rate"] == 100.0
    assert s["blended_roas"] == 4.0
    assert s["roas"] == 4.0
