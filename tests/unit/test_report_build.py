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
    assert s["all_orders_gross_revenue"] == 1000.0
    assert s["tracked_revenue"] == 800.0
    assert s["blended_roas"] == 4.0
    assert s["roas"] == 4.0


def test_report_buckets_orders_in_reporting_timezone(report_db, monkeypatch):
    monkeypatch.setenv("REPORT_TIMEZONE", "Etc/GMT+6")
    insert_rows(
        report_db,
        "orders",
        [
            order(
                "late-local-order",
                "2026-01-15T03:00:00Z",
                "late-customer",
                gross=125,
                net=125,
                sale_group_id="late-group",
            )
        ],
    )

    report = build_hyros_like_report(report_db, _inputs())
    series = report["charts"]["time_series"]
    jan14 = next(row for row in series if row["date"] == "2026-01-14")
    jan15 = next(row for row in series if row["date"] == "2026-01-15")

    # 03:00 UTC is 21:00 on Jan 14 in the account's UTC-06 reporting zone.
    assert jan14["tracked_orders"] == 1
    assert jan14["tracked_revenue"] == 125.0
    assert jan15["tracked_orders"] == 1
    assert jan15["tracked_revenue"] == 800.0


def test_report_separates_raw_sales_customer_sales_and_recurring_customers(empty_db, monkeypatch):
    monkeypatch.setenv("REPORT_TIMEZONE", "Etc/GMT+6")
    insert_rows(
        empty_db,
        "spend",
        [spend(date="2026-01-15", clicks=10, cost=100, impressions=500)],
    )
    insert_rows(
        empty_db,
        "touchpoints",
        [
            touchpoint("2026-01-15T12:00:00Z", "customer-1", session_id="s1"),
            touchpoint("2026-01-15T12:05:00Z", "customer-2", session_id="s2"),
        ],
    )
    insert_rows(
        empty_db,
        "orders",
        [
            order("o1", "2026-01-15T13:00:00Z", "customer-1", net=100, gross=100, sale_group_id="group-1"),
            order("o2", "2026-01-15T13:04:00Z", "customer-1", net=50, gross=50, sale_group_id="group-1"),
            order(
                "o3",
                "2026-01-16T13:00:00Z",
                "customer-1",
                net=20,
                gross=20,
                is_recurring=1,
                sale_group_id="group-2",
            ),
            order("o4", "2026-01-16T14:00:00Z", "customer-2", net=200, gross=200, sale_group_id="group-3"),
        ],
    )

    report = build_hyros_like_report(empty_db, _inputs())
    totals = report["summary_totals"]

    assert totals["all_orders_count"] == 4
    assert totals["total_customers"] == 4
    assert totals["new_customers"] == 3
    assert totals["recurring_customers"] == 1
    assert totals["unique_customers"] == 2
    assert totals["net_new_customers"] == 2
    assert totals["attributed_sale_groups"] == 3
    assert totals["attributed_aov"] == 123.33


def test_report_matches_hyros_customer_and_net_cac_denominators(empty_db):
    insert_rows(
        empty_db,
        "spend",
        [spend(date="2026-01-15", clicks=10, cost=100, impressions=500)],
    )
    insert_rows(
        empty_db,
        "orders",
        [
            order("o1", "2026-01-15T12:00:00Z", "email-1", net=100, gross=100,
                  processor="stripe", processor_customer_id="cus-1"),
            order("o2", "2026-01-15T13:00:00Z", "email-1", net=50, gross=50,
                  processor="stripe", processor_customer_id="cus-1"),
            order("o3", "2026-01-15T14:00:00Z", "email-2", net=75, gross=75,
                  processor="stripe", processor_customer_id="cus-2", is_recurring=1),
            order("o4", "2026-01-15T15:00:00Z", "email-3", net=0, gross=25, refunds=25,
                  processor="stripe", processor_customer_id="cus-3"),
            order("o5", "2026-01-15T16:00:00Z", "email-4", net=40, gross=40,
                  processor="stripe", processor_customer_id=""),
        ],
    )

    totals = build_hyros_like_report(empty_db, _inputs())["summary_totals"]

    assert totals["all_orders_count"] == 5
    assert totals["total_customers"] == 4  # refunded sale excluded
    assert totals["new_customers"] == 3
    assert totals["recurring_customers"] == 1
    assert totals["unique_customers"] == 2  # distinct non-refunded Stripe customer ids
    assert totals["cac"] == 50.0


def test_refunded_recurring_sale_stays_in_hyros_recurring_split(empty_db):
    insert_rows(
        empty_db,
        "orders",
        [
            order("o1", "2026-01-15T12:00:00Z", "email-1", net=100, gross=100),
            order("o2", "2026-01-15T13:00:00Z", "email-2", net=0, gross=50,
                  refunds=50, is_recurring=1),
        ],
    )

    totals = build_hyros_like_report(empty_db, _inputs())["summary_totals"]

    assert totals["total_customers"] == 1
    assert totals["recurring_customers"] == 1
    assert totals["new_customers"] == 0
