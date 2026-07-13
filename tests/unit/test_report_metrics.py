"""Tests for the new-customer / lead accuracy metrics in build_hyros_like_report.

These assert that NET CAC (spend / net-new customers), Cost-per-Lead
(spend / leads) and the New-Customers count are computed distinctly from CPA
(spend / orders), and that the per-day chart series carries tracked totals so
the dashboard charts don't read flat when attribution is incomplete.
"""

from __future__ import annotations

import json

import pytest

from attributionops.report import ReportInputs, build_hyros_like_report
from tests.helpers import insert_rows, order, spend, touchpoint


def _lead(conversion_id, ts, customer_key, ctype="Lead"):
    return {
        "conversion_id": conversion_id,
        "ts": ts,
        "type": ctype,
        "value": "0",
        "order_id": "",
        "customer_key": customer_key,
    }


@pytest.fixture
def metrics_db(empty_db):
    # Spend inside the window.
    insert_rows(empty_db, "spend", [
        spend(date="2026-06-05", platform="meta", account_id="acc", campaign_id="cmp1",
              adset_id="set1", ad_id="ad1", clicks=100, cost=600, impressions=5000,
              metadata=json.dumps({"campaign_name": "Camp 1"})),
    ])
    insert_rows(empty_db, "touchpoints", [
        touchpoint("2026-06-09T09:00:00Z", "c1", platform="meta", campaign_id="cmp1"),
        touchpoint("2026-06-09T09:00:00Z", "c2", platform="meta", campaign_id="cmp1"),
        touchpoint("2026-06-19T09:00:00Z", "c3", platform="meta", campaign_id="cmp1"),
    ])
    # Orders: c1's first-ever order predates the window (so c1 is *returning*);
    # c2 and c3 first purchase inside the window (so they are *new*).
    insert_rows(empty_db, "orders", [
        order("o0", "2026-05-15T12:00:00Z", "c1", gross=100, net=100, cogs=10, fees=5),
        order("o1", "2026-06-10T12:00:00Z", "c1", gross=200, net=200, cogs=20, fees=8),
        order("o2", "2026-06-12T12:00:00Z", "c2", gross=300, net=300, cogs=30, fees=9),
        order("o3", "2026-06-20T12:00:00Z", "c3", gross=400, net=400, cogs=40, fees=12),
    ])
    # Leads: four distinct customers submit a lead in the window. Purchase
    # conversions for all three in-window orders make attributed_orders = 3,
    # so CPA (600/3=200), CAC (600/2=300) and CPL (600/4=150) are all distinct.
    insert_rows(empty_db, "conversions", [
        _lead("lc1", "2026-06-08T08:00:00Z", "c2"),
        _lead("lc2", "2026-06-11T08:00:00Z", "c4"),
        _lead("lc3", "2026-06-18T08:00:00Z", "c5"),
        _lead("lc4", "2026-06-18T09:00:00Z", "c6"),
        _lead("pc1", "2026-06-10T12:00:00Z", "c1", ctype="Purchase"),
        _lead("pc2", "2026-06-12T12:00:00Z", "c2", ctype="Purchase"),
        _lead("pc3", "2026-06-20T12:00:00Z", "c3", ctype="Purchase"),
    ])
    return empty_db


def _inputs():
    return ReportInputs(
        report_name="Metrics Report",
        start_date="2026-06-01",
        end_date="2026-06-30",
        preset=None,
        currency="USD",
        attribution_model="last_click",
        lookback_days=30,
        conversion_type="Purchase",
        use_date_of_click_attribution=False,
        active_tab="campaign",
    )


def test_new_returning_and_lead_counts(metrics_db):
    st = build_hyros_like_report(metrics_db, _inputs())["summary_totals"]
    # The Hyros New Customers widget counts non-refunded non-recurring sales;
    # first-ever identities remain available separately as net_new_customers.
    assert st["new_customers"] == 3
    assert st["net_new_customers"] == 2
    assert st["returning_customers"] == 1
    # c2, c4, c5, c6 each have a lead conversion in the window.
    assert st["leads"] == 4


def test_cac_cpl_cpa_are_distinct(metrics_db):
    st = build_hyros_like_report(metrics_db, _inputs())["summary_totals"]
    # Hyros NET CAC = spend / distinct non-refunded customers = 600 / 3.
    assert st["cac"] == pytest.approx(200.0)
    # Cost per Lead = spend / leads = 600 / 4 = 150.
    assert st["cpl"] == pytest.approx(150.0)
    # CPA = spend / attributed orders = 600 / 3 = 200 — a different denominator.
    assert st["cpa"] == pytest.approx(200.0)
    assert st["cac"] != st["cpl"]


def test_time_series_carries_tracked_totals(metrics_db):
    ts = build_hyros_like_report(metrics_db, _inputs())["charts"]["time_series"]
    # All 4 in-window orders are tracked across the per-day series.
    assert sum(r["tracked_orders"] for r in ts) == 3  # c1, c2, c3 orders inside window
    assert sum(r["new_customers"] for r in ts) == 3
    assert sum(r["tracked_revenue"] for r in ts) == pytest.approx(900.0)  # 200 + 300 + 400
