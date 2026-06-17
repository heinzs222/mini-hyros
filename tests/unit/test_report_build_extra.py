"""Additional build_hyros_like_report scenarios: traffic-source proxy clicks,
reported-value merge, and the touchpoint-only fallback path.
"""

from __future__ import annotations

import pytest

from attributionops.report import ReportInputs, build_hyros_like_report
from tests.helpers import insert_rows, order, spend, touchpoint

JAN = "2026-01-01"
JAN_END = "2026-01-31"


def _inputs(active_tab):
    return ReportInputs(
        report_name="R",
        start_date=JAN,
        end_date=JAN_END,
        preset="Custom",
        currency="USD",
        attribution_model="last_click",
        lookback_days=30,
        conversion_type="Purchase",
        use_date_of_click_attribution=False,
        active_tab=active_tab,
    )


def test_traffic_source_tab_uses_session_proxy_and_reported(empty_db):
    insert_rows(
        empty_db,
        "spend",
        [
            spend(date="2026-01-10", platform="meta", clicks=100, cost=200),
            spend(date="2026-01-10", platform="google", account_id="acc_google",
                  campaign_id="cmp2", adset_id="set2", ad_id="ad2", clicks=50, cost=100),
        ],
    )
    insert_rows(
        empty_db,
        "touchpoints",
        [
            touchpoint("2026-01-12T00:00:00Z", "c1", platform="meta", ad_id="ad1"),
            touchpoint("2026-01-12T00:00:00Z", "c2", platform="google", campaign_id="cmp2", adset_id="set2", ad_id="ad2"),
        ],
    )
    insert_rows(
        empty_db,
        "orders",
        [
            order("o1", "2026-01-15T00:00:00Z", "c1", net=800),
            order("o2", "2026-01-15T00:00:00Z", "c2", net=300),
        ],
    )
    insert_rows(
        empty_db,
        "sessions",
        [
            {"session_id": "e1", "ts": "2026-01-13T00:00:00Z", "utm_medium": "email"},
            {"session_id": "g1", "ts": "2026-01-13T00:00:00Z", "utm_medium": "organic"},
            {"session_id": "r1", "ts": "2026-01-13T00:00:00Z", "utm_medium": "referral"},
            {"session_id": "d1", "ts": "2026-01-13T00:00:00Z", "utm_source": "direct"},
        ],
    )
    insert_rows(
        empty_db,
        "reported_value",
        [
            {"platform": "meta", "date": "2026-01-10", "account_id": "acc_meta",
             "campaign_id": "cmp1", "adset_id": "set1", "ad_id": "ad1",
             "conversion_type": "Purchase", "reported_value": "700"},
        ],
    )

    report = build_hyros_like_report(empty_db, _inputs("traffic_source"))
    rows = {r["name"]: r for r in report["table"]["rows"]}

    # Paid sources from spend/attribution.
    assert "facebook / paid_social" in rows
    assert "google / cpc" in rows
    # Session-derived (unpaid) sources surface via the session-count proxy.
    assert "newsletter / email" in rows
    assert "organic / organic" in rows
    assert "referral / referral" in rows
    assert "direct / (none)" in rows

    # Paid clicks are preserved (not overwritten by the session proxy).
    assert rows["facebook / paid_social"]["metrics"]["clicks"] == 100
    # The unpaid source gets its clicks from the session count proxy.
    assert rows["newsletter / email"]["metrics"]["clicks"] == 1
    # Reported value merged onto the matching paid source row.
    assert rows["facebook / paid_social"]["metrics"]["reported"] == 700.0

    # Platform comparison reflects both ad platforms.
    platforms = {r["platform"] for r in report["platform_comparison"]["rows"]}
    assert platforms == {"meta", "google"}


def test_campaign_tab_falls_back_to_touchpoints_without_spend(empty_db):
    # No spend and no orders -> the only signal is touchpoints; the report must
    # still surface the campaign entity so it is not invisible in the UI.
    insert_rows(
        empty_db,
        "touchpoints",
        [
            touchpoint("2026-01-12T00:00:00Z", "c1", platform="meta", campaign_id="cmp1", adset_id="set1", ad_id="ad1"),
            touchpoint("2026-01-13T00:00:00Z", "c1", platform="meta", campaign_id="cmp1", adset_id="set1", ad_id="ad1"),
        ],
    )
    report = build_hyros_like_report(empty_db, _inputs("campaign"))
    names = {r["name"] for r in report["table"]["rows"]}
    assert "cmp1" in names


def test_action_plan_flags_losers_winners_and_tracking(empty_db):
    # meta campaign: $200 spend, no attributed revenue -> losing.
    # google campaign: $100 spend, $800 attributed revenue -> winning.
    insert_rows(
        empty_db,
        "spend",
        [
            spend(date="2026-01-10", platform="meta", account_id="acc_meta",
                  campaign_id="cmp_m", adset_id="set_m", ad_id="ad_m", clicks=100, cost=200),
            spend(date="2026-01-10", platform="google", account_id="acc_g",
                  campaign_id="cmp_g", adset_id="set_g", ad_id="ad_g", clicks=50, cost=100),
        ],
    )
    insert_rows(
        empty_db,
        "touchpoints",
        [touchpoint("2026-01-12T00:00:00Z", "c1", platform="google",
                    campaign_id="cmp_g", adset_id="set_g", ad_id="ad_g")],
    )
    insert_rows(empty_db, "orders", [order("o1", "2026-01-15T00:00:00Z", "c1", net=800)])

    report = build_hyros_like_report(empty_db, _inputs("campaign"))
    titles = " ".join(a["title"] for a in report["action_plan"])
    assert "Cut/refresh losing" in titles   # the meta loser
    assert "Scale winner" in titles         # the google winner
    assert "Improve tracking coverage" in titles  # sessions absent -> low coverage
    # Low tracking percentage should also raise a diagnostics anomaly.
    assert report["diagnostics"]["anomalies"]


def test_click_date_attribution_adds_assumption_note(empty_db):
    insert_rows(empty_db, "touchpoints", [touchpoint("2026-01-12T00:00:00Z", "c1", ad_id="ad1")])
    insert_rows(empty_db, "orders", [order("o1", "2026-01-15T00:00:00Z", "c1", net=100)])
    inputs = ReportInputs(
        report_name="R", start_date=JAN, end_date=JAN_END, preset=None, currency="USD",
        attribution_model="last_click", lookback_days=30, conversion_type="Purchase",
        use_date_of_click_attribution=True, active_tab="ad",
    )
    report = build_hyros_like_report(empty_db, inputs)
    assert report["report_meta"]["use_date_of_click_attribution"] is True
    assert any("click-date attribution" in a for a in report["diagnostics"]["assumptions"])
