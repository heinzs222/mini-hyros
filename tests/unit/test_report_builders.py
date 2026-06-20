"""Unit tests for the DB-backed builder helpers in attributionops.report.

These cover the platform-comparison, funnel-snapshot, funnel-name map and
child-count logic with precise golden assertions.
"""

from __future__ import annotations

from attributionops.report import (
    _build_funnel_snapshot,
    _build_platform_comparison,
    _child_count_map,
    _funnel_name_map,
)
from tests.helpers import ad_name, insert_rows, spend

JAN = "2026-01-01"
JAN_END = "2026-01-31"


# ── _build_platform_comparison ────────────────────────────────────────────────
def test_platform_comparison_merges_spend_and_attribution(empty_db):
    insert_rows(
        empty_db,
        "spend",
        [
            spend(date="2026-01-10", platform="meta", clicks=100, cost=200),
            spend(date="2026-01-10", platform="google", clicks=50, cost=100),
        ],
    )
    attrib_rows = [
        {"platform": "meta", "orders": 2, "revenue": 800, "total_revenue": 1000, "cogs": 100, "fees": 50},
        {"platform": "google", "orders": 1, "revenue": 300, "total_revenue": 400, "cogs": 50, "fees": 20},
    ]
    rows = _build_platform_comparison(empty_db, start_date=JAN, end_date=JAN_END, attrib_rows=attrib_rows)

    # Sorted by revenue desc -> meta first.
    assert [r["platform"] for r in rows] == ["meta", "google"]
    meta = rows[0]
    assert meta["label"] == "Facebook Ads"
    assert meta["clicks"] == 100
    assert meta["cost"] == 200.0
    assert meta["revenue"] == 800.0
    assert meta["profit"] == 450.0  # 800 - 200 - 100 - 50
    assert meta["roas"] == 4.0
    assert meta["cvr"] == 2.0       # 2 / 100 * 100
    assert meta["cpa"] == 100.0
    assert meta["aov"] == 400.0
    # Shares are computed across both platforms.
    assert meta["clicks_share"] == round(100 / 150 * 100, 2)
    assert meta["revenue_share"] == round(800 / 1100 * 100, 2)


# ── _funnel_name_map ──────────────────────────────────────────────────────────
def test_funnel_name_map_reads_funnel_entities(empty_db):
    insert_rows(
        empty_db,
        "ad_names",
        [
            ad_name(platform="site", entity_type="funnel", entity_id="/checkout", name="Checkout Flow"),
            ad_name(platform="meta", entity_type="campaign", entity_id="cmp1", name="Not A Funnel"),
        ],
    )
    assert _funnel_name_map(empty_db) == {"/checkout": "Checkout Flow"}


# ── _build_funnel_snapshot ────────────────────────────────────────────────────
def test_funnel_snapshot_visits_leads_purchases_and_naming(empty_db):
    insert_rows(
        empty_db,
        "ad_names",
        [ad_name(platform="site", entity_type="funnel", entity_id="/checkout", name="Checkout Flow")],
    )
    insert_rows(
        empty_db,
        "sessions",
        [
            {"session_id": "s1", "ts": "2026-01-10T00:00:00Z", "customer_key": "c1", "landing_page": "/checkout"},
            {"session_id": "s2", "ts": "2026-01-10T00:00:00Z", "customer_key": "c2", "landing_page": "/pricing"},
        ],
    )
    insert_rows(
        empty_db,
        "conversions",
        [
            {"conversion_id": "k1", "ts": "2026-01-11T00:00:00Z", "type": "lead", "value": "0", "customer_key": "c1"},
            {"conversion_id": "k2", "ts": "2026-01-12T00:00:00Z", "type": "purchase", "value": "500", "customer_key": "c1"},
            {"conversion_id": "k3", "ts": "2026-01-12T00:00:00Z", "type": "purchase", "value": "200", "customer_key": "c2"},
        ],
    )
    rows = {r["funnel_id"]: r for r in _build_funnel_snapshot(empty_db, start_date=JAN, end_date=JAN_END)}

    checkout = rows["/checkout"]
    assert checkout["funnel_name"] == "Checkout Flow"  # from ad_names map
    assert checkout["visits"] == 1
    assert checkout["leads"] == 1
    assert checkout["purchases"] == 1
    assert checkout["revenue"] == 500.0
    assert checkout["lead_rate"] == 100.0
    assert checkout["aov"] == 500.0

    pricing = rows["/pricing"]
    assert pricing["funnel_name"] == "Pricing"  # humanised fallback
    assert pricing["purchases"] == 1
    assert pricing["revenue"] == 200.0


# ── _child_count_map ──────────────────────────────────────────────────────────
def test_child_count_map_counts_distinct_children(empty_db):
    insert_rows(
        empty_db,
        "spend",
        [
            spend(date="2026-01-10", platform="meta", account_id="acc", campaign_id="cmp1", adset_id="set1", ad_id="ad1"),
            spend(date="2026-01-10", platform="meta", account_id="acc", campaign_id="cmp1", adset_id="set2", ad_id="ad2"),
        ],
    )
    counts = _child_count_map(empty_db, start_date=JAN, end_date=JAN_END, active_tab="campaign")
    assert counts["meta|acc|cmp1"] == 2  # two distinct ad sets under the campaign


def test_child_count_map_ad_tab_has_no_children(empty_db):
    assert _child_count_map(empty_db, start_date=JAN, end_date=JAN_END, active_tab="ad") == {}
