from __future__ import annotations

import sqlite3

from attributionops.report_integrity import (
    build_dimension_coverage,
    resolve_attribution_dimensions,
    visible_report_columns,
)
from attributionops.tools.ads import ads_get_spend
from attributionops.tools.tracking import tracking_health_check


def test_campaign_id_or_unique_name_resolves_to_spend_account() -> None:
    spend = [
        {
            "platform": "google",
            "account_id": "5655721748",
            "campaign_id": "22681332420",
            "adset_id": "",
            "ad_id": "",
            "name": "PMax QC Broad Prospecting",
        }
    ]
    attribution = [
        {
            "platform": "google",
            "account_id": "",
            "campaign_id": "PMax QC Broad Prospecting",
            "orders": 1,
            "revenue": 449,
        },
        {
            "platform": "google",
            "account_id": "",
            "campaign_id": "22681332420",
            "orders": 2,
            "revenue": 898,
        },
    ]

    resolved = resolve_attribution_dimensions(spend, attribution, active_tab="campaign")

    assert {row["account_id"] for row in resolved} == {"5655721748"}
    assert {row["campaign_id"] for row in resolved} == {"22681332420"}
    coverage = build_dimension_coverage(resolved, active_tab="campaign")
    assert coverage["source_attributed_orders"] == 3
    assert coverage["dimension_attributed_orders"] == 3
    assert coverage["unmapped_orders"] == 0


def test_ambiguous_campaign_name_is_not_guessed() -> None:
    spend = [
        {
            "platform": "meta",
            "account_id": "1",
            "campaign_id": "100",
            "adset_id": "",
            "ad_id": "",
            "name": "Sales",
        },
        {
            "platform": "meta",
            "account_id": "1",
            "campaign_id": "200",
            "adset_id": "",
            "ad_id": "",
            "name": "Sales",
        },
    ]
    attribution = [
        {
            "platform": "meta",
            "account_id": "",
            "campaign_id": "Sales",
            "orders": 1,
            "revenue": 100,
        }
    ]

    resolved = resolve_attribution_dimensions(spend, attribution, active_tab="campaign")

    assert resolved[0]["campaign_id"] == "Sales"
    assert resolved[0]["account_id"] == ""
    assert resolved[0]["_dimension_resolution"] == "unmatched"
    coverage = build_dimension_coverage(resolved, active_tab="campaign")
    # The alias remains visible for investigation, but it is not counted as
    # platform-mapped because two real campaigns share the same display name.
    assert coverage["dimension_identifier_orders"] == 1
    assert coverage["dimension_attributed_orders"] == 0
    assert coverage["unmatched_identifier_orders"] == 1
    assert coverage["unmapped_orders"] == 1


def test_reported_columns_hide_without_reported_feed() -> None:
    columns = [
        {"key": "orders"},
        {"key": "reported"},
        {"key": "reported_delta"},
    ]
    assert visible_report_columns(columns, reported_rows=[]) == [{"key": "orders"}]
    assert visible_report_columns(columns, reported_rows=[{"reported_value": 1}]) == columns


def test_ads_get_spend_hides_only_zero_activity_entities(tmp_path) -> None:
    db_path = str(tmp_path / "report.sqlite")
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """CREATE TABLE spend (
                platform TEXT, date TEXT, account_id TEXT, campaign_id TEXT,
                adset_id TEXT, ad_id TEXT, creative_id TEXT,
                clicks TEXT, cost TEXT, impressions TEXT, metadata TEXT
            )"""
        )
        conn.executemany(
            "INSERT INTO spend VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            [
                ("tiktok", "2026-07-05", "739", "zero", "", "", "", "0", "0", "0", "{}"),
                ("tiktok", "2026-07-05", "739", "live", "", "", "", "3", "12.5", "100", "{}"),
            ],
        )
        conn.commit()

    rows = ads_get_spend(
        db_path,
        platform="all",
        start_date="2026-07-04",
        end_date="2026-07-11",
        breakdown="campaign",
    )["rows"]

    assert [row["campaign_id"] for row in rows] == ["live"]


def test_tracking_health_uses_selected_range_not_lifetime(tmp_path) -> None:
    db_path = str(tmp_path / "tracking.sqlite")
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE sessions (
                session_id TEXT, visitor_id TEXT, ts TEXT,
                gclid TEXT, fbclid TEXT, ttclid TEXT, customer_key TEXT
            );
            CREATE TABLE orders (
                order_id TEXT, ts TEXT, customer_key TEXT,
                session_id TEXT, visitor_id TEXT, channel TEXT, platform TEXT,
                campaign_id TEXT, adset_id TEXT, ad_id TEXT, creative_id TEXT,
                gclid TEXT, fbclid TEXT, ttclid TEXT
            );
            CREATE TABLE conversions (
                order_id TEXT, customer_key TEXT, session_id TEXT, visitor_id TEXT
            );
            CREATE TABLE touchpoints (
                ts TEXT, channel TEXT, platform TEXT, campaign_id TEXT,
                adset_id TEXT, ad_id TEXT, creative_id TEXT,
                gclid TEXT, fbclid TEXT, ttclid TEXT,
                customer_key TEXT, session_id TEXT, visitor_id TEXT
            );
            """
        )
        # Lifetime history is healthy, while the selected July range contains one
        # session/order with no source. The range-scoped result must show 0/1.
        conn.execute(
            "INSERT INTO sessions VALUES (?,?,?,?,?,?,?)",
            ("old", "old-v", "2026-01-01T12:00:00Z", "g-old", "", "", "old-c"),
        )
        conn.execute(
            "INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("old-o", "2026-01-01T13:00:00Z", "old-c", "old", "old-v", "paid_search", "google", "1", "", "", "", "g-old", "", ""),
        )
        conn.execute(
            "INSERT INTO sessions VALUES (?,?,?,?,?,?,?)",
            ("new", "new-v", "2026-07-05T12:00:00Z", "", "", "", "new-c"),
        )
        conn.execute(
            "INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("new-o", "2026-07-05T13:00:00Z", "new-c", "new", "new-v", "", "", "", "", "", "", "", "", ""),
        )
        conn.commit()

    result = tracking_health_check(
        db_path,
        start_date="2026-07-04",
        end_date="2026-07-11",
    )

    assert result["scope"]["type"] == "date_range"
    assert result["coverage"]["orders_total"] == 1
    assert result["coverage"]["orders_with_source"] == 0
    assert result["coverage"]["sessions_total"] == 1
    assert result["coverage"]["sessions_with_click_id"] == 0
