"""Unit tests for attributionops.tools.ads."""

from __future__ import annotations

import json

import pytest

from attributionops.tools.ads import (
    ads_get_reported_value,
    ads_get_spend,
    ads_list_platforms,
)
from tests.helpers import insert_rows, spend

JAN = "2026-01-01"
JAN_END = "2026-01-31"


@pytest.fixture
def spend_db(empty_db):
    insert_rows(
        empty_db,
        "spend",
        [
            spend(date="2026-01-10", platform="meta", account_id="acc_meta",
                  campaign_id="cmp1", adset_id="set1", ad_id="ad1",
                  clicks=10, cost=5, impressions=1000,
                  metadata=json.dumps({"campaign_name": "C1", "adset_name": "S1", "ad_name": "A1"})),
            spend(date="2026-01-11", platform="meta", account_id="acc_meta",
                  campaign_id="cmp1", adset_id="set1", ad_id="ad1",
                  clicks=20, cost=7.5, impressions=2000, metadata="{}"),
            spend(date="2026-01-10", platform="google", account_id="acc_google",
                  campaign_id="cmp2", adset_id="set2", ad_id="ad2",
                  clicks=5, cost=3, impressions=500, metadata="{}"),
            # Outside the report window -> must be excluded.
            spend(date="2025-12-01", platform="meta", account_id="acc_meta",
                  campaign_id="cmp1", adset_id="set1", ad_id="ad1",
                  clicks=999, cost=999, impressions=999, metadata="{}"),
        ],
    )
    return empty_db


def _by_name(result):
    return {r["name"]: r for r in result["rows"]}


def test_list_platforms_distinct_sorted(spend_db):
    assert ads_list_platforms(spend_db) == {"platforms": ["google", "meta"]}


def test_get_spend_campaign_breakdown_aggregates(spend_db):
    rows = _by_name(ads_get_spend(spend_db, platform="all", start_date=JAN, end_date=JAN_END, breakdown="campaign"))
    # Meta campaign name resolves from metadata; counts sum across both January rows.
    assert rows["C1"]["clicks"] == 30
    assert rows["C1"]["cost"] == 12.5
    assert rows["C1"]["impressions"] == 3000
    # Google row has empty metadata -> name falls back to campaign_id.
    assert rows["cmp2"]["clicks"] == 5
    assert rows["cmp2"]["cost"] == 3.0


def test_get_spend_day_breakdown(spend_db):
    rows = _by_name(ads_get_spend(spend_db, platform="all", start_date=JAN, end_date=JAN_END, breakdown="day"))
    assert rows["2026-01-10"]["clicks"] == 15  # meta(10) + google(5)
    assert rows["2026-01-10"]["cost"] == 8.0
    assert rows["2026-01-11"]["clicks"] == 20


def test_get_spend_traffic_source_breakdown(spend_db):
    rows = _by_name(ads_get_spend(spend_db, platform="all", start_date=JAN, end_date=JAN_END, breakdown="traffic_source"))
    assert "facebook / paid_social" in rows
    assert "google / cpc" in rows
    assert rows["facebook / paid_social"]["clicks"] == 30


def test_get_spend_platform_breakdown(spend_db):
    rows = _by_name(ads_get_spend(spend_db, platform="all", start_date=JAN, end_date=JAN_END, breakdown="platform"))
    assert rows["meta"]["clicks"] == 30
    assert rows["meta"]["cost"] == 12.5
    assert rows["google"]["clicks"] == 5
    assert rows["google"]["cost"] == 3.0


def test_get_spend_platform_filter(spend_db):
    result = ads_get_spend(spend_db, platform="meta", start_date=JAN, end_date=JAN_END, breakdown="ad")
    platforms = {r["platform"] for r in result["rows"]}
    assert platforms == {"meta"}


def test_get_spend_excludes_rows_outside_date_window(spend_db):
    rows = _by_name(ads_get_spend(spend_db, platform="meta", start_date=JAN, end_date=JAN_END, breakdown="campaign"))
    # The Dec-01 row (clicks=999) must not leak into the January total.
    assert rows["C1"]["clicks"] == 30


def test_get_spend_metadata_is_json_encoded(spend_db):
    rows = _by_name(ads_get_spend(spend_db, platform="meta", start_date=JAN, end_date=JAN_END, breakdown="campaign"))
    meta = json.loads(rows["C1"]["metadata"])
    assert meta["campaign_name"] == "C1"


def test_get_spend_ad_account_breakdown(spend_db):
    rows = _by_name(ads_get_spend(spend_db, platform="all", start_date=JAN, end_date=JAN_END, breakdown="ad_account"))
    assert rows["acc_meta"]["clicks"] == 30
    assert "acc_google" in rows


def test_get_spend_ad_set_and_ad_use_metadata_names(spend_db):
    rows_set = _by_name(ads_get_spend(spend_db, platform="meta", start_date=JAN, end_date=JAN_END, breakdown="ad_set"))
    assert "S1" in rows_set  # adset_name from metadata
    rows_ad = _by_name(ads_get_spend(spend_db, platform="meta", start_date=JAN, end_date=JAN_END, breakdown="ad"))
    assert "A1" in rows_ad  # ad_name from metadata


def test_get_spend_invalid_breakdown_raises(spend_db):
    with pytest.raises(ValueError, match="breakdown must be one of"):
        ads_get_spend(spend_db, platform="all", start_date=JAN, end_date=JAN_END, breakdown="nope")


# ── reported value ────────────────────────────────────────────────────────────
@pytest.fixture
def reported_db(empty_db):
    insert_rows(
        empty_db,
        "reported_value",
        [
            {"platform": "meta", "date": "2026-01-10", "account_id": "acc_meta",
             "campaign_id": "cmp1", "adset_id": "set1", "ad_id": "ad1",
             "conversion_type": "Purchase", "reported_value": "100"},
            {"platform": "meta", "date": "2026-01-11", "account_id": "acc_meta",
             "campaign_id": "cmp1", "adset_id": "set1", "ad_id": "ad1",
             "conversion_type": "Purchase", "reported_value": "50"},
            # Different conversion type -> excluded when querying Purchase.
            {"platform": "meta", "date": "2026-01-10", "account_id": "acc_meta",
             "campaign_id": "cmp1", "adset_id": "set1", "ad_id": "ad1",
             "conversion_type": "Lead", "reported_value": "999"},
        ],
    )
    return empty_db


def test_reported_value_sums_matching_conversion_type(reported_db):
    rows = ads_get_reported_value(reported_db, platform="all", start_date=JAN, end_date=JAN_END,
                                  breakdown="campaign", conversion_type="Purchase")["rows"]
    assert len(rows) == 1
    assert rows[0]["reported_value"] == 150.0  # 100 + 50, Lead excluded


@pytest.mark.parametrize("breakdown", ["traffic_source", "ad_account", "campaign", "ad_set", "ad"])
def test_reported_value_single_group_breakdowns_sum_to_total(reported_db, breakdown):
    rows = ads_get_reported_value(reported_db, platform="all", start_date=JAN, end_date=JAN_END,
                                  breakdown=breakdown, conversion_type="Purchase")["rows"]
    assert len(rows) == 1
    assert rows[0]["reported_value"] == 150.0


def test_reported_value_day_breakdown_splits_by_date(reported_db):
    rows = {r["name"]: r for r in ads_get_reported_value(
        reported_db, platform="all", start_date=JAN, end_date=JAN_END,
        breakdown="day", conversion_type="Purchase")["rows"]}
    assert rows["2026-01-10"]["reported_value"] == 100.0
    assert rows["2026-01-11"]["reported_value"] == 50.0


def test_reported_value_platform_filter(reported_db):
    rows = ads_get_reported_value(reported_db, platform="google", start_date=JAN, end_date=JAN_END,
                                  breakdown="campaign", conversion_type="Purchase")["rows"]
    assert rows == []  # no google reported values seeded


def test_reported_value_invalid_breakdown_raises(reported_db):
    with pytest.raises(ValueError, match="breakdown must be one of"):
        ads_get_reported_value(reported_db, platform="all", start_date=JAN, end_date=JAN_END,
                               breakdown="nope", conversion_type="Purchase")
