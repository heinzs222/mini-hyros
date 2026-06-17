"""Unit tests for pure helper functions across the backend api/* modules."""

from __future__ import annotations

from datetime import date

import pytest
from fastapi import HTTPException

import api.ad_names as ad_names
import api.cohort as cohort
import api.journey as journey
import api.spend_sync as ss
from tests.helpers import ad_name, insert_rows


# ── spend_sync numeric / date helpers ─────────────────────────────────────────
@pytest.mark.parametrize(
    "value,expected",
    [("1,234.5", 1234.5), ("12", 12.0), ("1,000,000", 1_000_000.0), ("abc", 0.0), (None, 0.0)],
)
def test_num(value, expected):
    assert ss._num(value) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("1,234.56", 1234.56),
        ("(50)", -50.0),
        ("1,5", 1.5),          # comma decimal
        ("-", 0.0),
        ("—", 0.0),       # em dash
        ("1234", 1234.0),
    ],
)
def test_parse_number(value, expected):
    assert ss._parse_number(value) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        ("2026-01-05", "2026-01-05"),
        ("01/05/2026", "2026-01-05"),       # %m/%d/%Y
        ("Jan 5, 2026", "2026-01-05"),
        ("2026-01-05T12:00:00Z", "2026-01-05"),
        ("garbage", ""),
        ("", ""),
    ],
)
def test_parse_import_date(value, expected):
    assert ss._parse_import_date(value) == expected


def test_clean_id_strips_commas_and_space():
    assert ss._clean_id(" 12,345 ") == "12345"


@pytest.mark.parametrize("value,expected", [(0, "0"), (1.0, "1"), (1.5, "1.5"), (1.23456, "1.23456")])
def test_fmt_decimal(value, expected):
    assert ss._fmt_decimal(value) == expected


def test_normalize_header_strips_non_alphanumeric():
    assert ss._normalize_header(" Cost ($) ") == "cost"
    assert ss._normalize_header("Campaign Name") == "campaignname"


def test_normalize_date_range_orders_and_validates():
    assert ss._normalize_date_range("2026-01-10", "2026-01-20") == ("2026-01-10", "2026-01-20")
    # Reversed inputs get swapped.
    assert ss._normalize_date_range("2026-01-20", "2026-01-10") == ("2026-01-10", "2026-01-20")


def test_normalize_date_range_invalid_raises_400():
    with pytest.raises(HTTPException) as exc:
        ss._normalize_date_range("not-a-date", "2026-01-20")
    assert exc.value.status_code == 400


def test_default_dates_is_thirty_day_window():
    start, end = ss._default_dates()
    assert (date.fromisoformat(end) - date.fromisoformat(start)).days == 30


def test_spend_push_tokens_reads_env(monkeypatch):
    monkeypatch.delenv("GOOGLE_ADS_SCRIPT_TOKEN", raising=False)
    monkeypatch.delenv("SPEND_WEBHOOK_TOKEN", raising=False)
    monkeypatch.delenv("SITE_TOKEN", raising=False)
    assert ss._spend_push_tokens() == []
    monkeypatch.setenv("SITE_TOKEN", "abc")
    assert ss._spend_push_tokens() == ["abc"]


# ── cohort ────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("value,expected", [("2026-01-15T10:00:00Z", "2026-01"), ("2026-12-01", "2026-12"), (None, "unknown")])
def test_month_key(value, expected):
    assert cohort._month_key(value) == expected


# ── journey ───────────────────────────────────────────────────────────────────
def test_parse_ts_valid_and_invalid():
    assert journey._parse_ts("2026-01-15T00:00:00Z") is not None
    assert journey._parse_ts("") is None
    assert journey._parse_ts("nonsense") is None


@pytest.mark.parametrize(
    "start,end,expected",
    [
        ("2026-01-15T00:00:00", "2026-01-15T00:00:00", "<1 min"),
        ("2026-01-15T00:00:00", "2026-01-15T00:30:00", "30 min"),
        ("2026-01-15T00:00:00", "2026-01-15T02:00:00", "2.0 hours"),
        ("2026-01-15T00:00:00", "2026-01-17T00:00:00", "2.0 days"),
        ("2026-01-15T02:00:00", "2026-01-15T00:00:00", "<1 min"),  # negative clamps
        ("bad", "2026-01-15T00:00:00", ""),
    ],
)
def test_duration_label(start, end, expected):
    assert journey._duration_label(start, end) == expected


@pytest.mark.parametrize(
    "event,expected",
    [
        ({"type": "touchpoint", "details": {"platform": "meta", "campaign_name": "Camp"}}, "meta: Camp"),
        ({"type": "touchpoint", "details": {"platform": "meta"}}, "meta"),
        ({"type": "order", "details": {}}, "Purchase"),
        ({"type": "conversion", "details": {"conversion_type": "Lead"}}, "Lead"),
        ({"type": "session", "details": {"event_name": "AddToCart"}}, "AddToCart"),
        ({"type": "session", "details": {"event_name": "pageview", "page_title": "Home"}}, "Home"),
        ({"type": "weird", "details": {}}, "weird"),
    ],
)
def test_event_label(event, expected):
    assert journey._event_label(event) == expected


def test_entity_name_lookup():
    names = {("meta", "campaign", "c1"): "Camp One"}
    assert journey._entity_name(names, "meta", "campaign", "c1") == "Camp One"
    assert journey._entity_name(names, "meta", "campaign", "missing") == ""
    assert journey._entity_name(names, "meta", "campaign", "") == ""


# ── ad_names ──────────────────────────────────────────────────────────────────
def test_meta_api_version_default_and_override(monkeypatch):
    monkeypatch.delenv("META_API_VERSION", raising=False)
    assert ad_names._meta_api_version() == "v18.0"
    monkeypatch.setenv("META_API_VERSION", "v20.0")
    assert ad_names._meta_api_version() == "v20.0"
    assert ad_names._meta_url("act_1/campaigns").startswith("https://graph.facebook.com/v20.0/")


def test_get_name_map_includes_platform_and_raw_keys(api_db):
    insert_rows(
        api_db,
        "ad_names",
        [ad_name(platform="meta", entity_type="campaign", entity_id="cmp1", name="Camp 1")],
    )
    name_map = ad_names.get_name_map(api_db)
    assert name_map["meta|cmp1"] == "Camp 1"
    assert name_map["cmp1"] == "Camp 1"


def test_get_name_map_filters_by_entity_type(api_db):
    insert_rows(
        api_db,
        "ad_names",
        [
            ad_name(platform="meta", entity_type="campaign", entity_id="cmp1", name="Camp 1"),
            ad_name(platform="meta", entity_type="ad", entity_id="ad1", name="Ad 1"),
        ],
    )
    only_ads = ad_names.get_name_map(api_db, entity_type="ad")
    assert "ad1" in only_ads
    assert "cmp1" not in only_ads
