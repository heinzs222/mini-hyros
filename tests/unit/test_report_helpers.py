"""Unit tests for the pure helper functions in attributionops.report."""

from __future__ import annotations

import pytest

from attributionops.report import (
    _breakdown_key_has_value,
    _default_row_name_for_breakdown,
    _format_metrics,
    _format_model_label,
    _funnel_key_from_path,
    _humanize_funnel_name,
    _key_from_attrib_row,
    _key_from_spend_row,
    _normalize_landing_path,
    _platform_label,
    _tab_to_breakdown,
    _tabs,
    _traffic_source_for_platform,
)


# ── _format_metrics ───────────────────────────────────────────────────────────
def test_format_metrics_full_math():
    m = _format_metrics(
        clicks=100,
        cost=200.0,
        total_revenue=1000.0,
        revenue=800.0,
        cogs=100.0,
        fees=50.0,
        orders=10.0,
        reported=700.0,
        fixed_cost_allocation=None,
        impressions=10_000,
    )
    assert m["profit"] == 450.0          # 800 - 200 - 100 - 50
    assert m["net_profit"] == 450.0      # no fixed-cost allocation
    assert m["reported_delta"] == 100.0  # revenue - reported
    assert m["cpc"] == 2.0               # cost / clicks
    assert m["cpa"] == 20.0              # cost / orders
    assert m["roas"] == 4.0              # revenue / cost
    assert m["cvr"] == 10.0              # orders/clicks * 100
    assert m["aov"] == 80.0              # revenue / orders
    assert m["rpc"] == 8.0               # revenue / clicks
    assert m["margin_pct"] == 56.25      # profit/revenue * 100
    assert m["cpm"] == 20.0              # cost*1000 / impressions
    assert m["ctr"] == 1.0               # clicks/impressions * 100


def test_format_metrics_fixed_cost_allocation_reduces_net_profit():
    m = _format_metrics(
        clicks=10, cost=100.0, total_revenue=0.0, revenue=500.0, cogs=0.0, fees=0.0,
        orders=5.0, reported=None, fixed_cost_allocation=120.0,
    )
    assert m["profit"] == 400.0
    assert m["net_profit"] == 280.0  # 400 - 120


def test_format_metrics_zero_divisions_yield_none():
    m = _format_metrics(
        clicks=0, cost=0.0, total_revenue=0.0, revenue=0.0, cogs=0.0, fees=0.0,
        orders=0.0, reported=None, fixed_cost_allocation=None, impressions=0,
    )
    assert m["cpc"] is None
    assert m["cpa"] is None
    assert m["roas"] is None
    assert m["cvr"] is None
    assert m["aov"] is None
    assert m["margin_pct"] is None
    assert m["cpm"] is None
    assert m["ctr"] is None
    assert m["reported"] is None
    assert m["reported_delta"] is None


# ── label formatting ──────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    "model,label",
    [
        ("last_click", "Last Click"),
        ("first-click", "First Click"),
        ("time decay", "Time-Decay"),
        ("data_driven_proxy", "Data-Driven Proxy"),
        ("mystery", "mystery"),
    ],
)
def test_format_model_label(model, label):
    assert _format_model_label(model) == label


@pytest.mark.parametrize(
    "platform,source",
    [
        ("meta", "facebook / paid_social"),
        ("google", "google / cpc"),
        ("tiktok", "tiktok / paid_social"),
        ("snap", "unknown / paid"),
    ],
)
def test_traffic_source_for_platform(platform, source):
    assert _traffic_source_for_platform(platform) == source


@pytest.mark.parametrize(
    "platform,label",
    [("meta", "Facebook Ads"), ("google", "Google Ads"), ("tiktok", "TikTok Ads"), ("x", "Unknown")],
)
def test_platform_label(platform, label):
    assert _platform_label(platform) == label


# ── landing-path / funnel helpers ─────────────────────────────────────────────
@pytest.mark.parametrize(
    "raw,expected",
    [
        ("", "/"),
        ("/pricing", "/pricing"),
        ("pricing", "/pricing"),
        ("https://example.com/checkout/step?x=1#f", "/checkout/step"),
        ("https://example.com", "/"),
        ("https://example.com/?a=b", "/"),
    ],
)
def test_normalize_landing_path(raw, expected):
    assert _normalize_landing_path(raw) == expected


@pytest.mark.parametrize(
    "path,key",
    [("/checkout/step", "/checkout"), ("/", "/"), ("pricing/x", "/pricing")],
)
def test_funnel_key_from_path(path, key):
    assert _funnel_key_from_path(path) == key


@pytest.mark.parametrize(
    "key,name",
    [("/", "Homepage"), ("/order-form", "Order Form"), ("/vsl_page", "Vsl Page")],
)
def test_humanize_funnel_name(key, name):
    assert _humanize_funnel_name(key) == name


# ── breakdown key builders ────────────────────────────────────────────────────
def test_key_from_spend_row_levels():
    row = {
        "name": "src", "platform": "meta", "account_id": "acc",
        "campaign_id": "c", "adset_id": "s", "ad_id": "a",
    }
    assert _key_from_spend_row(row, "traffic_source") == "src"
    assert _key_from_spend_row(row, "ad_account") == "meta|acc"
    assert _key_from_spend_row(row, "campaign") == "meta|acc|c"
    assert _key_from_spend_row(row, "ad_set") == "meta|acc|c|s"
    assert _key_from_spend_row(row, "ad") == "meta|acc|c|s|a"


def test_key_from_spend_row_invalid_breakdown():
    with pytest.raises(ValueError):
        _key_from_spend_row({}, "bogus")


@pytest.mark.parametrize(
    "channel,platform,expected",
    [
        ("email", "", "newsletter / email"),
        ("organic", "", "organic / organic"),
        ("paid", "meta", "facebook / paid_social"),
        ("paid", "", "unknown / unknown"),
    ],
)
def test_key_from_attrib_row_traffic_source(channel, platform, expected):
    row = {"channel": channel, "platform": platform}
    assert _key_from_attrib_row(row, "traffic_source") == expected


def test_key_from_attrib_row_invalid_breakdown():
    with pytest.raises(ValueError):
        _key_from_attrib_row({}, "bogus")


@pytest.mark.parametrize(
    "breakdown,row,fallback,expected",
    [
        ("ad_account", {"account_id": "acc"}, "fb", "acc"),
        ("ad_account", {"platform": "meta"}, "fb", "meta account"),
        ("ad_account", {}, "fb", "fb"),
        ("campaign", {"campaign_id": "c1"}, "fb", "c1"),
        ("campaign", {}, "fb", "fb"),
        ("ad_set", {"adset_id": "s1"}, "fb", "s1"),
        ("ad", {"ad_id": "a1"}, "fb", "a1"),
        ("traffic_source", {}, "fb", "fb"),
    ],
)
def test_default_row_name_for_breakdown(breakdown, row, fallback, expected):
    assert _default_row_name_for_breakdown(row, breakdown, fallback) == expected


@pytest.mark.parametrize(
    "key,breakdown,expected",
    [
        ("meta|acc", "ad_account", True),
        ("|", "ad_account", False),
        ("meta|acc|c", "campaign", True),
        ("meta|acc|", "campaign", False),
        ("meta|acc|c|s", "ad_set", True),
        ("meta|acc|c|", "ad_set", False),
        ("meta|acc|c|s|a", "ad", True),
        ("meta|acc|c|s|", "ad", False),
        ("anything", "traffic_source", True),
    ],
)
def test_breakdown_key_has_value(key, breakdown, expected):
    assert _breakdown_key_has_value(key, breakdown) is expected


# ── tab metadata ──────────────────────────────────────────────────────────────
def test_tabs_lists_five_levels():
    keys = [t["key"] for t in _tabs()]
    assert keys == ["traffic_source", "ad_account", "campaign", "ad_set", "ad"]


@pytest.mark.parametrize("tab", ["traffic_source", "ad_account", "campaign", "ad_set", "ad"])
def test_tab_to_breakdown_is_identity(tab):
    assert _tab_to_breakdown(tab) == tab
