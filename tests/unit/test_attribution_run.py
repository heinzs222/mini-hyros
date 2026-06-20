"""Golden-value tests for attribution_run / attribution_day_totals.

These exercise the core attribution engine against small, fully-controlled
SQLite fixtures so every attributed dollar can be asserted exactly.
"""

from __future__ import annotations

import pytest

from attributionops.tools.attribution import attribution_day_totals, attribution_run
from tests.helpers import insert_rows, order, spend, touchpoint

JAN = "2026-01-01"
JAN_END = "2026-01-31"


def _seed_account_spend(db: str) -> None:
    """Spend rows that let the engine resolve account_id for the meta ads."""
    insert_rows(
        db,
        "spend",
        [
            spend(date="2026-01-10", ad_id="ad_meta_1"),
            spend(date="2026-01-14", ad_id="ad_meta_2"),
        ],
    )


def _row_by_ad(result: dict) -> dict[str, dict]:
    return {r["ad_id"]: r for r in result["rows"]}


# ── input validation ──────────────────────────────────────────────────────────
def test_non_purchase_conversion_type_raises(empty_db):
    with pytest.raises(ValueError, match="conversion_type=Purchase"):
        attribution_run(
            empty_db,
            model="last_click",
            start_date=JAN,
            end_date=JAN_END,
            lookback_days=30,
            conversion_type="Lead",
            value_type="revenue",
        )


def test_invalid_date_basis_raises(empty_db):
    with pytest.raises(ValueError, match="date_basis"):
        attribution_run(
            empty_db,
            model="last_click",
            start_date=JAN,
            end_date=JAN_END,
            lookback_days=30,
            conversion_type="Purchase",
            value_type="revenue",
            date_basis="weird",
        )


def test_invalid_value_type_raises(empty_db):
    insert_rows(empty_db, "touchpoints", [touchpoint("2026-01-14T00:00:00Z", "c1", ad_id="ad_meta_2")])
    insert_rows(empty_db, "orders", [order("o1", "2026-01-15T00:00:00Z", "c1", gross=120, net=100)])
    with pytest.raises(ValueError, match="value_type must be one of"):
        attribution_run(
            empty_db,
            model="last_click",
            start_date=JAN,
            end_date=JAN_END,
            lookback_days=30,
            conversion_type="Purchase",
            value_type="nonsense",
        )


# ── last-click golden values + unattributed accounting ────────────────────────
def test_last_click_full_scenario(empty_db):
    _seed_account_spend(empty_db)
    insert_rows(
        empty_db,
        "touchpoints",
        [
            # C1: two touches inside the lookback window.
            touchpoint("2026-01-10T09:00:00Z", "c1", ad_id="ad_meta_1", creative_id="cr1"),
            touchpoint("2026-01-14T09:00:00Z", "c1", ad_id="ad_meta_2", creative_id="cr2"),
            # C3: touch BEFORE the 30-day lookback window -> order is unattributed.
            touchpoint("2025-11-01T09:00:00Z", "c3", ad_id="ad_meta_1"),
            # C4: touch AFTER the order timestamp -> excluded -> order unattributed.
            touchpoint("2026-01-20T09:00:00Z", "c4", ad_id="ad_meta_2"),
        ],
    )
    insert_rows(
        empty_db,
        "orders",
        [
            order("o1", "2026-01-15T12:00:00Z", "c1", gross=120, net=100, cogs=30, fees=10),
            order("o2", "2026-01-15T12:00:00Z", "c2", gross=50, net=40),  # no touchpoints
            order("o3", "2026-01-15T12:00:00Z", "c3", gross=50, net=40),  # touch out of window
            order("o4", "2026-01-15T12:00:00Z", "c4", gross=50, net=40),  # touch after order
        ],
    )

    result = attribution_run(
        empty_db,
        model="last_click",
        start_date=JAN,
        end_date=JAN_END,
        lookback_days=30,
        conversion_type="Purchase",
        value_type="revenue",
    )

    assert result["unattributed_orders"] == 3
    rows = _row_by_ad(result)
    # Both of C1's touches appear, but last-click puts all value on the final one;
    # the earlier touch shows up with zeroed metrics.
    assert set(rows) == {"ad_meta_1", "ad_meta_2"}
    assert rows["ad_meta_1"]["revenue"] == 0.0
    assert rows["ad_meta_1"]["orders"] == 0.0
    row = rows["ad_meta_2"]
    assert row["account_id"] == "acc_meta"  # resolved from the spend table
    assert row["orders"] == 1.0
    assert row["total_revenue"] == 120.00
    assert row["revenue"] == 100.00
    assert row["cogs"] == 30.00
    assert row["fees"] == 10.00
    assert row["primary_value"] == 100.00  # value_type=revenue -> net


def test_first_click_credits_first_touch(empty_db):
    _seed_account_spend(empty_db)
    insert_rows(
        empty_db,
        "touchpoints",
        [
            touchpoint("2026-01-10T09:00:00Z", "c1", ad_id="ad_meta_1"),
            touchpoint("2026-01-14T09:00:00Z", "c1", ad_id="ad_meta_2"),
        ],
    )
    insert_rows(empty_db, "orders", [order("o1", "2026-01-15T12:00:00Z", "c1", net=100)])

    rows = _row_by_ad(
        attribution_run(
            empty_db,
            model="first_click",
            start_date=JAN,
            end_date=JAN_END,
            lookback_days=30,
            conversion_type="Purchase",
            value_type="revenue",
        )
    )
    assert set(rows) == {"ad_meta_1", "ad_meta_2"}
    assert rows["ad_meta_1"]["revenue"] == 100.00
    assert rows["ad_meta_2"]["revenue"] == 0.0


def test_linear_splits_revenue_across_touches(empty_db):
    _seed_account_spend(empty_db)
    insert_rows(
        empty_db,
        "touchpoints",
        [
            touchpoint("2026-01-10T09:00:00Z", "c1", ad_id="ad_meta_1"),
            touchpoint("2026-01-14T09:00:00Z", "c1", ad_id="ad_meta_2"),
        ],
    )
    insert_rows(empty_db, "orders", [order("o1", "2026-01-15T12:00:00Z", "c1", net=100)])

    rows = _row_by_ad(
        attribution_run(
            empty_db,
            model="linear",
            start_date=JAN,
            end_date=JAN_END,
            lookback_days=30,
            conversion_type="Purchase",
            value_type="revenue",
        )
    )
    assert rows["ad_meta_1"]["revenue"] == 50.00
    assert rows["ad_meta_2"]["revenue"] == 50.00
    assert rows["ad_meta_1"]["orders"] == 0.5
    assert rows["ad_meta_2"]["orders"] == 0.5


@pytest.mark.parametrize(
    "value_type,expected",
    [
        ("total_revenue", 120.00),
        ("gross", 120.00),
        ("revenue", 100.00),
        ("net", 100.00),
        ("cogs", 30.00),
        ("fees", 10.00),
        ("orders", 1.0),
    ],
)
def test_value_type_selects_primary_value(empty_db, value_type, expected):
    _seed_account_spend(empty_db)
    insert_rows(empty_db, "touchpoints", [touchpoint("2026-01-14T09:00:00Z", "c1", ad_id="ad_meta_2")])
    insert_rows(
        empty_db,
        "orders",
        [order("o1", "2026-01-15T12:00:00Z", "c1", gross=120, net=100, cogs=30, fees=10)],
    )
    result = attribution_run(
        empty_db,
        model="last_click",
        start_date=JAN,
        end_date=JAN_END,
        lookback_days=30,
        conversion_type="Purchase",
        value_type=value_type,
    )
    assert result["rows"][0]["primary_value"] == expected


# ── lookback window boundary ──────────────────────────────────────────────────
def test_touchpoint_exactly_at_window_start_is_included(empty_db):
    # window_start == order_ts - lookback; the condition is `tp_ts < window_start`,
    # so a touch exactly on the boundary must be kept.
    insert_rows(empty_db, "touchpoints", [touchpoint("2026-01-21T00:00:00Z", "c1", ad_id="ad_x")])
    insert_rows(empty_db, "orders", [order("o1", "2026-01-31T00:00:00Z", "c1", net=100)])
    result = attribution_run(
        empty_db,
        model="last_click",
        start_date=JAN,
        end_date=JAN_END,
        lookback_days=10,
        conversion_type="Purchase",
        value_type="revenue",
    )
    assert result["unattributed_orders"] == 0
    assert result["rows"][0]["revenue"] == 100.00


def test_touchpoint_one_second_before_window_is_excluded(empty_db):
    insert_rows(empty_db, "touchpoints", [touchpoint("2026-01-20T23:59:59Z", "c1", ad_id="ad_x")])
    insert_rows(empty_db, "orders", [order("o1", "2026-01-31T00:00:00Z", "c1", net=100)])
    result = attribution_run(
        empty_db,
        model="last_click",
        start_date=JAN,
        end_date=JAN_END,
        lookback_days=10,
        conversion_type="Purchase",
        value_type="revenue",
    )
    assert result["unattributed_orders"] == 1
    assert result["rows"] == []


# ── date-of-click attribution ─────────────────────────────────────────────────
def test_click_date_basis_excludes_clicks_outside_report_window(empty_db):
    insert_rows(empty_db, "touchpoints", [touchpoint("2026-01-08T09:00:00Z", "c1", ad_id="ad_meta_2")])
    insert_rows(empty_db, "orders", [order("o1", "2026-01-12T12:00:00Z", "c1", net=100)])

    # Conversion basis: the order date drives inclusion -> attributed.
    conv = attribution_run(
        empty_db,
        model="last_click",
        start_date=JAN,
        end_date=JAN_END,
        lookback_days=30,
        conversion_type="Purchase",
        value_type="revenue",
        date_basis="conversion",
    )
    assert conv["rows"][0]["revenue"] == 100.00

    # Click basis with a window starting AFTER the click date -> the click is
    # filtered out even though the order is still selected.
    click = attribution_run(
        empty_db,
        model="last_click",
        start_date="2026-01-10",
        end_date=JAN_END,
        lookback_days=30,
        conversion_type="Purchase",
        value_type="revenue",
        date_basis="click",
    )
    assert click["rows"] == []
    assert click["unattributed_orders"] == 0  # window was non-empty; just date-filtered


# ── day totals helper ─────────────────────────────────────────────────────────
def test_attribution_day_totals_groups_by_conversion_date(empty_db):
    insert_rows(
        empty_db,
        "touchpoints",
        [
            touchpoint("2026-01-14T09:00:00Z", "c1", ad_id="ad_meta_2"),
            touchpoint("2026-01-14T09:00:00Z", "c2", ad_id="ad_meta_2"),
        ],
    )
    insert_rows(
        empty_db,
        "orders",
        [
            order("o1", "2026-01-15T12:00:00Z", "c1", gross=120, net=100, cogs=30, fees=10),
            order("o2", "2026-01-16T12:00:00Z", "c2", gross=60, net=50, cogs=15, fees=5),
        ],
    )
    result = attribution_day_totals(
        empty_db,
        model="last_click",
        start_date=JAN,
        end_date=JAN_END,
        lookback_days=30,
        conversion_type="Purchase",
    )
    by_day = {r["date"]: r for r in result["rows"]}
    assert by_day["2026-01-15"]["revenue"] == 100.00
    assert by_day["2026-01-15"]["orders"] == 1.0
    assert by_day["2026-01-16"]["revenue"] == 50.00
    assert by_day["2026-01-16"]["total_revenue"] == 60.00


def test_attribution_day_totals_click_basis_groups_by_click_date(empty_db):
    insert_rows(empty_db, "touchpoints", [touchpoint("2026-01-14T09:00:00Z", "c1", ad_id="ad1")])
    insert_rows(empty_db, "orders", [order("o1", "2026-01-15T12:00:00Z", "c1", net=100)])
    result = attribution_day_totals(
        empty_db,
        model="last_click",
        start_date=JAN,
        end_date=JAN_END,
        lookback_days=30,
        conversion_type="Purchase",
        date_basis="click",
    )
    by_day = {r["date"]: r for r in result["rows"]}
    # Revenue lands on the click date (Jan 14), not the conversion date (Jan 15).
    assert "2026-01-14" in by_day
    assert by_day["2026-01-14"]["revenue"] == 100.00
    assert "2026-01-15" not in by_day


def test_attribution_day_totals_invalid_date_basis_raises(empty_db):
    with pytest.raises(ValueError, match="date_basis"):
        attribution_day_totals(
            empty_db,
            model="last_click",
            start_date=JAN,
            end_date=JAN_END,
            lookback_days=30,
            conversion_type="Purchase",
            date_basis="bad",
        )


def test_attribution_day_totals_counts_unattributed(empty_db):
    insert_rows(empty_db, "orders", [order("o1", "2026-01-15T12:00:00Z", "c1", net=100)])
    result = attribution_day_totals(
        empty_db,
        model="last_click",
        start_date=JAN,
        end_date=JAN_END,
        lookback_days=30,
        conversion_type="Purchase",
    )
    assert result["unattributed_orders"] == 1
    assert result["rows"] == []
