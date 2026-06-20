"""Unit tests for attributionops.tools.forecasting."""

from __future__ import annotations

import pytest

from attributionops.tools.forecasting import forecasting_run
from tests.helpers import insert_rows, order

JAN = "2026-01-01"
MAR_END = "2026-03-31"


def test_invalid_method_raises(empty_db):
    with pytest.raises(ValueError, match="method must be one of"):
        forecasting_run(empty_db, start_date=JAN, end_date=MAR_END, cohort_window=30, method="ml")


def test_cohort_groups_by_first_order_date(empty_db):
    insert_rows(
        empty_db,
        "orders",
        [
            order("o1", "2026-01-10T00:00:00Z", "c1", net=100),
            order("o2", "2026-01-15T00:00:00Z", "c1", net=50),   # same customer, within window
            order("o3", "2026-01-10T00:00:00Z", "c2", net=200),  # same cohort date
        ],
    )
    result = forecasting_run(empty_db, start_date=JAN, end_date=MAR_END, cohort_window=30, method="simple")
    assert len(result["rows"]) == 1
    row = result["rows"][0]
    assert row["cohort_date"] == "2026-01-10"
    assert row["customers"] == 2
    assert row["net_revenue_sum"] == 350.0  # 100 + 50 + 200
    assert row["avg_net_revenue_per_customer"] == 175.0
    assert row["cohort_window_days"] == 30


def test_orders_outside_cohort_window_are_excluded(empty_db):
    insert_rows(
        empty_db,
        "orders",
        [
            order("o1", "2026-01-10T00:00:00Z", "c1", net=100),
            order("o2", "2026-02-20T00:00:00Z", "c1", net=500),  # 41 days later, window=30
        ],
    )
    result = forecasting_run(empty_db, start_date=JAN, end_date=MAR_END, cohort_window=30, method="historical")
    row = result["rows"][0]
    assert row["net_revenue_sum"] == 100.0  # the 500 order falls outside the 30-day window
