"""HYROS-parity sale-group metrics (Stage 1b) and NET CAC denominator.

Definitions were reverse-engineered and validated to the integer against a real
HYROS account export:
- a sale group whose (buyer identity, net amount) occurred within the trailing
  30 days is a RENEWAL;
- New Customers = non-renewal sale groups in window (refunds irrelevant);
- Sales = non-renewal sale groups with no refund;
- AOV = revenue / ALL sale groups;
- NET CAC = cost / first-ever buyers in window (net_new_customers).
"""

from __future__ import annotations

from attributionops.report import ReportInputs, build_hyros_like_report
from tests.helpers import insert_rows, order, spend, touchpoint


def _inputs(start="2026-01-10", end="2026-01-20") -> ReportInputs:
    return ReportInputs(
        report_name="t",
        start_date=start,
        end_date=end,
        preset=None,
        currency="USD",
        attribution_model="last_click",
        lookback_days=30,
        conversion_type="Purchase",
        use_date_of_click_attribution=False,
        active_tab="traffic_source",
    )


def test_renewal_excluded_from_sales_and_new_customers(empty_db):
    insert_rows(empty_db, "spend", [spend(date="2026-01-12", clicks=10, cost=100)])
    insert_rows(
        empty_db,
        "orders",
        [
            # First-ever charge 10 days before the window: makes the in-window
            # same-amount charge a renewal.
            order("o_prior", "2026-01-02T10:00:00Z", "cust_a", gross=50, net=50),
            # In-window renewal of the same (identity, amount) pair.
            order("o_renew", "2026-01-12T10:00:00Z", "cust_a", gross=50, net=50),
            # In-window first-time purchase from a different buyer.
            order("o_new", "2026-01-13T10:00:00Z", "cust_b", gross=200, net=200),
        ],
    )
    report = build_hyros_like_report(empty_db, _inputs())
    s = report["summary_totals"]
    assert s["tracked_sale_groups"] == 2  # both in-window checkouts
    assert s["hyros_new_customers"] == 1  # renewal excluded
    assert s["hyros_sales_count"] == 1


def test_refunded_group_counts_as_customer_but_not_sale(empty_db):
    insert_rows(
        empty_db,
        "orders",
        [
            order("o_ok", "2026-01-12T10:00:00Z", "cust_a", gross=100, net=100),
            order("o_ref", "2026-01-13T10:00:00Z", "cust_b", gross=80, net=30, refunds=50),
        ],
    )
    report = build_hyros_like_report(empty_db, _inputs())
    s = report["summary_totals"]
    assert s["tracked_sale_groups"] == 2
    # HYROS: refunded checkouts still count toward New Customers...
    assert s["hyros_new_customers"] == 2
    # ...but are excluded from the Sales widget.
    assert s["hyros_sales_count"] == 1


def test_multi_charge_sale_group_counts_once(empty_db):
    insert_rows(
        empty_db,
        "orders",
        [
            order("o1", "2026-01-12T10:00:00Z", "cust_a", gross=100, net=100, sale_group_id="grp1"),
            order("o2", "2026-01-12T10:03:00Z", "cust_a", gross=40, net=40, sale_group_id="grp1"),
        ],
    )
    report = build_hyros_like_report(empty_db, _inputs())
    s = report["summary_totals"]
    assert s["all_orders_count"] == 2  # raw charges
    assert s["tracked_sale_groups"] == 1  # one checkout
    assert s["hyros_sales_count"] == 1
    # AOV divides by sale groups, not charges: 140 / 1.
    assert s["all_orders_aov"] == 140.0


def test_net_cac_divides_by_first_ever_buyers(empty_db):
    insert_rows(empty_db, "spend", [spend(date="2026-01-12", clicks=100, cost=300)])
    insert_rows(
        empty_db,
        "orders",
        [
            # Returning buyer: first-ever purchase long before the window.
            order("o_old", "2025-06-01T10:00:00Z", "cust_ret", gross=90, net=90,
                  processor="stripe", processor_customer_id="cus_ret"),
            order("o_ret", "2026-01-12T10:00:00Z", "cust_ret", gross=120, net=120,
                  processor="stripe", processor_customer_id="cus_ret"),
            # Genuinely first-ever buyer inside the window.
            order("o_new", "2026-01-13T10:00:00Z", "cust_new", gross=200, net=200,
                  processor="stripe", processor_customer_id="cus_new"),
        ],
    )
    report = build_hyros_like_report(empty_db, _inputs())
    s = report["summary_totals"]
    assert s["net_new_customers"] == 1
    # NET CAC = cost / first-ever buyers, NOT cost / all window buyers (2).
    assert s["cac"] == 300.0
