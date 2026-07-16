"""Unit coverage for attributionops/hyros_import.py.

Small synthetic CSV fixtures (written via tmp_path) exercise the validated
HYROS metric definitions: sale-group aggregation, the refund/PENDING filters
on `sales`, 30-day renewal exclusion, quarantine of malformed rows, the
new-leads compound filter, and UTC-06 window bucketing of the sales Date
format.
"""

from __future__ import annotations

from datetime import datetime

from attributionops.hyros_import import (
    compute_leads_metrics,
    compute_sales_metrics,
    parse_leads_csv,
    parse_sales_csv,
)

SALES_HEADER = (
    "Email,First Name,Last Name,Income,Date,Phones,Sale Group,Cost of goods,"
    "Shipping Value,Taxes,Refund,Name,Origin Source,Last Source,Status,Info,Order Name"
)
LEADS_HEADER = (
    "Email,Phone Numbers,First name,Last name,IPs,Stage,Parent Email,"
    "Income Generated,Join Date,Last Source Date,First source,Last source,"
    " Tracked URL, Previous URL,Ad Optimization Consent,Status,Error message"
)

WINDOW = ("2026-07-01", "2026-07-15")


def sale_line(
    email: str,
    income: str,
    ts: str,
    group: str,
    *,
    taxes: str = "0",
    refund: str = "0",
    status: str = "SENT",
    source: str = "Ad Campaign A",
) -> str:
    return (
        f"{email},First,Last,{income},{ts}Z UTC-06:00,15145550000,{group},"
        f"0,0,{taxes},{refund},Product,{source},{source},{status},-,-"
    )


def lead_line(
    email: str,
    join_date: str,
    *,
    income: str = "0",
    first_source: str = "@ad-alias",
    last_source_date: str = "-",
) -> str:
    return (
        f"{email},15145550000,First,Last,1.2.3.4,-,-,{income},{join_date},"
        f"{last_source_date},{first_source},{first_source},"
        f"https://x.test/,https://google.test/,UNSPECIFIED,Sent,-"
    )


def write_sales_csv(tmp_path, lines: list[str]) -> str:
    path = tmp_path / "sales.csv"
    path.write_text("\n".join([SALES_HEADER] + lines) + "\n", encoding="utf-8")
    return str(path)


def write_leads_csv(tmp_path, lines: list[str]) -> str:
    path = tmp_path / "leads.csv"
    path.write_text("\n".join([LEADS_HEADER] + lines) + "\n", encoding="utf-8")
    return str(path)


def test_multi_line_item_sale_group_aggregates_as_one_group(tmp_path):
    path = write_sales_csv(
        tmp_path,
        [
            sale_line("a@x.com", "100", "2026-07-05T10:00:00", "g1", taxes="10"),
            sale_line("a@x.com", "47.17", "2026-07-05T10:00:00", "g1", taxes="5"),
        ],
    )
    parsed = parse_sales_csv(path)
    assert parsed.quarantined == 0
    assert len(parsed.rows) == 2

    metrics = compute_sales_metrics(parsed.rows, *WINDOW)
    # Both line items belong to ONE sale group.
    assert metrics["sale_groups_total"] == 1
    assert metrics["sales"] == 1
    assert metrics["new_customers"] == 1
    assert metrics["revenue"] == 147.17
    # AOV = (income - refund + taxes) / distinct groups = (147.17 + 15) / 1.
    assert metrics["aov"] == 162.17
    assert metrics["net_cac_denominator"] == 1
    assert metrics["per_day"]["2026-07-05"] == {
        "revenue": 147.17,
        "sales": 1,
        "sale_groups": 1,
    }
    assert metrics["per_source"]["Ad Campaign A"]["sale_groups"] == 1
    assert metrics["per_source"]["Ad Campaign A"]["line_items"] == 2


def test_refunded_group_excluded_from_sales_but_counts_as_new_customer(tmp_path):
    path = write_sales_csv(
        tmp_path,
        [
            sale_line("a@x.com", "100", "2026-07-03T09:00:00", "g1"),
            sale_line("b@x.com", "100", "2026-07-04T09:00:00", "g2", refund="40"),
        ],
    )
    metrics = compute_sales_metrics(parse_sales_csv(path).rows, *WINDOW)
    # Refund > 0 drops g2 from `sales` only; new_customers ignores refunds.
    assert metrics["sales"] == 1
    assert metrics["new_customers"] == 2
    assert metrics["sale_groups_total"] == 2
    # Revenue nets the refund out.
    assert metrics["revenue"] == 160.0


def test_renewal_excluded_from_sales_and_new_customers(tmp_path):
    path = write_sales_csv(
        tmp_path,
        [
            # Same email + same group net amount 10 days before the window
            # purchase -> the window group is a renewal.
            sale_line("r@x.com", "99", "2026-06-25T12:00:00", "g_prior"),
            sale_line("r@x.com", "99", "2026-07-05T12:00:00", "g_window"),
            sale_line("new@x.com", "99", "2026-07-06T12:00:00", "g_new"),
        ],
    )
    metrics = compute_sales_metrics(parse_sales_csv(path).rows, *WINDOW)
    assert metrics["sale_groups_total"] == 2  # g_window + g_new
    # g_window is a renewal: excluded from BOTH sales and new_customers.
    assert metrics["new_customers"] == 1
    assert metrics["sales"] == 1
    # r@x.com's first-ever purchase (Jun 25) is outside the window.
    assert metrics["net_cac_denominator"] == 1
    # Renewals still count toward revenue and the AOV group denominator.
    assert metrics["revenue"] == 198.0
    assert metrics["aov"] == 99.0


def test_renewal_matches_charged_amount_even_when_prior_charge_was_refunded(tmp_path):
    path = write_sales_csv(
        tmp_path,
        [
            # Prior 99 charge fully refunded (net 0) — the CHARGED amount
            # still marks the later identical 99 charge as a renewal.
            sale_line("r@x.com", "99", "2026-06-10T12:00:00", "g_prior", refund="99"),
            sale_line("r@x.com", "99", "2026-07-05T12:00:00", "g_rebill"),
            # Prior group charged 745.17 + 199 together; a later lone 745.17
            # charge matches one of its line items -> renewal.
            sale_line("m@x.com", "745.17", "2026-06-14T19:00:00", "g_big"),
            sale_line("m@x.com", "199", "2026-06-14T19:00:00", "g_big"),
            sale_line("m@x.com", "745.17", "2026-07-01T10:00:00", "g_repeat"),
        ],
    )
    metrics = compute_sales_metrics(parse_sales_csv(path).rows, *WINDOW)
    assert metrics["sale_groups_total"] == 2  # g_rebill + g_repeat
    assert metrics["new_customers"] == 0
    assert metrics["sales"] == 0


def test_pending_line_item_excludes_group_from_sales_only(tmp_path):
    path = write_sales_csv(
        tmp_path,
        [
            sale_line("a@x.com", "100", "2026-07-03T09:00:00", "g1"),
            sale_line("p@x.com", "50", "2026-07-04T09:00:00", "g2", status="PENDING"),
        ],
    )
    metrics = compute_sales_metrics(parse_sales_csv(path).rows, *WINDOW)
    assert metrics["sales"] == 1
    assert metrics["new_customers"] == 2
    assert metrics["revenue"] == 150.0


def test_malformed_rows_are_quarantined_not_guessed(tmp_path):
    good = sale_line("a@x.com", "100", "2026-07-03T09:00:00", "g1")
    # Unquoted comma inside a name field -> 18 fields instead of 17.
    malformed = sale_line("b@x.com", "100", "2026-07-04T09:00:00", "g2").replace(
        "First,Last", "First,Sur,Name", 1
    )
    # Correct column count but an unparseable Date -> also quarantined.
    bad_date = sale_line("c@x.com", "100", "2026-07-05T09:00:00", "g3").replace(
        "2026-07-05T09:00:00Z UTC-06:00", "not-a-date", 1
    )
    path = write_sales_csv(tmp_path, [good, malformed, bad_date])
    parsed = parse_sales_csv(path)
    assert parsed.quarantined == 2
    assert len(parsed.rows) == 1
    assert parsed.rows[0].email == "a@x.com"
    assert parsed.histogram == {17: 2, 18: 1}
    assert parsed.total_rows == 3


def test_new_leads_compound_filter(tmp_path):
    path = write_leads_csv(
        tmp_path,
        [
            lead_line("ok@x.com", "2026-07-05 10:00:00"),
            # Already a paying customer (Income Generated > 0) -> excluded.
            lead_line("paid@x.com", "2026-07-06 10:00:00", income="396.66"),
            # No marketing attribution ('-' First source) -> excluded.
            lead_line("organic@x.com", "2026-07-07 10:00:00", first_source="-"),
            # Duplicate email of an already-counted new lead -> still distinct 1.
            lead_line("ok@x.com", "2026-07-08 11:00:00"),
            # Joined before the window but re-touched inside it: only the
            # union 'touches' candidate sees it.
            lead_line(
                "old@x.com",
                "2026-05-01 10:00:00",
                last_source_date="2026-07-09 10:00:00",
            ),
        ],
    )
    parsed = parse_leads_csv(path)
    assert parsed.quarantined == 0
    metrics = compute_leads_metrics(parsed.rows, *WINDOW)
    assert metrics["new_leads"] == 1
    assert metrics["leads_joined"] == 4  # all rows with Join Date in window
    assert metrics["leads_touches_candidate"] == 5  # union adds old@x.com
    assert metrics["per_day"]["2026-07-05"] == {"leads_joined": 1, "new_leads": 1}
    assert metrics["per_day"]["2026-07-06"] == {"leads_joined": 1, "new_leads": 0}


def test_revenue_window_filtering_and_utc06_date_parse(tmp_path):
    path = write_sales_csv(
        tmp_path,
        [
            # The 'Z UTC-06:00' suffix's clock reading IS the local wall time:
            # 23:59:59 on Jun 30 stays on Jun 30 (outside the window).
            sale_line("before@x.com", "10", "2026-06-30T23:59:59", "g0"),
            sale_line("first@x.com", "20", "2026-07-01T00:00:00", "g1"),
            sale_line("last@x.com", "30", "2026-07-15T23:59:59", "g2"),
            sale_line("after@x.com", "40", "2026-07-16T00:00:00", "g3"),
        ],
    )
    parsed = parse_sales_csv(path)
    # Parsed as a naive local datetime; the trailing offset is annotation only.
    assert parsed.rows[0].date == datetime(2026, 6, 30, 23, 59, 59)

    metrics = compute_sales_metrics(parsed.rows, *WINDOW)
    assert metrics["revenue"] == 50.0  # g1 + g2 only
    assert metrics["sale_groups_total"] == 2
    assert metrics["sales"] == 2
    assert sorted(metrics["per_day"]) == ["2026-07-01", "2026-07-15"]
