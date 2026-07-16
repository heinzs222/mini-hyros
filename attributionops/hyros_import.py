"""HYROS CSV export parsing + HYROS-definition metric computation.

This module is the ground-truth side of the HYROS parity reconciliation tool
(scripts/reconcile_hyros.py). It parses the two CSV exports HYROS produces
("sales" line-item export and "leads" contact export) and computes window
metrics using definitions that were reverse-engineered and validated to
0-error (or the noted sub-0.2% residual) against the HYROS dashboard:

- revenue(window)   = sum(Income - Refund) over ALL line items whose Date
                      falls in the window (local UTC-06 days). [0.06% residual,
                      fully explained by export-snapshot timing]
- renewal           : a sale group is a RENEWAL if any of its line items'
                      (lowercased email, round(charged Income, 2)) pair also
                      occurred as a prior line-item CHARGE in the trailing 30
                      days before the group's earliest timestamp (full-history
                      comparison). The charged amount is gross of refunds:
                      a refunded prior charge (Income=99, Refund=99) still
                      marks a later identical 99 charge as a renewal, and a
                      charge matching one line item of a larger prior group
                      still counts. (A stricter group-net-vs-prior-groups
                      variant undercounts renewals on the reference export:
                      241/216 instead of the dashboard-exact 236/214.)
- new_customers     = count of non-renewal sale groups in window
                      (refund/status irrelevant). [validated 236/236]
- sales             = count of non-renewal sale groups with total refund == 0
                      AND no line item with Status == 'PENDING'.
                      [validated 214/214]
- aov               = sum(Income - Refund + Taxes) / count(ALL distinct sale
                      groups in window). [0.17% residual]
- net_cac_denominator = distinct emails whose FIRST-EVER purchase (across the
                      full export history) falls in the window.
                      [validated 178 exactly]
- new_leads         = distinct lead emails with Join Date in window AND
                      Income Generated == 0 AND First source not in {'', '-'}.
                      [validated 716/716]
- leads_joined      = count of lead rows with Join Date in window (all) — a
                      CPL-denominator approximation, NOT a validated match.
- leads_touches_candidate = count of lead rows with Join Date in window OR
                      Last Source Date in window (union) — the closest CSV
                      reconstruction of HYROS's touch-based CPL denominator.

Timestamp semantics (validated against the reference exports):
- sales 'Date' looks like '2026-07-05T10:00:00Z UTC-06:00'. The clock reading
  IS the UTC-06 local wall time — parse the leading timestamp as a naive local
  datetime and ignore the trailing offset annotation. Rows whose Date does not
  match this shape are quarantined, never guessed.
- leads 'Join Date' / 'Last Source Date' are naive 'YYYY-MM-DD HH:MM:SS'
  strings with no offset; they are treated as already-local by default.

Parsing uses csv.reader (never a naive split): a column-count histogram is
built first and any row whose field count differs from the header length is
quarantined and counted (2/14887 sales rows and ~131/58372 leads rows on the
reference exports).

Pure stdlib; no I/O beyond reading the given path.
"""

from __future__ import annotations

import csv
import re
from bisect import bisect_left
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

# Expected headers, for documentation and sanity checking. Field mapping is
# positional (index-based) against these layouts; the leads header really does
# carry leading spaces on ' Tracked URL' / ' Previous URL' in the raw export.
SALES_HEADER = [
    "Email", "First Name", "Last Name", "Income", "Date", "Phones",
    "Sale Group", "Cost of goods", "Shipping Value", "Taxes", "Refund",
    "Name", "Origin Source", "Last Source", "Status", "Info", "Order Name",
]
LEADS_HEADER = [
    "Email", "Phone Numbers", "First name", "Last name", "IPs", "Stage",
    "Parent Email", "Income Generated", "Join Date", "Last Source Date",
    "First source", "Last source", "Tracked URL", "Previous URL",
    "Ad Optimization Consent", "Status", "Error message",
]

# '2026-07-05T10:00:00Z UTC-06:00' — the clock reading is the local wall time.
_SALES_DATE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})Z UTC[+-]\d{2}:\d{2}$"
)
_RENEWAL_LOOKBACK = timedelta(days=30)


@dataclass
class SaleLineItem:
    email: str  # lowercased/stripped
    first_name: str
    last_name: str
    income: float
    date: datetime  # naive local (UTC-06) wall time
    phones: str
    sale_group: str
    cost_of_goods: float
    shipping_value: float
    taxes: float
    refund: float
    name: str
    origin_source: str
    last_source: str
    status: str
    info: str
    order_name: str


@dataclass
class LeadRow:
    email: str  # lowercased/stripped
    phone_numbers: str
    first_name: str
    last_name: str
    ips: str
    stage: str
    parent_email: str
    income_generated: float
    join_date: datetime | None  # naive, treated as already-local
    last_source_date: datetime | None
    first_source: str
    last_source: str
    tracked_url: str
    previous_url: str
    ad_optimization_consent: str
    status: str
    error_message: str


@dataclass
class ParsedCsv:
    """Parse result: clean typed rows + quarantine accounting."""

    rows: list[Any]
    quarantined: int
    histogram: dict[int, int] = field(default_factory=dict)

    @property
    def total_rows(self) -> int:
        """Total data rows seen in the file (clean + quarantined)."""
        return sum(self.histogram.values())


def _to_float(raw: str) -> float:
    text = (raw or "").strip()
    if text in ("", "-"):
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _parse_naive_dt(raw: str) -> datetime | None:
    text = (raw or "").strip()
    if text in ("", "-"):
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def _load_csv_rows(path: str) -> tuple[list[str], list[list[str]], Counter, int]:
    """csv.reader pass over `path`.

    Builds the column-count histogram over every data row first, quarantining
    (dropping + counting) any row whose field count != the header length.
    Returns (header, clean_rows, histogram, quarantined_count).
    """
    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            return [], [], Counter(), 0
        expected = len(header)
        histogram: Counter = Counter()
        clean: list[list[str]] = []
        quarantined = 0
        for row in reader:
            histogram[len(row)] += 1
            if len(row) != expected:
                quarantined += 1
                continue
            clean.append(row)
    return header, clean, histogram, quarantined


def parse_sales_csv(path: str) -> ParsedCsv:
    """Parse a HYROS sales (line-item) export into SaleLineItem rows.

    Rows with a malformed column count or an unparseable Date are quarantined.
    """
    _header, raw_rows, histogram, quarantined = _load_csv_rows(path)
    rows: list[SaleLineItem] = []
    for r in raw_rows:
        match = _SALES_DATE_RE.match(r[4].strip())
        if not match:
            quarantined += 1
            continue
        rows.append(
            SaleLineItem(
                email=r[0].strip().lower(),
                first_name=r[1].strip(),
                last_name=r[2].strip(),
                income=_to_float(r[3]),
                date=datetime.strptime(match.group("ts"), "%Y-%m-%dT%H:%M:%S"),
                phones=r[5].strip(),
                sale_group=r[6].strip(),
                cost_of_goods=_to_float(r[7]),
                shipping_value=_to_float(r[8]),
                taxes=_to_float(r[9]),
                refund=_to_float(r[10]),
                name=r[11].strip(),
                origin_source=r[12].strip(),
                last_source=r[13].strip(),
                status=r[14].strip(),
                info=r[15].strip(),
                order_name=r[16].strip(),
            )
        )
    return ParsedCsv(rows=rows, quarantined=quarantined, histogram=dict(histogram))


def parse_leads_csv(path: str) -> ParsedCsv:
    """Parse a HYROS leads (contact) export into LeadRow rows."""
    _header, raw_rows, histogram, quarantined = _load_csv_rows(path)
    rows: list[LeadRow] = []
    for r in raw_rows:
        rows.append(
            LeadRow(
                email=r[0].strip().lower(),
                phone_numbers=r[1].strip(),
                first_name=r[2].strip(),
                last_name=r[3].strip(),
                ips=r[4].strip(),
                stage=r[5].strip(),
                parent_email=r[6].strip(),
                income_generated=_to_float(r[7]),
                join_date=_parse_naive_dt(r[8]),
                last_source_date=_parse_naive_dt(r[9]),
                first_source=r[10].strip(),
                last_source=r[11].strip(),
                tracked_url=r[12].strip(),
                previous_url=r[13].strip(),
                ad_optimization_consent=r[14].strip(),
                status=r[15].strip(),
                error_message=r[16].strip(),
            )
        )
    return ParsedCsv(rows=rows, quarantined=quarantined, histogram=dict(histogram))


def _window_bounds(start_iso: str, end_iso: str) -> tuple[datetime, datetime]:
    """[start 00:00, end+1d 00:00) as naive local datetimes; end is inclusive."""
    start = datetime.fromisoformat(start_iso)
    end_excl = datetime.fromisoformat(end_iso) + timedelta(days=1)
    return start, end_excl


def compute_sales_metrics(
    rows: list[SaleLineItem], start_iso: str, end_iso: str
) -> dict[str, Any]:
    """Compute HYROS-definition sales metrics for the [start, end] local window.

    `rows` must be the FULL parsed history (not pre-filtered to the window):
    renewal detection and the net-CAC denominator both depend on out-of-window
    history.
    """
    start, end_excl = _window_bounds(start_iso, end_iso)

    # --- group line items into sale groups over the FULL history ---
    groups: dict[str, dict[str, Any]] = {}
    first_by_email: dict[str, datetime] = {}
    # (email, round(charged Income, 2)) -> sorted line-item timestamps, used by
    # renewal detection. Keyed on the CHARGED amount (gross of refunds): a
    # refunded prior charge still marks a later identical charge as a renewal.
    charge_ts_by_key: dict[tuple[str, float], list[datetime]] = defaultdict(list)
    for li in rows:
        g = groups.get(li.sale_group)
        if g is None:
            g = groups[li.sale_group] = {
                "email": li.email,
                "first_ts": li.date,
                "net": 0.0,
                "tax": 0.0,
                "refund": 0.0,
                "pending": False,
                "charge_keys": set(),
            }
        g["net"] += li.income - li.refund
        g["tax"] += li.taxes
        g["refund"] += li.refund
        g["charge_keys"].add((li.email, round(li.income, 2)))
        if li.date < g["first_ts"]:
            g["first_ts"] = li.date
        if li.status.upper() == "PENDING":
            g["pending"] = True

        charge_ts_by_key[(li.email, round(li.income, 2))].append(li.date)
        prev = first_by_email.get(li.email)
        if prev is None or li.date < prev:
            first_by_email[li.email] = li.date

    for ts_list in charge_ts_by_key.values():
        ts_list.sort()

    def _is_renewal(g: dict[str, Any]) -> bool:
        """True iff any of the group's charges repeats a prior charge in the
        trailing 30 days before the group's earliest timestamp.

        Same-group line items never self-match: only strictly-earlier
        timestamps qualify, and a group's line items are never earlier than
        its own first_ts.
        """
        for key in g["charge_keys"]:
            ts_list = charge_ts_by_key[key]
            i = bisect_left(ts_list, g["first_ts"])
            # ts_list[i-1] is the latest strictly-earlier occurrence.
            if i > 0 and ts_list[i - 1] >= g["first_ts"] - _RENEWAL_LOOKBACK:
                return True
        return False

    # --- line-item level window aggregates (revenue / AOV numerator) ---
    revenue = 0.0
    aov_numerator = 0.0
    per_day: dict[str, dict[str, Any]] = {}
    per_source: dict[str, dict[str, Any]] = {}
    for li in rows:
        if not (start <= li.date < end_excl):
            continue
        net = li.income - li.refund
        revenue += net
        aov_numerator += net + li.taxes
        day = li.date.date().isoformat()
        d = per_day.setdefault(day, {"revenue": 0.0, "sales": 0, "sale_groups": 0})
        d["revenue"] += net
        source = li.origin_source or "-"
        s = per_source.setdefault(
            source, {"line_items": 0, "sale_groups": set(), "revenue": 0.0}
        )
        s["line_items"] += 1
        s["sale_groups"].add(li.sale_group)
        s["revenue"] += net

    # --- sale-group level window aggregates ---
    sale_groups_total = 0
    new_customers = 0
    sales = 0
    for g in groups.values():
        if not (start <= g["first_ts"] < end_excl):
            continue
        sale_groups_total += 1
        day = g["first_ts"].date().isoformat()
        d = per_day.setdefault(day, {"revenue": 0.0, "sales": 0, "sale_groups": 0})
        d["sale_groups"] += 1
        if _is_renewal(g):
            continue
        new_customers += 1
        if round(g["refund"], 2) <= 0 and not g["pending"]:
            sales += 1
            d["sales"] += 1

    net_cac_denominator = sum(
        1 for ts in first_by_email.values() if start <= ts < end_excl
    )
    aov = (aov_numerator / sale_groups_total) if sale_groups_total else None

    return {
        "revenue": round(revenue, 2),
        "sales": sales,
        "new_customers": new_customers,
        "aov": round(aov, 2) if aov is not None else None,
        "net_cac_denominator": net_cac_denominator,
        "sale_groups_total": sale_groups_total,
        "per_day": {
            day: {
                "revenue": round(v["revenue"], 2),
                "sales": v["sales"],
                "sale_groups": v["sale_groups"],
            }
            for day, v in sorted(per_day.items())
        },
        "per_source": {
            source: {
                "line_items": v["line_items"],
                "sale_groups": len(v["sale_groups"]),
                "revenue": round(v["revenue"], 2),
            }
            for source, v in per_source.items()
        },
    }


def compute_leads_metrics(
    rows: list[LeadRow], start_iso: str, end_iso: str
) -> dict[str, Any]:
    """Compute HYROS-definition lead metrics for the [start, end] local window."""
    start, end_excl = _window_bounds(start_iso, end_iso)

    new_lead_emails: set[str] = set()
    leads_joined = 0
    touches_union_rows = 0
    per_day: dict[str, dict[str, int]] = {}

    for lr in rows:
        joined_in = lr.join_date is not None and start <= lr.join_date < end_excl
        touched_in = (
            lr.last_source_date is not None
            and start <= lr.last_source_date < end_excl
        )
        if joined_in:
            leads_joined += 1
            day = lr.join_date.date().isoformat()
            d = per_day.setdefault(day, {"leads_joined": 0, "new_leads": 0})
            d["leads_joined"] += 1
            if (
                abs(lr.income_generated) < 1e-9
                and lr.first_source not in ("", "-")
                and lr.email not in new_lead_emails
            ):
                new_lead_emails.add(lr.email)
                d["new_leads"] += 1
        if joined_in or touched_in:
            touches_union_rows += 1

    return {
        "new_leads": len(new_lead_emails),
        "leads_joined": leads_joined,
        "leads_touches_candidate": touches_union_rows,
        "per_day": {day: v for day, v in sorted(per_day.items())},
    }
