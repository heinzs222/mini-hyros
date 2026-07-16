#!/usr/bin/env python3
"""Reconcile Vigil's report metrics against HYROS ground-truth CSV exports.

Parses the HYROS sales/leads exports with the validated definitions in
attributionops/hyros_import.py, pulls Vigil's numbers for the same window
through the EXISTING report engine (build_hyros_like_report) or the live
/api/report endpoint, and prints a per-metric PASS/FAIL table at a
configurable tolerance (default +/-5%).

Usage:
    python3 scripts/reconcile_hyros.py \
        --sales-csv hyros_sales.csv --leads-csv hyros_leads.csv \
        --start 2026-07-01 --end 2026-07-15 \
        (--db path.sqlite | --api-base https://host [--api-token TOKEN]) \
        [--hyros-targets targets.json] [--tolerance 0.05] \
        [--by-day] [--by-source] [--out report.json]

Metrics with no CSV ground truth (cost, roas, cac, cpl, ltv, ...) can be
supplied via --hyros-targets, a JSON file of
    {"metric": {"value": N, "window": "YYYY-MM-DD..YYYY-MM-DD", "source": "..."}}
entries; those rows are annotated '(manual target)' in the output.

Exit codes: 0 = all PASS/SKIP, 1 = any FAIL, 2 = parse/API error.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from attributionops.hyros_import import (  # noqa: E402
    ParsedCsv,
    compute_leads_metrics,
    compute_sales_metrics,
    parse_leads_csv,
    parse_sales_csv,
)

# hyros metric -> Vigil summary_totals field names, in preference order.
# Later entries are graceful-degradation fallbacks for fields a parallel
# work-stream is still adding ('hyros_*', 'all_orders_aov', ...).
VIGIL_FIELD_MAP: dict[str, tuple[str, ...]] = {
    "revenue": ("all_orders_revenue",),
    "sales": ("hyros_sales_count", "all_orders_count"),
    "new_customers": ("hyros_new_customers", "new_customers"),
    "aov": ("all_orders_aov", "blended_aov"),
    "net_cac_denominator": ("net_new_customers",),
    "leads": ("leads",),
    "new_leads": ("new_leads_qualified", "new_leads"),
    "cac": ("cac",),
    "cpl": ("cpl",),
}

METRIC_ORDER = [
    "revenue",
    "sales",
    "new_customers",
    "aov",
    "net_cac_denominator",
    "leads",
    "new_leads",
    "leads_touches_candidate",
    "cac",
    "cpl",
]


class ReconcileError(RuntimeError):
    """Parse/API/DB failure — maps to exit code 2."""


def _print_quarantine(label: str, parsed: ParsedCsv) -> None:
    total = parsed.total_rows
    pct = (parsed.quarantined / total * 100.0) if total else 0.0
    histogram = {k: parsed.histogram[k] for k in sorted(parsed.histogram)}
    print(
        f"[parse] {label}: {total} data rows, {len(parsed.rows)} usable, "
        f"{parsed.quarantined} quarantined ({pct:.3f}%); "
        f"column-count histogram: {histogram}"
    )


def _fetch_vigil_from_db(db_path: str, start: str, end: str) -> dict[str, Any]:
    from attributionops.report import ReportInputs, build_hyros_like_report

    inputs = ReportInputs(
        report_name="HYROS reconcile",
        start_date=start,
        end_date=end,
        preset=None,
        currency="CAD",
        attribution_model="last_click",
        lookback_days=30,
        conversion_type="Purchase",
        use_date_of_click_attribution=False,
        active_tab="traffic_source",
    )
    return build_hyros_like_report(db_path, inputs)


def _fetch_vigil_from_api(
    api_base: str, api_token: str | None, start: str, end: str
) -> dict[str, Any]:
    params = urllib.parse.urlencode(
        {"start_date": start, "end_date": end, "no_cache": "true"}
    )
    url = f"{api_base.rstrip('/')}/api/report?{params}"
    request = urllib.request.Request(url)
    if api_token:
        request.add_header("Authorization", f"Bearer {api_token}")
    try:
        with urllib.request.urlopen(request, timeout=120) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, ValueError) as exc:
        raise ReconcileError(f"API request failed ({url}): {exc}") from exc


def _load_targets(path: str | None) -> dict[str, dict[str, Any]]:
    if not path:
        return {}
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError) as exc:
        raise ReconcileError(
            f"--hyros-targets must be a readable JSON file "
            f"({{metric: {{value, window, source}}}}): {exc}"
        ) from exc
    if not isinstance(data, dict):
        raise ReconcileError("--hyros-targets JSON must be an object keyed by metric")
    return data


def _vigil_value(
    summary: dict[str, Any], metric: str
) -> tuple[float | None, str | None]:
    """Resolve a metric to (value, summary_totals field used)."""
    # Unmapped metrics (e.g. manual-target 'cost'/'roas'/'ltv') fall back to a
    # same-named summary_totals field when one exists.
    for key in VIGIL_FIELD_MAP.get(metric, (metric,)):  # degrade gracefully
        if key in summary and summary.get(key) is not None:
            try:
                return float(summary[key]), key
            except (TypeError, ValueError):
                return None, key
    return None, None


def _fmt_num(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float) and not value.is_integer():
        return f"{value:,.2f}"
    return f"{int(value):,}"


def _build_results(
    hyros_values: dict[str, float | None],
    summary: dict[str, Any],
    targets: dict[str, dict[str, Any]],
    tolerance: float,
    window_label: str,
) -> list[dict[str, Any]]:
    metrics = list(METRIC_ORDER) + [m for m in targets if m not in METRIC_ORDER]
    results: list[dict[str, Any]] = []
    for metric in metrics:
        notes: list[str] = []
        hyros_val = hyros_values.get(metric)
        if metric in targets:
            target = targets[metric] if isinstance(targets[metric], dict) else {}
            target_val = target.get("value")
            if hyros_val is None and target_val is not None:
                hyros_val = float(target_val)
                notes.append("(manual target)")
                if target.get("source"):
                    notes.append(f"source={target['source']}")
                target_window = str(target.get("window") or "")
                if target_window and target_window != window_label:
                    notes.append(
                        f"WARNING target window {target_window} != {window_label}"
                    )
            elif target_val is not None:
                notes.append(f"manual target {target_val} ignored (CSV value present)")

        vigil_val, vigil_field = _vigil_value(summary, metric)
        if vigil_field:
            preferred = VIGIL_FIELD_MAP.get(metric, (vigil_field,))[0]
            if vigil_field != preferred:
                notes.append(f"vigil field: {vigil_field} (fallback; no {preferred})")
            else:
                notes.append(f"vigil field: {vigil_field}")

        if metric == "leads":
            notes.append("hyros side = rows with Join Date in window (approximation)")
        if metric == "leads_touches_candidate":
            notes.append("CPL-denominator candidate; no vigil equivalent")

        if hyros_val is None or vigil_val is None:
            status = "SKIP"
            delta_pct = None
            if hyros_val is None:
                notes.append("no hyros ground truth (use --hyros-targets)")
            if vigil_val is None:
                notes.append("no vigil value")
        elif hyros_val == 0:
            delta_pct = 0.0 if vigil_val == 0 else float("inf")
            status = "PASS" if vigil_val == 0 else "FAIL"
        else:
            delta_pct = (vigil_val - hyros_val) / abs(hyros_val) * 100.0
            status = "PASS" if abs(delta_pct) <= tolerance * 100.0 else "FAIL"

        results.append(
            {
                "metric": metric,
                "hyros": hyros_val,
                "vigil": vigil_val,
                "vigil_field": vigil_field,
                "delta_pct": (
                    round(delta_pct, 3)
                    if delta_pct is not None and delta_pct != float("inf")
                    else delta_pct
                ),
                "status": status,
                "note": "; ".join(notes),
            }
        )
    return results


def _print_table(results: list[dict[str, Any]]) -> None:
    header = ["metric", "hyros", "vigil", "delta%", "status", "note"]
    rows = []
    for r in results:
        if r["delta_pct"] is None:
            delta = "-"
        elif r["delta_pct"] == float("inf"):
            delta = "inf"
        else:
            delta = f"{r['delta_pct']:+.2f}%"
        rows.append(
            [r["metric"], _fmt_num(r["hyros"]), _fmt_num(r["vigil"]), delta,
             r["status"], r["note"]]
        )
    widths = [max(len(str(row[i])) for row in [header] + rows) for i in range(len(header))]
    def line(row: list[str]) -> str:
        cells = []
        for i, cell in enumerate(row):
            # right-align numeric columns, left-align text
            cells.append(str(cell).rjust(widths[i]) if 1 <= i <= 3 else str(cell).ljust(widths[i]))
        return " | ".join(cells).rstrip()

    print()
    print(line(header))
    print("-+-".join("-" * w for w in widths))
    for row in rows:
        print(line(row))


def _print_by_day(
    sales_metrics: dict[str, Any],
    leads_metrics: dict[str, Any],
    time_series: list[dict[str, Any]],
) -> None:
    vigil_by_day = {str(row.get("date")): row for row in time_series}
    days = sorted(
        set(sales_metrics["per_day"])
        | set(leads_metrics["per_day"])
        | set(vigil_by_day)
    )
    header = [
        "day", "hyros_revenue", "hyros_sales", "hyros_groups",
        "hyros_new_leads", "vigil_revenue", "vigil_sale_groups",
    ]
    rows = []
    for day in days:
        sale_day = sales_metrics["per_day"].get(day, {})
        lead_day = leads_metrics["per_day"].get(day, {})
        vigil_day = vigil_by_day.get(day, {})
        rows.append(
            [
                day,
                _fmt_num(sale_day.get("revenue")),
                _fmt_num(sale_day.get("sales")),
                _fmt_num(sale_day.get("sale_groups")),
                _fmt_num(lead_day.get("new_leads")),
                _fmt_num(vigil_day.get("tracked_revenue")),
                _fmt_num(vigil_day.get("tracked_sale_groups")),
            ]
        )
    widths = [max(len(str(row[i])) for row in [header] + rows) for i in range(len(header))]
    print()
    print("Per-day breakdown (hyros CSV vs vigil charts.time_series):")
    print(" | ".join(h.ljust(widths[i]) if i == 0 else h.rjust(widths[i]) for i, h in enumerate(header)))
    print("-+-".join("-" * w for w in widths))
    for row in rows:
        print(
            " | ".join(
                str(c).ljust(widths[i]) if i == 0 else str(c).rjust(widths[i])
                for i, c in enumerate(row)
            )
        )


def _print_by_source(sales_metrics: dict[str, Any], limit: int = 25) -> None:
    items = sorted(
        sales_metrics["per_source"].items(),
        key=lambda kv: kv[1]["revenue"],
        reverse=True,
    )
    header = ["origin_source", "sale_groups", "line_items", "revenue"]
    rows = []
    for source, v in items[:limit]:
        rows.append(
            [source or "-", str(v["sale_groups"]), str(v["line_items"]), _fmt_num(v["revenue"])]
        )
    if len(items) > limit:
        rest = items[limit:]
        rows.append(
            [
                f"({len(rest)} more sources)",
                str(sum(v["sale_groups"] for _, v in rest)),
                str(sum(v["line_items"] for _, v in rest)),
                _fmt_num(round(sum(v["revenue"] for _, v in rest), 2)),
            ]
        )
    widths = [max(len(str(row[i])) for row in [header] + rows) for i in range(len(header))]
    print()
    print("Per-source breakdown (hyros sales CSV, by Origin Source):")
    print(" | ".join(h.ljust(widths[i]) if i == 0 else h.rjust(widths[i]) for i, h in enumerate(header)))
    print("-+-".join("-" * w for w in widths))
    for row in rows:
        print(
            " | ".join(
                str(c).ljust(widths[i]) if i == 0 else str(c).rjust(widths[i])
                for i, c in enumerate(row)
            )
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Reconcile Vigil report metrics against HYROS CSV exports."
    )
    parser.add_argument("--sales-csv", required=True, help="HYROS sales export CSV")
    parser.add_argument("--leads-csv", required=True, help="HYROS leads export CSV")
    parser.add_argument("--start", required=True, help="Window start (YYYY-MM-DD, local)")
    parser.add_argument("--end", required=True, help="Window end (YYYY-MM-DD, local, inclusive)")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--db", help="Local Vigil sqlite warehouse path")
    source.add_argument("--api-base", help="Vigil API base URL (uses GET /api/report)")
    parser.add_argument("--api-token", help="Bearer token for --api-base mode")
    parser.add_argument(
        "--hyros-targets",
        help="JSON file of manual HYROS dashboard targets "
        "({metric: {value, window, source}}) for metrics with no CSV ground truth",
    )
    parser.add_argument("--tolerance", type=float, default=0.05,
                        help="PASS tolerance as a fraction (default 0.05 = +/-5%%)")
    parser.add_argument("--by-day", action="store_true", help="Print per-day breakdown")
    parser.add_argument("--by-source", action="store_true",
                        help="Print per-Origin-Source breakdown (hyros side)")
    parser.add_argument("--out", help="Write full JSON report to this path")
    args = parser.parse_args(argv)

    window_label = f"{args.start}..{args.end}"

    try:
        sales_parsed = parse_sales_csv(args.sales_csv)
        leads_parsed = parse_leads_csv(args.leads_csv)
    except OSError as exc:
        print(f"error: failed to read CSV: {exc}", file=sys.stderr)
        return 2

    _print_quarantine(args.sales_csv, sales_parsed)
    _print_quarantine(args.leads_csv, leads_parsed)

    try:
        targets = _load_targets(args.hyros_targets)
        sales_metrics = compute_sales_metrics(sales_parsed.rows, args.start, args.end)
        leads_metrics = compute_leads_metrics(leads_parsed.rows, args.start, args.end)
        if args.db:
            report = _fetch_vigil_from_db(args.db, args.start, args.end)
            vigil_source = f"db:{args.db}"
        else:
            report = _fetch_vigil_from_api(args.api_base, args.api_token, args.start, args.end)
            vigil_source = f"api:{args.api_base}"
    except ReconcileError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # DB/report build failures are also exit-2 errors
        print(f"error: failed to compute metrics or fetch vigil report: {exc}", file=sys.stderr)
        return 2

    summary = report.get("summary_totals") or {}
    if not summary:
        print("error: vigil report has no summary_totals", file=sys.stderr)
        return 2
    time_series = (report.get("charts") or {}).get("time_series") or []

    hyros_values: dict[str, float | None] = {
        "revenue": sales_metrics["revenue"],
        "sales": float(sales_metrics["sales"]),
        "new_customers": float(sales_metrics["new_customers"]),
        "aov": sales_metrics["aov"],
        "net_cac_denominator": float(sales_metrics["net_cac_denominator"]),
        "leads": float(leads_metrics["leads_joined"]),
        "new_leads": float(leads_metrics["new_leads"]),
        "leads_touches_candidate": float(leads_metrics["leads_touches_candidate"]),
        "cac": None,
        "cpl": None,
    }

    print(f"\nWindow {window_label} (local reporting days) | vigil source: {vigil_source}")
    print(
        f"[hyros] sale_groups_total={sales_metrics['sale_groups_total']} "
        f"sales={sales_metrics['sales']} new_customers={sales_metrics['new_customers']} "
        f"net_cac_denominator={sales_metrics['net_cac_denominator']} "
        f"revenue={sales_metrics['revenue']:,.2f} aov={sales_metrics['aov']} "
        f"new_leads={leads_metrics['new_leads']}"
    )

    results = _build_results(hyros_values, summary, targets, args.tolerance, window_label)
    _print_table(results)

    if args.by_day:
        _print_by_day(sales_metrics, leads_metrics, time_series)
    if args.by_source:
        _print_by_source(sales_metrics)

    failed = [r["metric"] for r in results if r["status"] == "FAIL"]
    skipped = [r["metric"] for r in results if r["status"] == "SKIP"]
    print(
        f"\n{sum(1 for r in results if r['status'] == 'PASS')} PASS, "
        f"{len(failed)} FAIL, {len(skipped)} SKIP "
        f"(tolerance +/-{args.tolerance * 100:.1f}%)"
    )
    if failed:
        print(f"FAILED metrics: {', '.join(failed)}")

    if args.out:
        payload = {
            "window": {"start": args.start, "end": args.end},
            "tolerance": args.tolerance,
            "vigil_source": vigil_source,
            "quarantine": {
                "sales_csv": {
                    "total_rows": sales_parsed.total_rows,
                    "quarantined": sales_parsed.quarantined,
                    "histogram": sales_parsed.histogram,
                },
                "leads_csv": {
                    "total_rows": leads_parsed.total_rows,
                    "quarantined": leads_parsed.quarantined,
                    "histogram": leads_parsed.histogram,
                },
            },
            "hyros": {"sales_metrics": sales_metrics, "leads_metrics": leads_metrics},
            "vigil": {"summary_totals": summary, "time_series": time_series},
            "results": results,
        }
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, default=str)
        print(f"Full JSON report written to {args.out}")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
