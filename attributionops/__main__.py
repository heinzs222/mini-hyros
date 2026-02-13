from __future__ import annotations

import argparse
import json
import sys

from attributionops.config import default_db_path
from attributionops.report import ReportInputs, build_hyros_like_report
from attributionops.tools.ads import ads_get_reported_value, ads_get_spend, ads_list_platforms
from attributionops.tools.attribution import attribution_run
from attributionops.tools.audiences import audiences_sync
from attributionops.tools.conversions import conversions_push
from attributionops.tools.forecasting import forecasting_run
from attributionops.tools.integrations import integrations_status
from attributionops.tools.logs import logs_search
from attributionops.tools.tracking import tracking_health_check
from attributionops.tools.warehouse import warehouse_query


def _print(obj: object) -> None:
    print(json.dumps(obj, indent=2, sort_keys=True))


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="attributionops", description="Local AttributionOps (dummy) tools + HYROS-like report generator.")
    parser.add_argument("--db", type=str, default=default_db_path(), help="SQLite db path (default: data/dummy/attributionops_demo.sqlite)")

    sub = parser.add_subparsers(dest="cmd", required=True)

    tool = sub.add_parser("tool", help="Run a local tool (e.g., tracking.health_check).")
    tool.add_argument("name", type=str, help="Tool name, e.g. integrations.status, tracking.health_check, ads.get_spend")
    tool.add_argument("--start-date", type=str, default="")
    tool.add_argument("--end-date", type=str, default="")
    tool.add_argument("--platform", type=str, default="all")
    tool.add_argument("--breakdown", type=str, default="campaign")
    tool.add_argument("--conversion-type", type=str, default="Purchase")
    tool.add_argument("--model", type=str, default="last_click")
    tool.add_argument("--lookback-days", type=int, default=30)
    tool.add_argument("--value-type", type=str, default="revenue")
    tool.add_argument("--sql", type=str, default="")
    tool.add_argument("--query", type=str, default="")
    tool.add_argument("--cohort-window", type=int, default=30)
    tool.add_argument("--method", type=str, default="simple")
    tool.add_argument("--events-json", type=str, default="")
    tool.add_argument("--segment-json", type=str, default="")

    report = sub.add_parser("report", help="Generate a HYROS-like performance report JSON.")
    report.add_argument("--start-date", type=str, required=True)
    report.add_argument("--end-date", type=str, required=True)
    report.add_argument("--preset", type=str, default="Custom")
    report.add_argument("--currency", type=str, default="USD")
    report.add_argument("--model", type=str, default="last_click")
    report.add_argument("--lookback-days", type=int, default=30)
    report.add_argument("--conversion-type", type=str, default="Purchase")
    report.add_argument("--active-tab", type=str, default="traffic_source", choices=["traffic_source", "ad_account", "campaign", "ad_set", "ad"])
    report.add_argument("--use-date-of-click-attribution", action="store_true")
    report.add_argument("--report-name", type=str, default="HYROS-like Performance Report (Local Dummy)")
    report.add_argument("--out", type=str, default="", help="Optional output file path (written as UTF-8).")

    args = parser.parse_args(argv)
    db_path = args.db

    if args.cmd == "tool":
        name = args.name.strip()

        if name in ("integrations.status", "integrations_status"):
            _print(integrations_status(db_path))
            return 0
        if name in ("tracking.health_check", "tracking_health_check"):
            _print(tracking_health_check(db_path))
            return 0
        if name in ("ads.list_platforms", "ads_list_platforms"):
            _print(ads_list_platforms(db_path))
            return 0
        if name in ("ads.get_spend", "ads_get_spend"):
            if not args.start_date or not args.end_date:
                raise SystemExit("--start-date and --end-date are required for ads.get_spend")
            _print(
                ads_get_spend(
                    db_path,
                    platform=args.platform,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    breakdown=args.breakdown,
                )
            )
            return 0
        if name in ("ads.get_reported_value", "ads_get_reported_value"):
            if not args.start_date or not args.end_date:
                raise SystemExit("--start-date and --end-date are required for ads.get_reported_value")
            _print(
                ads_get_reported_value(
                    db_path,
                    platform=args.platform,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    breakdown=args.breakdown,
                    conversion_type=args.conversion_type,
                )
            )
            return 0
        if name in ("attribution.run", "attribution_run"):
            if not args.start_date or not args.end_date:
                raise SystemExit("--start-date and --end-date are required for attribution.run")
            _print(
                attribution_run(
                    db_path,
                    model=args.model,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    lookback_days=args.lookback_days,
                    conversion_type=args.conversion_type,
                    value_type=args.value_type,
                )
            )
            return 0
        if name in ("forecasting.run", "forecasting_run"):
            if not args.start_date or not args.end_date:
                raise SystemExit("--start-date and --end-date are required for forecasting.run")
            _print(
                forecasting_run(
                    db_path,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    cohort_window=args.cohort_window,
                    method=args.method,
                )
            )
            return 0
        if name in ("conversions.push", "conversions_push"):
            events = json.loads(args.events_json) if args.events_json else []
            _print(conversions_push(platform=args.platform, events=events))
            return 0
        if name in ("audiences.sync", "audiences_sync"):
            segment = json.loads(args.segment_json) if args.segment_json else {}
            _print(audiences_sync(platform=args.platform, segment_definition=segment))
            return 0
        if name in ("logs.search", "logs_search"):
            _print(logs_search(query=args.query, start_date=args.start_date, end_date=args.end_date))
            return 0
        if name in ("warehouse.query", "warehouse_query"):
            if not args.sql:
                raise SystemExit("--sql is required for warehouse.query")
            _print(warehouse_query(db_path, args.sql))
            return 0

        raise SystemExit(f"Unknown tool: {name}")

    if args.cmd == "report":
        report_inputs = ReportInputs(
            report_name=args.report_name,
            start_date=args.start_date,
            end_date=args.end_date,
            preset=args.preset,
            currency=args.currency,
            attribution_model=args.model,
            lookback_days=args.lookback_days,
            conversion_type=args.conversion_type,
            use_date_of_click_attribution=bool(args.use_date_of_click_attribution),
            active_tab=args.active_tab,
        )
        result = build_hyros_like_report(db_path, report_inputs)
        # JSON only (for UI rendering)
        payload = json.dumps(result, indent=2)
        if args.out:
            with open(args.out, "w", encoding="utf-8", newline="\n") as f:
                f.write(payload)
        print(payload)
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
