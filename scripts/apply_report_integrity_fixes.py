#!/usr/bin/env python3
"""Idempotently apply report-integrity changes to attributionops/report.py."""

from __future__ import annotations

import sys
from pathlib import Path


def _replace_once(text: str, old: str, new: str, label: str) -> str:
    if new in text:
        return text
    if old not in text:
        raise RuntimeError(f"Cannot apply {label}: expected source text was not found")
    return text.replace(old, new, 1)


def apply_report_integrity_fixes(target: Path) -> bool:
    original = target.read_text(encoding="utf-8")
    text = original

    text = _replace_once(
        text,
        "from attributionops.tools.tracking import tracking_health_check\n",
        "from attributionops.tools.tracking import tracking_health_check\n"
        "from attributionops.report_integrity import (\n"
        "    build_dimension_coverage,\n"
        "    resolve_attribution_dimensions,\n"
        "    visible_report_columns,\n"
        ")\n",
        "report-integrity imports",
    )

    text = _replace_once(
        text,
        "    health = tracking_health_check(db_path)\n",
        "    health = tracking_health_check(\n"
        "        db_path,\n"
        "        start_date=inputs.start_date,\n"
        "        end_date=inputs.end_date,\n"
        "        lookback_days_for_order_source=inputs.lookback_days,\n"
        "    )\n",
        "date-scoped tracking health",
    )

    text = _replace_once(
        text,
        '''    attrib_rows = _attr["run"]["rows"]
    attrib_by_day = _attr["day_totals"]["rows"]
''',
        '''    attrib_rows = resolve_attribution_dimensions(
        spend_rows,
        list(_attr["run"]["rows"]),
        active_tab=inputs.active_tab,
    )
    dimension_coverage = build_dimension_coverage(
        attrib_rows,
        active_tab=inputs.active_tab,
    )
    attrib_by_day = _attr["day_totals"]["rows"]
''',
        "dimension resolution and coverage",
    )

    text = _replace_once(
        text,
        '''        "table": {
            "active_tab": inputs.active_tab,
            "columns": [
''',
        '''        "table": {
            "active_tab": inputs.active_tab,
            "coverage": dimension_coverage,
            "columns": visible_report_columns([
''',
        "table coverage and dynamic columns",
    )

    text = _replace_once(
        text,
        '''                {"key": "reported_delta", "label": "Ads Delta", "type": "money"},
            ],
            "rows": table_rows,
''',
        '''                {"key": "reported_delta", "label": "Ads Delta", "type": "money"},
            ], reported_rows=reported_rows),
            "rows": table_rows,
''',
        "reported-column visibility",
    )

    text = _replace_once(
        text,
        '''    if tracking_percentage < 85:
        diagnostics["anomalies"].append(
''',
        '''    if dimension_coverage.get("unmapped_orders", 0) > 0:
        diagnostics["anomalies"].append(
            {
                "what": (
                    f"{dimension_coverage['unmapped_orders']} source-attributed orders "
                    f"lack the selected {inputs.active_tab.replace('_', ' ')} identifier."
                ),
                "likely_cause": "Source/identity is known but the platform dimension ID was not captured or uniquely resolvable.",
                "verification_step": "Inspect GHL UTM/click-ID fields and campaign aliases; ambiguous names remain unmapped by design.",
            }
        )

    if tracking_percentage < 85:
        diagnostics["anomalies"].append(
''',
        "dimension mapping diagnostic",
    )

    if text == original:
        print(f"Already patched {target}")
        return False

    target.write_text(text, encoding="utf-8")
    print(f"Patched {target}")
    return True


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    target = (
        Path(sys.argv[1]).resolve()
        if len(sys.argv) > 1
        else project_root / "attributionops" / "report.py"
    )
    apply_report_integrity_fixes(target)


if __name__ == "__main__":
    main()
