"""Deterministic helpers that keep report dimensions honest.

Attribution touchpoints often arrive from GHL with a campaign display name while
spend arrives from the platform with a numeric campaign ID and account ID. This
module joins those records only when the match is unique. Ambiguous aliases stay
unmapped rather than being guessed, because a pretty dashboard is not worth fake
attribution.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable

from attributionops.util import normalize_campaign_key, to_float


_DIMENSION_FIELD = {
    "traffic_source": "",
    "ad_account": "account_id",
    "campaign": "campaign_id",
    "ad_set": "adset_id",
    "ad": "ad_id",
}


def _norm(value: Any) -> str:
    return normalize_campaign_key(value)


def _platform(value: Any) -> str:
    return str(value or "").strip().lower()


def _unique_alias_map(
    spend_rows: Iterable[dict[str, Any]],
    *,
    id_field: str,
) -> dict[tuple[str, str], dict[str, str]]:
    """Return only unambiguous (platform, alias) -> canonical spend identities."""

    candidates: dict[tuple[str, str], dict[tuple[str, ...], dict[str, str]]] = defaultdict(dict)
    for row in spend_rows:
        platform = _platform(row.get("platform"))
        canonical_id = str(row.get(id_field) or "").strip()
        if not platform or not canonical_id:
            continue

        canonical = {
            "platform": platform,
            "account_id": str(row.get("account_id") or "").strip(),
            "campaign_id": str(row.get("campaign_id") or "").strip(),
            "adset_id": str(row.get("adset_id") or "").strip(),
            "ad_id": str(row.get("ad_id") or "").strip(),
        }
        identity = tuple(canonical[key] for key in ("account_id", "campaign_id", "adset_id", "ad_id"))

        aliases = {
            _norm(canonical_id),
            _norm(row.get("name")),
        }
        # Spend metadata may have already supplied the current entity's display
        # name through ``name``. Blank aliases are never candidates.
        for alias in aliases:
            if alias:
                candidates[(platform, alias)][identity] = canonical

    out: dict[tuple[str, str], dict[str, str]] = {}
    for key, identities in candidates.items():
        if len(identities) == 1:
            out[key] = next(iter(identities.values()))
    return out


def resolve_attribution_dimensions(
    spend_rows: list[dict[str, Any]],
    attrib_rows: list[dict[str, Any]],
    *,
    active_tab: str,
    alias_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Canonicalize attribution rows against the selected spend breakdown.

    A match is accepted only when the platform plus normalized ID/display-name
    alias identifies exactly one spend entity. Existing non-empty aliases remain
    visible when no unique platform match exists, but are marked unmatched so the
    coverage diagnostic does not pretend they are joined to spend.

    ``alias_rows`` supplies the spend identities used to build the alias map —
    ideally the lifetime spend catalog, so an alias that is ambiguous across the
    account's history stays unmapped in every date window instead of resolving
    only when the window happens to contain a single candidate. When omitted,
    the in-window ``spend_rows`` are used.
    """

    field = _DIMENSION_FIELD.get(active_tab, "")
    if not field or not attrib_rows:
        return [dict(row) for row in attrib_rows]

    alias_map = _unique_alias_map(
        alias_rows if alias_rows is not None else spend_rows,
        id_field=field,
    )
    resolved: list[dict[str, Any]] = []
    for raw in attrib_rows:
        row = dict(raw)
        platform = _platform(row.get("platform"))
        alias = _norm(row.get(field))
        canonical = alias_map.get((platform, alias)) if platform and alias else None

        if canonical:
            row["platform"] = canonical["platform"] or row.get("platform", "")
            row["account_id"] = canonical["account_id"] or row.get("account_id", "")
            # Canonicalize the current level and every populated parent. Do not
            # manufacture children the attribution row never carried.
            if active_tab in {"campaign", "ad_set", "ad"}:
                row["campaign_id"] = canonical["campaign_id"] or row.get("campaign_id", "")
            if active_tab in {"ad_set", "ad"}:
                row["adset_id"] = canonical["adset_id"] or row.get("adset_id", "")
            if active_tab == "ad":
                row["ad_id"] = canonical["ad_id"] or row.get("ad_id", "")
            row["_dimension_resolution"] = "matched"
        elif alias:
            row["_dimension_resolution"] = "unmatched"
        else:
            row["_dimension_resolution"] = "missing"

        resolved.append(row)
    return resolved


def build_dimension_coverage(
    attrib_rows: list[dict[str, Any]],
    *,
    active_tab: str,
) -> dict[str, Any]:
    """Describe identifier capture and deterministic mapping for one dimension."""

    field = _DIMENSION_FIELD.get(active_tab, "")
    source_orders = sum(to_float(row.get("orders")) for row in attrib_rows)
    source_revenue = sum(to_float(row.get("revenue")) for row in attrib_rows)

    if active_tab == "traffic_source":
        identifier_rows = [
            row for row in attrib_rows
            if str(row.get("platform") or row.get("channel") or "").strip()
        ]
        mapped_rows = identifier_rows
    elif field:
        identifier_rows = [row for row in attrib_rows if str(row.get(field) or "").strip()]
        mapped_rows = [
            row for row in attrib_rows
            if row.get("_dimension_resolution") == "matched"
        ]
    else:
        identifier_rows = []
        mapped_rows = []

    identifier_orders = sum(to_float(row.get("orders")) for row in identifier_rows)
    identifier_revenue = sum(to_float(row.get("revenue")) for row in identifier_rows)
    mapped_orders = sum(to_float(row.get("orders")) for row in mapped_rows)
    mapped_revenue = sum(to_float(row.get("revenue")) for row in mapped_rows)
    unmapped_orders = max(source_orders - mapped_orders, 0.0)
    unmapped_revenue = max(source_revenue - mapped_revenue, 0.0)
    unmatched_identifier_orders = max(identifier_orders - mapped_orders, 0.0)
    missing_identifier_orders = max(source_orders - identifier_orders, 0.0)

    rate = (mapped_orders / source_orders * 100.0) if source_orders else None
    return {
        "dimension": active_tab,
        "source_attributed_orders": round(source_orders, 2),
        "dimension_identifier_orders": round(identifier_orders, 2),
        "dimension_attributed_orders": round(mapped_orders, 2),
        "unmatched_identifier_orders": round(unmatched_identifier_orders, 2),
        "missing_identifier_orders": round(missing_identifier_orders, 2),
        "unmapped_orders": round(unmapped_orders, 2),
        "source_attributed_revenue": round(source_revenue, 2),
        "dimension_identifier_revenue": round(identifier_revenue, 2),
        "dimension_attributed_revenue": round(mapped_revenue, 2),
        "unmapped_revenue": round(unmapped_revenue, 2),
        "dimension_attribution_rate": round(rate, 2) if rate is not None else None,
    }


def visible_report_columns(
    columns: list[dict[str, Any]],
    *,
    reported_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Hide platform-reported-value columns when no such feed exists."""

    if reported_rows:
        return columns
    hidden = {"reported", "reported_delta"}
    return [column for column in columns if str(column.get("key") or "") not in hidden]
