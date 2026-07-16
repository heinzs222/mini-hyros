"""Consistency between /api/report parent rows and /api/report/children."""

from __future__ import annotations

from datetime import datetime

from attributionops.util import report_timezone
from tests.helpers import insert_rows, order, spend, touchpoint


def _today() -> str:
    return datetime.now(report_timezone()).date().isoformat()


def test_children_clamps_future_window(client, api_db):
    # Data exists today; the dashboard passes a future window through to the
    # drill-down. The parent report clamps to today, so the children endpoint
    # must query the same clamped window instead of the empty future one.
    insert_rows(api_db, "spend", [spend(date=_today(), platform="meta", campaign_id="cmp1",
                                        clicks=10, cost=25, impressions=1000)])

    r = client.get(
        "/api/report/children",
        params={
            "parent_tab": "traffic_source",
            "parent_id": "facebook / paid_social",
            "start_date": "2099-01-01",
            "end_date": "2099-12-31",
        },
    )
    assert r.status_code == 200
    rows = r.json()["rows"]
    assert rows, "future window should be clamped to today, not queried as-is"
    assert rows[0]["metrics"]["cost"] == 25.0


def test_children_rejects_range_inverted_by_clamp(client, api_db):
    r = client.get(
        "/api/report/children",
        params={
            "parent_tab": "traffic_source",
            "parent_id": "facebook / paid_social",
            "start_date": "2099-01-01",
            "end_date": "2020-01-01",
        },
    )
    assert r.status_code == 400


def test_children_resolve_display_name_touchpoints(client, api_db):
    # GHL-style touchpoints carry the campaign display name while spend carries
    # the numeric campaign id. The parent report canonicalizes its row ids via
    # resolve_attribution_dimensions, so the drill-down must do the same or the
    # expanded children lose their orders/revenue.
    insert_rows(api_db, "spend", [spend(
        date="2026-01-14", platform="google", account_id="5655721748",
        campaign_id="22681332420", adset_id="", ad_id="",
        clicks=50, cost=120, impressions=5000,
        metadata='{"campaign_name": "PMax QC Broad Prospecting"}',
    )])
    insert_rows(api_db, "touchpoints", [touchpoint(
        "2026-01-14T09:00:00Z", "c1", platform="google",
        campaign_id="PMax QC Broad Prospecting", adset_id="", ad_id="",
    )])
    insert_rows(api_db, "orders", [order("o1", "2026-01-15T12:00:00Z", "c1", gross=1000, net=800)])

    r = client.get(
        "/api/report/children",
        params={
            "parent_tab": "traffic_source",
            "parent_id": "google / cpc",
            "start_date": "2026-01-01",
            "end_date": "2026-01-31",
        },
    )
    assert r.status_code == 200
    rows = {row["id"]: row for row in r.json()["rows"]}
    canonical = rows.get("google|22681332420")
    assert canonical is not None, f"expected canonical campaign row, got {sorted(rows)}"
    assert canonical["metrics"]["orders"] == 1
    assert canonical["metrics"]["revenue"] == 800.0
    assert canonical["metrics"]["cost"] == 120.0


def test_alias_resolution_survives_multi_ad_campaigns(client, api_db):
    # The lifetime alias catalog must aggregate spend to the breakdown level.
    # A campaign whose lifetime spend spans several child ads is still ONE
    # campaign identity — ad-level granularity would make its name look like
    # multiple candidates and silently disable alias resolution everywhere.
    insert_rows(api_db, "spend", [
        spend(
            date="2026-01-10", platform="google", account_id="5655721748",
            campaign_id="22681332420", adset_id="grp1", ad_id="ad_a",
            clicks=30, cost=70, impressions=3000,
            metadata='{"campaign_name": "PMax QC Broad Prospecting"}',
        ),
        spend(
            date="2026-01-14", platform="google", account_id="5655721748",
            campaign_id="22681332420", adset_id="grp1", ad_id="ad_b",
            clicks=20, cost=50, impressions=2000,
            metadata='{"campaign_name": "PMax QC Broad Prospecting"}',
        ),
    ])
    insert_rows(api_db, "touchpoints", [touchpoint(
        "2026-01-14T09:00:00Z", "c1", platform="google",
        campaign_id="PMax QC Broad Prospecting", adset_id="", ad_id="",
    )])
    insert_rows(api_db, "orders", [order("o1", "2026-01-15T12:00:00Z", "c1", gross=1000, net=800)])

    r = client.get(
        "/api/report",
        params={
            "start_date": "2026-01-01",
            "end_date": "2026-01-31",
            "active_tab": "campaign",
        },
    )
    assert r.status_code == 200
    body = r.json()
    row_ids = [str(row.get("id", "")) for row in body["table"]["rows"]]
    assert any("22681332420" in rid for rid in row_ids), f"campaign alias failed to canonicalize: {row_ids}"
    coverage = body["table"]["coverage"]
    assert int(coverage.get("dimension_attributed_orders") or 0) >= 1
    assert int(coverage.get("unmapped_orders") or 0) == 0

    # The drill-down must agree with the parent's resolution.
    rc = client.get(
        "/api/report/children",
        params={
            "parent_tab": "campaign",
            "parent_id": "google|5655721748|22681332420",
            "start_date": "2026-01-01",
            "end_date": "2026-01-31",
        },
    )
    assert rc.status_code == 200
