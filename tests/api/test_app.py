"""Tests for top-level app endpoints: health, root, report, drill-down, ad-names CRUD."""

from __future__ import annotations

from tests.helpers import insert_rows, order, spend, touchpoint


def _seed_basic(db: str) -> None:
    insert_rows(db, "spend", [spend(date="2026-01-14", platform="meta", account_id="acc_meta",
                                     campaign_id="cmp1", adset_id="set1", ad_id="ad1",
                                     clicks=100, cost=200)])
    insert_rows(db, "touchpoints", [touchpoint("2026-01-14T09:00:00Z", "c1", platform="meta",
                                               campaign_id="cmp1", adset_id="set1", ad_id="ad1")])
    insert_rows(db, "orders", [order("o1", "2026-01-15T12:00:00Z", "c1", gross=1000, net=800, cogs=100, fees=50)])


def test_health(client, api_db):
    r = client.get("/api/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["db"] == api_db
    # Health now also advertises the reporting timezone so the dashboard can
    # compute preset date ranges in the same zone the backend buckets days into.
    assert isinstance(body.get("timezone"), str) and body["timezone"]


def test_root_returns_service_banner(client):
    assert client.get("/").json() == {"status": "ok", "service": "mini-hyros"}


def test_report_endpoint_returns_structure(client, api_db):
    _seed_basic(api_db)
    r = client.get("/api/report", params={"start_date": "2026-01-01", "end_date": "2026-01-31",
                                          "active_tab": "campaign"})
    assert r.status_code == 200
    body = r.json()
    assert body["table"]["active_tab"] == "campaign"
    assert body["summary_totals"]["attributed_revenue"] == 800.0


def test_report_resolves_campaign_names_from_ad_names(client, api_db):
    _seed_basic(api_db)
    # Map the campaign id to a friendly name; the report should substitute it.
    client.post("/api/ad-names", json={"platform": "meta", "entity_type": "campaign",
                                       "entity_id": "cmp1", "name": "Spring Sale"})
    r = client.get("/api/report", params={"start_date": "2026-01-01", "end_date": "2026-01-31",
                                          "active_tab": "campaign"})
    names = [row["name"] for row in r.json()["table"]["rows"]]
    assert "Spring Sale" in names


def test_report_children_requires_params(client):
    # parent_tab and parent_id are required query params.
    assert client.get("/api/report/children").status_code == 422


def test_report_children_invalid_parent_tab(client, api_db):
    r = client.get("/api/report/children", params={"parent_tab": "bogus", "parent_id": "x"})
    assert r.status_code == 200
    assert r.json()["error"] == "Invalid parent_tab"


# ── ad-names CRUD ─────────────────────────────────────────────────────────────
def test_ad_names_crud_roundtrip(client, api_db):
    # Initially empty.
    assert client.get("/api/ad-names").json() == {"rows": [], "count": 0}

    # Create.
    up = client.post("/api/ad-names", json={"platform": "meta", "entity_type": "campaign",
                                            "entity_id": "cmp1", "name": "Camp 1"})
    assert up.json()["ok"] is True

    listed = client.get("/api/ad-names").json()
    assert listed["count"] == 1
    assert listed["rows"][0]["name"] == "Camp 1"

    # Filter by platform.
    assert client.get("/api/ad-names", params={"platform": "google"}).json()["count"] == 0

    # Delete.
    d = client.request("DELETE", "/api/ad-names", json={"platform": "meta",
                                                        "entity_type": "campaign", "entity_id": "cmp1"})
    assert d.json() == {"ok": True}
    assert client.get("/api/ad-names").json()["count"] == 0


def test_ad_names_batch_upsert(client, api_db):
    r = client.post("/api/ad-names/batch", json=[
        {"platform": "meta", "entity_type": "ad", "entity_id": "a1", "name": "Ad 1"},
        {"platform": "meta", "entity_type": "ad", "entity_id": "a2", "name": "Ad 2"},
    ])
    assert r.json() == {"ok": True, "count": 2}
    assert client.get("/api/ad-names", params={"entity_type": "ad"}).json()["count"] == 2
