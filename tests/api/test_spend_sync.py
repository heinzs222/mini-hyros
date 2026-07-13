"""Tests for spend ingestion: CSV import (no network) and Meta API sync (respx-mocked)."""

from __future__ import annotations

import respx
from httpx import Response

from attributionops.db import sql_rows
from attributionops.tools.ads import ads_list_platforms

CSV = (
    "Day,Campaign,Campaign ID,Clicks,Impressions,Cost\n"
    "2026-01-10,Spring Sale,12345,100,5000,250.50\n"
    "2026-01-11,Spring Sale,12345,50,2000,125\n"
)


def test_import_csv_inserts_rows(client, api_db):
    r = client.post(
        "/api/spend/import_csv",
        json={"platform": "google", "account_id": "acc1", "csv_text": CSV, "replace": True},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["inserted"] == 2
    # Rows actually landed in the warehouse.
    assert "google" in ads_list_platforms(api_db)["platforms"]


def test_import_csv_skips_rows_without_date(client, api_db):
    csv_text = "Day,Campaign,Campaign ID,Clicks,Cost\n,No Date,999,10,5\n"
    body = client.post("/api/spend/import_csv", json={"platform": "google", "csv_text": csv_text}).json()
    assert body["inserted"] == 0
    assert body["skipped"] >= 1


def test_import_csv_requires_text(client):
    r = client.post("/api/spend/import_csv", json={"platform": "google", "csv_text": "   "})
    assert r.status_code == 400


def test_import_csv_rejects_unknown_platform(client):
    r = client.post("/api/spend/import_csv", json={"platform": "snapchat", "csv_text": "Day\n2026-01-01\n"})
    assert r.status_code == 400


def test_meta_spend_sync_with_mocked_api(client, api_db, monkeypatch):
    monkeypatch.setenv("META_ACCESS_TOKEN", "tok")
    monkeypatch.setenv("META_AD_ACCOUNT_ID", "act_123")

    meta_payload = {
        "data": [
            {
                "date_start": "2026-01-10",
                "account_id": "123",
                "campaign_id": "cmp1",
                "adset_id": "set1",
                "ad_id": "ad1",
                "clicks": "19",
                "inline_link_clicks": "10",
                "spend": "5.50",
                "impressions": "1000",
                "campaign_name": "C1",
                "adset_name": "S1",
                "ad_name": "A1",
            }
        ],
        "paging": {},
    }

    # assert_all_mocked=False lets the TestClient -> ASGI request pass through;
    # only the outbound Graph API call is intercepted.
    with respx.mock(assert_all_mocked=False) as router:
        graph = router.get(url__startswith="https://graph.facebook.com/").mock(
            return_value=Response(200, json=meta_payload)
        )
        r = client.post(
            "/api/spend/sync",
            params={"platform": "meta", "start_date": "2026-01-01", "end_date": "2026-01-31"},
        )

    assert r.status_code == 200
    assert graph.called
    body = r.json()
    # Meta now also fetches a campaign-level report for reconciliation; the mocked
    # ad-level report has exactly one row.
    assert body["platforms"]["meta"]["fetched_ads"] == 1
    assert body["synced"] >= 1
    # Campaign total equals the summed ad total, so no adjustment row is added.
    meta_rows = sql_rows(api_db, "SELECT * FROM spend WHERE platform = 'meta'")
    assert len(meta_rows) == 1
    assert meta_rows[0]["clicks"] == "10"
    assert '"click_metric":"inline_link_clicks"' in meta_rows[0]["metadata"]
    assert '"all_clicks":"19"' in meta_rows[0]["metadata"]
    # The mocked spend row is now queryable.
    assert "meta" in ads_list_platforms(api_db)["platforms"]


def test_meta_hyros_clicks_falls_back_for_legacy_payload():
    from api import spend_sync

    assert spend_sync._meta_hyros_clicks({"inline_link_clicks": "47", "clicks": "91"}) == 47
    assert spend_sync._meta_hyros_clicks({"clicks": "15"}) == 15


def test_meta_campaign_adjustments_are_disabled_for_hyros_parity(api_db, monkeypatch):
    from api import spend_sync

    monkeypatch.delenv("META_INCLUDE_CAMPAIGN_ADJUSTMENTS", raising=False)
    ad_rows = [{
        "date_start": "2026-07-04",
        "account_id": "123",
        "campaign_id": "cmp1",
        "adset_id": "set1",
        "ad_id": "ad1",
        "inline_link_clicks": "10",
        "spend": "5.50",
        "impressions": "1000",
    }]
    campaign_rows = [{
        "date_start": "2026-07-04",
        "account_id": "123",
        "campaign_id": "cmp1",
        "inline_link_clicks": "14",
        "spend": "8.00",
        "impressions": "1300",
    }]

    stats = spend_sync._write_meta_spend_rows(
        api_db, "123", "2026-07-04", "2026-07-04", ad_rows, campaign_rows
    )

    rows = sql_rows(api_db, "SELECT cost, clicks, impressions FROM spend WHERE platform='meta'")
    assert stats["inserted_campaign_adjustments"] == 0
    assert rows == [{"cost": "5.50", "clicks": "10", "impressions": "1000"}]


def test_meta_campaign_adjustments_remain_available_as_opt_in(api_db, monkeypatch):
    from api import spend_sync

    monkeypatch.setenv("META_INCLUDE_CAMPAIGN_ADJUSTMENTS", "true")
    ad_rows = [{
        "date_start": "2026-07-04",
        "account_id": "123",
        "campaign_id": "cmp1",
        "adset_id": "set1",
        "ad_id": "ad1",
        "inline_link_clicks": "10",
        "spend": "5.50",
        "impressions": "1000",
    }]
    campaign_rows = [{
        "date_start": "2026-07-04",
        "account_id": "123",
        "campaign_id": "cmp1",
        "inline_link_clicks": "14",
        "spend": "8.00",
        "impressions": "1300",
    }]

    stats = spend_sync._write_meta_spend_rows(
        api_db, "123", "2026-07-04", "2026-07-04", ad_rows, campaign_rows
    )

    rows = sql_rows(api_db, "SELECT SUM(CAST(cost AS REAL)) AS cost FROM spend WHERE platform='meta'")
    assert stats["inserted_campaign_adjustments"] == 1
    assert rows[0]["cost"] == 8.0


def test_meta_spend_sync_missing_credentials(client, api_db, monkeypatch):
    monkeypatch.delenv("META_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("META_AD_ACCOUNT_ID", raising=False)
    r = client.post("/api/spend/sync", params={"platform": "meta"})
    assert r.status_code == 200
    assert "META_ACCESS_TOKEN" in r.json()["platforms"]["meta"]["error"]
