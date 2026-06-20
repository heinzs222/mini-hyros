"""Tests for spend ingestion: CSV import (no network) and Meta API sync (respx-mocked)."""

from __future__ import annotations

import respx
from httpx import Response

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
                "clicks": "10",
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
    assert body["platforms"]["meta"]["fetched"] == 1
    assert body["synced"] >= 1
    # The mocked spend row is now queryable.
    assert "meta" in ads_list_platforms(api_db)["platforms"]


def test_meta_spend_sync_missing_credentials(client, api_db, monkeypatch):
    monkeypatch.delenv("META_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("META_AD_ACCOUNT_ID", raising=False)
    r = client.post("/api/spend/sync", params={"platform": "meta"})
    assert r.status_code == 200
    assert "META_ACCESS_TOKEN" in r.json()["platforms"]["meta"]["error"]
