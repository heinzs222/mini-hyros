"""Tests for the uncovered Google + TikTok spend sync paths and the Google Ads
Script push endpoint in ``api.spend_sync``.

CSV import, Meta sync, and the numeric/date helpers are covered elsewhere
(``test_spend_sync.py`` / ``test_backend_helpers.py``); this module targets the
Google Ads API sync, TikTok Reporting API sync, the ``/google-ads-script`` push
endpoint, and the remaining pure helpers.

All outbound HTTP is mocked with respx using ``assert_all_mocked=False`` so the
TestClient -> ASGI request still passes through; only the upstream platform
calls are intercepted. Nothing ever hits the network.
"""

from __future__ import annotations

import sqlite3

import respx
from httpx import Response

import api.spend_sync as ss
from attributionops.tools.ads import ads_list_platforms
from tests.helpers import ad_name, insert_rows


# ── pure helpers ───────────────────────────────────────────────────────────────


def test_clean_google_customer_id_strips_prefix_and_dashes():
    assert ss._clean_google_customer_id("customers/123-456-7890") == "1234567890"
    assert ss._clean_google_customer_id("  123-456  ") == "123456"
    assert ss._clean_google_customer_id("9876543210") == "9876543210"


def test_deep_get_walks_nested_dicts_and_handles_missing():
    payload = {"campaign": {"id": "c1", "name": "Spring"}}
    assert ss._deep_get(payload, "campaign", "id") == "c1"
    assert ss._deep_get(payload, "campaign", "missing") is None
    # Walking into a non-dict short-circuits to None instead of raising.
    assert ss._deep_get(payload, "campaign", "id", "deeper") is None
    assert ss._deep_get(payload, "nope") is None


def test_first_present_returns_first_nonempty_path():
    payload = {"metrics": {"costMicros": "", "cost_micros": "5000000"}}
    assert ss._first_present(payload, ("metrics", "costMicros"), ("metrics", "cost_micros")) == "5000000"
    # All paths empty/missing -> empty string.
    assert ss._first_present(payload, ("metrics", "nope"), ("other", "x")) == ""


def test_tiktok_value_prefers_direct_then_dimensions_then_metrics():
    row = {
        "ad_id": "direct_ad",
        "dimensions": {"stat_time_day": "2026-01-10 00:00:00", "campaign_id": "cmp9"},
        "metrics": {"spend": "12.34", "clicks": "7"},
    }
    assert ss._tiktok_value(row, "ad_id") == "direct_ad"          # direct wins
    assert ss._tiktok_value(row, "campaign_id") == "cmp9"          # from dimensions
    assert ss._tiktok_value(row, "spend") == "12.34"               # from metrics
    assert ss._tiktok_value(row, "absent") == ""                   # nothing -> ""


# ── _write_google_spend_rows (direct) ──────────────────────────────────────────


def _google_result_row():
    """A single Google Ads searchStream/search result row (camelCase keys)."""
    return {
        "segments": {"date": "2026-01-10"},
        "campaign": {"id": "111", "name": "Spring Sale"},
        "adGroup": {"id": "222", "name": "Group A"},
        "adGroupAd": {"ad": {"id": "333", "name": "Ad Z"}},
        "metrics": {"clicks": "40", "impressions": "2000", "costMicros": "5500000"},
    }


def test_write_google_spend_rows_inserts_and_converts_micros(api_db):
    ss._ensure_spend_table(api_db)
    stats = ss._write_google_spend_rows(
        api_db, "123-456-7890", "2026-01-01", "2026-01-31", [_google_result_row()]
    )
    assert stats["inserted"] == 1

    with sqlite3.connect(api_db) as conn:
        row = conn.execute(
            "SELECT account_id, campaign_id, adset_id, ad_id, clicks, cost, impressions, metadata "
            "FROM spend WHERE platform='google'"
        ).fetchone()

    account_id, campaign_id, adset_id, ad_id, clicks, cost, impressions, metadata = row
    assert account_id == "1234567890"  # cleaned customer id
    assert campaign_id == "111"
    assert adset_id == "222"
    assert ad_id == "333"
    assert clicks == "40"
    assert impressions == "2000"
    assert cost == "5.5"  # 5_500_000 micros -> 5.5
    assert '"campaign_name":"Spring Sale"' in metadata
    assert '"ad_name":"Ad Z"' in metadata
    assert '"source":"google_ads_api"' in metadata


def test_write_google_spend_rows_skips_dateless_rows(api_db):
    ss._ensure_spend_table(api_db)
    stats = ss._write_google_spend_rows(
        api_db, "111", "2026-01-01", "2026-01-31", [{"campaign": {"id": "c"}, "metrics": {}}]
    )
    assert stats["inserted"] == 0


# ── _sync_google_spend (respx-mocked OAuth + GAQL) ─────────────────────────────


GOOGLE_CREDS = {
    "GOOGLE_ADS_CLIENT_ID": "cid",
    "GOOGLE_ADS_CLIENT_SECRET": "secret",
    "GOOGLE_ADS_REFRESH_TOKEN": "refresh",
    "GOOGLE_ADS_DEVELOPER_TOKEN": "devtok",
    "GOOGLE_ADS_CUSTOMER_ID": "123-456-7890",
}


def _set_google_creds(monkeypatch):
    for k, v in GOOGLE_CREDS.items():
        monkeypatch.setenv(k, v)
    # Make sure script/push mode is not forcing the API sync to skip.
    monkeypatch.delenv("GOOGLE_ADS_SYNC_MODE", raising=False)
    monkeypatch.delenv("DISABLE_GOOGLE_ADS_API_SYNC", raising=False)


def test_google_spend_sync_with_mocked_api(client, api_db, monkeypatch):
    _set_google_creds(monkeypatch)

    search_payload = {"results": [_google_result_row()]}

    with respx.mock(assert_all_mocked=False) as router:
        oauth = router.post("https://oauth2.googleapis.com/token").mock(
            return_value=Response(200, json={"access_token": "ya29.abc"})
        )
        gaql = router.post(url__startswith="https://googleads.googleapis.com/").mock(
            return_value=Response(200, json=search_payload)
        )
        r = client.post(
            "/api/spend/sync",
            params={"platform": "google", "start_date": "2026-01-01", "end_date": "2026-01-31"},
        )

    assert r.status_code == 200
    assert oauth.called
    assert gaql.called
    body = r.json()
    g = body["platforms"]["google"]
    assert g["fetched"] == 1
    assert g["synced"] == 1
    assert g["customer_id"] == "1234567890"
    assert body["synced"] >= 1
    # Spend row landed and is queryable.
    assert "google" in ads_list_platforms(api_db)["platforms"]


def test_google_spend_sync_paginates_with_next_page_token(client, api_db, monkeypatch):
    _set_google_creds(monkeypatch)

    page1 = {"results": [_google_result_row()], "nextPageToken": "PAGE2"}
    second = dict(_google_result_row())
    second["adGroupAd"] = {"ad": {"id": "444", "name": "Ad Y"}}
    page2 = {"results": [second]}

    with respx.mock(assert_all_mocked=False) as router:
        router.post("https://oauth2.googleapis.com/token").mock(
            return_value=Response(200, json={"access_token": "ya29.abc"})
        )
        router.post(url__startswith="https://googleads.googleapis.com/").mock(
            side_effect=[Response(200, json=page1), Response(200, json=page2)]
        )
        r = client.post(
            "/api/spend/sync",
            params={"platform": "google", "start_date": "2026-01-01", "end_date": "2026-01-31"},
        )

    body = r.json()["platforms"]["google"]
    assert body["fetched"] == 2
    assert body["synced"] == 2


def test_google_spend_sync_missing_credentials(client, api_db, monkeypatch):
    for k in GOOGLE_CREDS:
        monkeypatch.delenv(k, raising=False)
    monkeypatch.delenv("GOOGLE_ADS_SYNC_MODE", raising=False)
    monkeypatch.delenv("DISABLE_GOOGLE_ADS_API_SYNC", raising=False)

    r = client.post("/api/spend/sync", params={"platform": "google"})
    assert r.status_code == 200
    g = r.json()["platforms"]["google"]
    assert g["synced"] == 0
    assert "GOOGLE_ADS_CLIENT_ID" in g["error"]


def test_google_spend_sync_disabled_by_mode(client, api_db, monkeypatch):
    _set_google_creds(monkeypatch)
    monkeypatch.setenv("GOOGLE_ADS_SYNC_MODE", "script")

    # No HTTP should be attempted; the sync short-circuits as skipped.
    r = client.post("/api/spend/sync", params={"platform": "google"})
    assert r.status_code == 200
    g = r.json()["platforms"]["google"]
    assert g.get("skipped") is True
    assert "disabled" in g["reason"].lower()


def test_google_spend_sync_api_error_returns_error_dict(client, api_db, monkeypatch):
    _set_google_creds(monkeypatch)

    error_body = {
        "error": {
            "status": "PERMISSION_DENIED",
            "message": "The caller does not have permission",
            "details": [
                {"errors": [{"errorCode": {"authorizationError": "USER_PERMISSION_DENIED"},
                             "message": "User does not have permission."}]}
            ],
        }
    }

    with respx.mock(assert_all_mocked=False) as router:
        router.post("https://oauth2.googleapis.com/token").mock(
            return_value=Response(200, json={"access_token": "ya29.abc"})
        )
        router.post(url__startswith="https://googleads.googleapis.com/").mock(
            return_value=Response(403, json=error_body)
        )
        r = client.post("/api/spend/sync", params={"platform": "google"})

    g = r.json()["platforms"]["google"]
    assert g["synced"] == 0
    assert "Google Ads API error" in g["error"]
    assert "PERMISSION_DENIED" in g["error"]


# ── TikTok spend sync ──────────────────────────────────────────────────────────


def _tiktok_ad_payload():
    return {
        "code": 0,
        "data": {
            "list": [
                {
                    "dimensions": {"ad_id": "ad777", "stat_time_day": "2026-01-12 00:00:00"},
                    "metrics": {"spend": "33.30", "impressions": "900", "clicks": "12"},
                }
            ],
            "page_info": {"page": 1, "total_page": 1},
        },
    }


def _tiktok_empty_payload():
    return {"code": 0, "data": {"list": [], "page_info": {"page": 1, "total_page": 1}}}


def test_write_tiktok_spend_rows_enriches_parents_from_ad_names(api_db):
    ss._ensure_spend_table(api_db)
    ss._ensure_ad_names_table(api_db)
    # ad -> adset, adset -> campaign, so the ad row resolves both parents.
    insert_rows(
        api_db,
        "ad_names",
        [
            ad_name(platform="tiktok", entity_type="adset", entity_id="set5", name="Set 5", parent_id="cmp5"),
            ad_name(platform="tiktok", entity_type="ad", entity_id="ad777", name="Ad 777", parent_id="set5"),
        ],
    )

    rows = [
        {
            "dimensions": {"ad_id": "ad777", "stat_time_day": "2026-01-12 00:00:00"},
            "metrics": {"spend": "33.30", "impressions": "900", "clicks": "12"},
        }
    ]
    stats = ss._write_tiktok_spend_rows(api_db, "adv1", "2026-01-01", "2026-01-31", rows)
    assert stats["inserted"] == 1

    with sqlite3.connect(api_db) as conn:
        row = conn.execute(
            "SELECT account_id, campaign_id, adset_id, ad_id, clicks, cost, impressions "
            "FROM spend WHERE platform='tiktok'"
        ).fetchone()
    account_id, campaign_id, adset_id, ad_id, clicks, cost, impressions = row
    assert account_id == "adv1"
    assert ad_id == "ad777"
    assert adset_id == "set5"        # resolved from ad_names
    assert campaign_id == "cmp5"     # resolved via adset -> campaign chain
    assert clicks == "12"
    assert impressions == "900"
    assert cost == "33.3"


def test_build_tiktok_ad_parent_map(api_db):
    ss._ensure_ad_names_table(api_db)
    insert_rows(
        api_db,
        "ad_names",
        [
            ad_name(platform="tiktok", entity_type="adset", entity_id="setA", name="A", parent_id="cmpA"),
            ad_name(platform="tiktok", entity_type="ad", entity_id="adA", name="AdA", parent_id="setA"),
            ad_name(platform="tiktok", entity_type="ad", entity_id="adOrphan", name="Orphan", parent_id=""),
        ],
    )
    mapping = ss._build_tiktok_ad_parent_map(api_db)
    assert mapping["adA"] == {"adset_id": "setA", "campaign_id": "cmpA"}
    assert mapping["adOrphan"] == {"adset_id": "", "campaign_id": ""}


def test_tiktok_spend_sync_with_mocked_api(client, api_db, monkeypatch):
    monkeypatch.setenv("TIKTOK_ACCESS_TOKEN", "tt-token")
    monkeypatch.setenv("TIKTOK_ADVERTISER_ID", "adv1")

    with respx.mock(assert_all_mocked=False) as router:
        # The report endpoint is hit twice (AUCTION_AD, then AUCTION_CAMPAIGN).
        report = router.get(
            url__startswith="https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"
        ).mock(side_effect=[Response(200, json=_tiktok_ad_payload()),
                            Response(200, json=_tiktok_empty_payload())])
        r = client.post(
            "/api/spend/sync",
            params={"platform": "tiktok", "start_date": "2026-01-01", "end_date": "2026-01-31"},
        )

    assert r.status_code == 200
    assert report.called
    body = r.json()["platforms"]["tiktok"]
    assert body["fetched"] == 1
    assert body["synced"] == 1
    assert body["advertiser_id"] == "adv1"
    assert "tiktok" in ads_list_platforms(api_db)["platforms"]


def test_tiktok_spend_sync_falls_back_to_campaign_level(client, api_db, monkeypatch):
    monkeypatch.setenv("TIKTOK_ACCESS_TOKEN", "tt-token")
    monkeypatch.setenv("TIKTOK_ADVERTISER_ID", "adv1")

    campaign_payload = {
        "code": 0,
        "data": {
            "list": [
                {
                    "dimensions": {"campaign_id": "cmp42", "stat_time_day": "2026-01-13 00:00:00"},
                    "metrics": {"spend": "10", "impressions": "100", "clicks": "5"},
                }
            ],
            "page_info": {"page": 1, "total_page": 1},
        },
    }

    with respx.mock(assert_all_mocked=False) as router:
        # Ad level returns empty -> falls back to campaign level rows.
        router.get(
            url__startswith="https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"
        ).mock(side_effect=[Response(200, json=_tiktok_empty_payload()),
                            Response(200, json=campaign_payload)])
        r = client.post("/api/spend/sync", params={"platform": "tiktok"})

    body = r.json()["platforms"]["tiktok"]
    assert body["fetched"] == 1
    assert body["synced"] == 1

    with sqlite3.connect(api_db) as conn:
        campaign_id = conn.execute(
            "SELECT campaign_id FROM spend WHERE platform='tiktok'"
        ).fetchone()[0]
    assert campaign_id == "cmp42"


def test_tiktok_spend_sync_not_connected(client, api_db, monkeypatch):
    monkeypatch.delenv("TIKTOK_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("TIKTOK_ADVERTISER_ID", raising=False)

    r = client.post("/api/spend/sync", params={"platform": "tiktok"})
    assert r.status_code == 200
    body = r.json()["platforms"]["tiktok"]
    assert body["synced"] == 0
    assert "TikTok not connected" in body["error"]


def test_tiktok_spend_sync_api_error_code(client, api_db, monkeypatch):
    monkeypatch.setenv("TIKTOK_ACCESS_TOKEN", "tt-token")
    monkeypatch.setenv("TIKTOK_ADVERTISER_ID", "adv1")

    err_payload = {"code": 40001, "message": "Invalid advertiser_id", "request_id": "req-1"}

    with respx.mock(assert_all_mocked=False) as router:
        router.get(
            url__startswith="https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"
        ).mock(return_value=Response(200, json=err_payload))
        r = client.post("/api/spend/sync", params={"platform": "tiktok"})

    body = r.json()["platforms"]["tiktok"]
    assert body["synced"] == 0
    assert "TikTok API error" in body["error"]


# ── /api/spend/google-ads-script push endpoint ─────────────────────────────────


def _script_payload(**overrides):
    base = {
        "account_id": "123-456-7890",
        "start_date": "2026-01-01",
        "end_date": "2026-01-31",
        "replace": True,
        "rows": [
            {
                "date": "2026-01-10",
                "campaign_id": "c1",
                "campaign_name": "Spring",
                "adset_id": "g1",
                "adset_name": "Group 1",
                "ad_id": "a1",
                "ad_name": "Ad 1",
                "clicks": 10,
                "impressions": 500,
                "cost": 42.5,
            }
        ],
    }
    base.update(overrides)
    return base


def test_google_ads_script_push_unauthorized(client, api_db, monkeypatch):
    monkeypatch.setenv("SITE_TOKEN", "secret-token")
    monkeypatch.delenv("GOOGLE_ADS_SCRIPT_TOKEN", raising=False)
    monkeypatch.delenv("SPEND_WEBHOOK_TOKEN", raising=False)

    # No token provided -> 401.
    r = client.post("/api/spend/google-ads-script", json=_script_payload())
    assert r.status_code == 401
    assert "Invalid spend push token" in r.json()["detail"]


def test_google_ads_script_push_no_tokens_configured(client, api_db, monkeypatch):
    for k in ("GOOGLE_ADS_SCRIPT_TOKEN", "SPEND_WEBHOOK_TOKEN", "SITE_TOKEN"):
        monkeypatch.delenv(k, raising=False)

    r = client.post(
        "/api/spend/google-ads-script",
        json=_script_payload(token="anything"),
    )
    assert r.status_code == 500
    assert "GOOGLE_ADS_SCRIPT_TOKEN" in r.json()["detail"]


def test_google_ads_script_push_authorized_via_header(client, api_db, monkeypatch):
    monkeypatch.setenv("GOOGLE_ADS_SCRIPT_TOKEN", "tok-abc")

    r = client.post(
        "/api/spend/google-ads-script",
        json=_script_payload(),
        headers={"x-mini-hyros-token": "tok-abc"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True
    assert body["inserted"] == 1
    assert body["source"] == "google_ads_script"
    assert body["names_upserted"] == 3  # campaign + adset + ad names

    with sqlite3.connect(api_db) as conn:
        row = conn.execute(
            "SELECT account_id, campaign_id, adset_id, ad_id, clicks, cost, impressions "
            "FROM spend WHERE platform='google'"
        ).fetchone()
    account_id, campaign_id, adset_id, ad_id, clicks, cost, impressions = row
    assert account_id == "1234567890"  # cleaned
    assert campaign_id == "c1"
    assert adset_id == "g1"
    assert ad_id == "a1"
    assert clicks == "10"
    assert cost == "42.5"
    assert impressions == "500"
    assert "google" in ads_list_platforms(api_db)["platforms"]


def test_google_ads_script_push_authorized_via_query_token(client, api_db, monkeypatch):
    monkeypatch.setenv("SITE_TOKEN", "qtok")

    r = client.post(
        "/api/spend/google-ads-script?token=qtok",
        json=_script_payload(),
    )
    assert r.status_code == 200
    assert r.json()["inserted"] == 1


def test_google_ads_script_push_authorized_via_payload_token(client, api_db, monkeypatch):
    monkeypatch.setenv("GOOGLE_ADS_SCRIPT_TOKEN", "ptok")

    r = client.post(
        "/api/spend/google-ads-script",
        json=_script_payload(token="ptok"),
    )
    assert r.status_code == 200
    assert r.json()["inserted"] == 1


def test_google_ads_script_push_no_usable_rows(client, api_db, monkeypatch):
    monkeypatch.setenv("GOOGLE_ADS_SCRIPT_TOKEN", "tok")

    # Row has a date but no campaign/adset/ad id -> skipped, nothing inserted.
    payload = _script_payload(
        rows=[{"date": "2026-01-10", "cost": 5.0}]
    )
    r = client.post(
        "/api/spend/google-ads-script",
        json=payload,
        headers={"x-mini-hyros-token": "tok"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["inserted"] == 0
    assert body["skipped"] >= 1
    assert body["warnings"]
