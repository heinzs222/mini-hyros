"""Tests for the UNCOVERED parts of backend/api/ad_names.py:

  * the platform sync flows (``_sync_meta`` / ``_sync_google`` / ``_sync_tiktok``)
    and the ``POST /api/ad-names/sync`` dispatch endpoint,
  * ``get_thumbnails_map``,
  * the GET helper endpoints ``/resolve``, ``/video-url`` and ``/thumbnail``.

All outbound HTTP is mocked with respx (``assert_all_mocked=False``); nothing
hits the network. Credentials are injected via ``monkeypatch.setenv``. The CRUD
endpoints and ``get_name_map`` / ``_meta_api_version`` are covered elsewhere and
are deliberately not retested here.
"""

from __future__ import annotations

import sqlite3

import httpx
import respx

import api.ad_names as ad_names
from tests.helpers import ad_name, insert_rows


# ── helpers ───────────────────────────────────────────────────────────────────

def _rows(db_path: str, where: str = "") -> list[dict]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        sql = "SELECT * FROM ad_names"
        if where:
            sql += f" WHERE {where}"
        return [dict(r) for r in conn.execute(sql).fetchall()]


def _clear_meta_env(monkeypatch) -> None:
    for var in ("META_ACCESS_TOKEN", "META_AD_ACCOUNT_ID", "META_API_VERSION"):
        monkeypatch.delenv(var, raising=False)


def _clear_google_env(monkeypatch) -> None:
    for var in (
        "GOOGLE_ADS_CLIENT_ID", "GOOGLE_ADS_CLIENT_SECRET", "GOOGLE_ADS_REFRESH_TOKEN",
        "GOOGLE_ADS_DEVELOPER_TOKEN", "GOOGLE_ADS_CUSTOMER_ID", "GOOGLE_ADS_LOGIN_CUSTOMER_ID",
        "GOOGLE_ADS_SYNC_MODE", "DISABLE_GOOGLE_ADS_API_SYNC", "GOOGLE_ADS_API_VERSION",
    ):
        monkeypatch.delenv(var, raising=False)


def _clear_tiktok_env(monkeypatch) -> None:
    for var in (
        "TIKTOK_ACCESS_TOKEN", "TIKTOK_ADVERTISER_ID", "TIKTOK_APP_ID", "TIKTOK_SECRET",
    ):
        monkeypatch.delenv(var, raising=False)


# ── POST /api/ad-names/sync dispatch ──────────────────────────────────────────

def test_sync_invalid_platform_returns_400(client, api_db):
    r = client.post("/api/ad-names/sync", params={"platform": "bogus"})
    assert r.status_code == 400
    assert "platform must be one of" in r.json()["detail"]


def test_sync_all_with_no_credentials_reports_errors_not_crash(client, api_db, monkeypatch):
    _clear_meta_env(monkeypatch)
    _clear_google_env(monkeypatch)
    _clear_tiktok_env(monkeypatch)

    r = client.post("/api/ad-names/sync", params={"platform": "all"})
    assert r.status_code == 200
    body = r.json()
    # Every platform ran and reported a credentials problem; nothing crashed.
    assert body["synced"] == 0
    assert set(body["platforms"].keys()) == {"meta", "google", "tiktok"}
    assert len(body["errors"]) == 3
    joined = " ".join(body["errors"])
    assert "meta:" in joined and "google:" in joined and "tiktok:" in joined
    # No rows written.
    assert _rows(api_db) == []


# ── Meta sync ─────────────────────────────────────────────────────────────────

def test_sync_meta_success_writes_rows(client, api_db, monkeypatch):
    _clear_meta_env(monkeypatch)
    monkeypatch.setenv("META_ACCESS_TOKEN", "tok-meta")
    monkeypatch.setenv("META_AD_ACCOUNT_ID", "act_123456")
    monkeypatch.setenv("META_API_VERSION", "v18.0")

    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith="https://graph.facebook.com/v18.0/act_123456/campaigns").mock(
            return_value=httpx.Response(200, json={"data": [
                {"id": "c1", "name": "Camp One", "status": "ACTIVE"},
                {"id": "c2", "name": "Camp Two", "status": "PAUSED"},
            ]})
        )
        router.get(url__startswith="https://graph.facebook.com/v18.0/act_123456/adsets").mock(
            return_value=httpx.Response(200, json={"data": [
                {"id": "s1", "name": "Set One", "campaign_id": "c1", "status": "ACTIVE"},
            ]})
        )
        router.get(url__startswith="https://graph.facebook.com/v18.0/act_123456/ads").mock(
            return_value=httpx.Response(200, json={"data": [
                {
                    "id": "a1", "name": "Ad Video", "adset_id": "s1", "campaign_id": "c1",
                    "status": "ACTIVE",
                    "creative": {
                        "id": "cr1", "image_url": "", "thumbnail_url": "https://img/thumb1.jpg",
                        "object_type": "VIDEO", "video_id": "vid99",
                    },
                },
                {
                    "id": "a2", "name": "Ad Image", "adset_id": "s1", "campaign_id": "c1",
                    "status": "ACTIVE",
                    "creative": {
                        "id": "cr2", "image_url": "https://img/photo2.jpg",
                        "object_type": "SHARE",
                    },
                },
            ]})
        )

        r = client.post("/api/ad-names/sync", params={"platform": "meta"})

    assert r.status_code == 200
    body = r.json()
    meta = body["platforms"]["meta"]
    assert "error" not in meta
    assert meta["campaigns"] == 2
    assert meta["adsets"] == 1
    assert meta["ads"] == 2
    assert meta["account_id"] == "123456"
    assert meta["synced"] == 5
    assert body["synced"] == 5

    # DB side-effects: rows visible via the list endpoint and directly in DB.
    listed = client.get("/api/ad-names", params={"platform": "meta"}).json()
    assert listed["count"] == 5

    ads = {r["entity_id"]: r for r in _rows(api_db, "entity_type='ad'")}
    assert ads["a1"]["name"] == "Ad Video"
    assert ads["a1"]["creative_type"] == "video"
    assert ads["a1"]["video_id"] == "vid99"
    assert ads["a1"]["thumbnail_url"] == "https://img/thumb1.jpg"
    assert ads["a1"]["creative_id"] == "cr1"
    # image_url preferred over thumbnail_url; object_type SHARE with image => "image"
    assert ads["a2"]["creative_type"] == "image"
    assert ads["a2"]["thumbnail_url"] == "https://img/photo2.jpg"
    assert ads["a2"]["source"] == "api"


def test_sync_meta_missing_credentials(client, api_db, monkeypatch):
    _clear_meta_env(monkeypatch)
    r = client.post("/api/ad-names/sync", params={"platform": "meta"})
    assert r.status_code == 200
    meta = r.json()["platforms"]["meta"]
    assert meta["synced"] == 0
    assert "META_ACCESS_TOKEN" in meta["error"]
    assert _rows(api_db) == []


def test_sync_meta_api_error_surfaces_message(client, api_db, monkeypatch):
    _clear_meta_env(monkeypatch)
    monkeypatch.setenv("META_ACCESS_TOKEN", "tok-meta")
    monkeypatch.setenv("META_AD_ACCOUNT_ID", "123456")

    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith="https://graph.facebook.com/").mock(
            return_value=httpx.Response(400, json={"error": {
                "message": "Invalid OAuth access token", "type": "OAuthException", "code": 190,
            }})
        )
        r = client.post("/api/ad-names/sync", params={"platform": "meta"})

    assert r.status_code == 200
    meta = r.json()["platforms"]["meta"]
    assert meta["synced"] == 0
    assert "Invalid OAuth access token" in meta["error"]
    # the dispatch endpoint bubbles the error up into results["errors"]
    assert any("meta:" in e for e in r.json()["errors"])
    assert _rows(api_db) == []


# ── Google sync ───────────────────────────────────────────────────────────────

def _set_google_creds(monkeypatch) -> None:
    monkeypatch.setenv("GOOGLE_ADS_CLIENT_ID", "gcid")
    monkeypatch.setenv("GOOGLE_ADS_CLIENT_SECRET", "gsecret")
    monkeypatch.setenv("GOOGLE_ADS_REFRESH_TOKEN", "grefresh")
    monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "gdev")
    monkeypatch.setenv("GOOGLE_ADS_CUSTOMER_ID", "111-222-3333")


def test_sync_google_success_writes_rows(client, api_db, monkeypatch):
    _clear_google_env(monkeypatch)
    _set_google_creds(monkeypatch)

    # The three GAQL POSTs all hit the same URL; queue distinct responses in order.
    gaql_responses = [
        httpx.Response(200, json={"results": [
            {"campaign": {"id": "100", "name": "G Camp"}},
        ]}),
        httpx.Response(200, json={"results": [
            {"adGroup": {"id": "200", "name": "G Group", "campaign": "customers/1112223333/campaigns/100"}},
        ]}),
        httpx.Response(200, json={"results": [
            {"adGroupAd": {"ad": {"id": "300", "name": "G Ad"}, "adGroup": "customers/1112223333/adGroups/200"}},
        ]}),
    ]

    with respx.mock(assert_all_mocked=False) as router:
        router.post(url__startswith="https://oauth2.googleapis.com/token").mock(
            return_value=httpx.Response(200, json={"access_token": "ya29.fresh"})
        )
        router.post(url__startswith="https://googleads.googleapis.com/").mock(
            side_effect=gaql_responses
        )
        r = client.post("/api/ad-names/sync", params={"platform": "google"})

    assert r.status_code == 200
    g = r.json()["platforms"]["google"]
    assert "error" not in g
    assert g["campaigns"] == 1
    assert g["adsets"] == 1
    assert g["ads"] == 1
    assert g["synced"] == 3
    assert g["customer_id"] == "111-222-3333"

    rows = {r["entity_id"]: r for r in _rows(api_db, "platform='google'")}
    assert rows["100"]["name"] == "G Camp" and rows["100"]["entity_type"] == "campaign"
    # parent_id parsed from the resource name (last path segment)
    assert rows["200"]["name"] == "G Group" and rows["200"]["parent_id"] == "100"
    assert rows["300"]["name"] == "G Ad" and rows["300"]["parent_id"] == "200"
    assert rows["100"]["source"] == "api"


def test_sync_google_missing_credentials(client, api_db, monkeypatch):
    _clear_google_env(monkeypatch)
    r = client.post("/api/ad-names/sync", params={"platform": "google"})
    assert r.status_code == 200
    g = r.json()["platforms"]["google"]
    assert g["synced"] == 0
    assert "GOOGLE_ADS_CLIENT_ID" in g["error"]
    assert _rows(api_db) == []


def test_sync_google_disabled_mode_skips(client, api_db, monkeypatch):
    _clear_google_env(monkeypatch)
    _set_google_creds(monkeypatch)
    monkeypatch.setenv("GOOGLE_ADS_SYNC_MODE", "script")

    r = client.post("/api/ad-names/sync", params={"platform": "google"})
    assert r.status_code == 200
    g = r.json()["platforms"]["google"]
    assert g["synced"] == 0
    assert g["skipped"] is True
    assert "disabled" in g["reason"].lower()
    assert _rows(api_db) == []


def test_sync_google_http_error_surfaces_message(client, api_db, monkeypatch):
    _clear_google_env(monkeypatch)
    _set_google_creds(monkeypatch)

    with respx.mock(assert_all_mocked=False) as router:
        router.post(url__startswith="https://oauth2.googleapis.com/token").mock(
            return_value=httpx.Response(200, json={"access_token": "ya29.fresh"})
        )
        router.post(url__startswith="https://googleads.googleapis.com/").mock(
            return_value=httpx.Response(403, json={"error": {
                "status": "PERMISSION_DENIED",
                "message": "The caller does not have permission",
                "details": [{"errors": [{
                    "errorCode": {"authorizationError": "USER_PERMISSION_DENIED"},
                    "message": "User doesn't have permission to access customer.",
                }]}],
            }})
        )
        r = client.post("/api/ad-names/sync", params={"platform": "google"})

    assert r.status_code == 200
    g = r.json()["platforms"]["google"]
    assert g["synced"] == 0
    assert "Google Ads API error" in g["error"]
    assert "PERMISSION_DENIED" in g["error"]
    assert _rows(api_db) == []


# ── TikTok sync ───────────────────────────────────────────────────────────────

def test_sync_tiktok_success_writes_rows(client, api_db, monkeypatch):
    _clear_tiktok_env(monkeypatch)
    # No DB token row exists, so get_or_refresh_tiktok_token falls back to the env
    # token and get_tiktok_advertiser_id falls back to the env advertiser id —
    # no token-refresh HTTP call is made.
    monkeypatch.setenv("TIKTOK_ACCESS_TOKEN", "tt-token")
    monkeypatch.setenv("TIKTOK_ADVERTISER_ID", "adv123")

    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith="https://business-api.tiktok.com/open_api/v1.3/campaign/get/").mock(
            return_value=httpx.Response(200, json={"code": 0, "data": {"list": [
                {"campaign_id": "tc1", "campaign_name": "TT Camp"},
            ]}})
        )
        router.get(url__startswith="https://business-api.tiktok.com/open_api/v1.3/adgroup/get/").mock(
            return_value=httpx.Response(200, json={"code": 0, "data": {"list": [
                {"adgroup_id": "tg1", "adgroup_name": "TT Group", "campaign_id": "tc1"},
            ]}})
        )
        router.get(url__startswith="https://business-api.tiktok.com/open_api/v1.3/ad/get/").mock(
            return_value=httpx.Response(200, json={"code": 0, "data": {"list": [
                {"ad_id": "ta1", "ad_name": "TT Video Ad", "adgroup_id": "tg1", "video_id": "v1"},
                {"ad_id": "ta2", "ad_name": "TT Image Ad", "adgroup_id": "tg1", "image_ids": ["img1"]},
            ]}})
        )
        router.get(url__startswith="https://business-api.tiktok.com/open_api/v1.3/video/info/").mock(
            return_value=httpx.Response(200, json={"code": 0, "data": {"list": [
                {"video_id": "v1", "cover_url": "https://tt/cover1.jpg"},
            ]}})
        )
        router.get(url__startswith="https://business-api.tiktok.com/open_api/v1.3/image/info/").mock(
            return_value=httpx.Response(200, json={"code": 0, "data": {"list": [
                {"image_id": "img1", "image_url": "https://tt/image1.jpg"},
            ]}})
        )
        r = client.post("/api/ad-names/sync", params={"platform": "tiktok"})

    assert r.status_code == 200
    t = r.json()["platforms"]["tiktok"]
    assert "error" not in t
    assert t["campaigns"] == 1
    assert t["adgroups"] == 1
    assert t["ads"] == 2
    assert t["synced"] == 4

    rows = {r["entity_id"]: r for r in _rows(api_db, "platform='tiktok'")}
    assert rows["tc1"]["name"] == "TT Camp" and rows["tc1"]["entity_type"] == "campaign"
    assert rows["tg1"]["parent_id"] == "tc1" and rows["tg1"]["entity_type"] == "adset"
    assert rows["ta1"]["creative_type"] == "video"
    assert rows["ta1"]["video_id"] == "v1"
    assert rows["ta1"]["thumbnail_url"] == "https://tt/cover1.jpg"
    assert rows["ta2"]["creative_type"] == "image"
    assert rows["ta2"]["thumbnail_url"] == "https://tt/image1.jpg"


def test_sync_tiktok_missing_credentials(client, api_db, monkeypatch):
    _clear_tiktok_env(monkeypatch)
    r = client.post("/api/ad-names/sync", params={"platform": "tiktok"})
    assert r.status_code == 200
    t = r.json()["platforms"]["tiktok"]
    assert t["synced"] == 0
    assert "TikTok not connected" in t["error"]
    assert _rows(api_db) == []


def test_sync_tiktok_api_error_code_surfaces(client, api_db, monkeypatch):
    _clear_tiktok_env(monkeypatch)
    monkeypatch.setenv("TIKTOK_ACCESS_TOKEN", "tt-token")
    monkeypatch.setenv("TIKTOK_ADVERTISER_ID", "adv123")

    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith="https://business-api.tiktok.com/open_api/v1.3/campaign/get/").mock(
            return_value=httpx.Response(200, json={"code": 40001, "message": "Invalid access token"})
        )
        r = client.post("/api/ad-names/sync", params={"platform": "tiktok"})

    assert r.status_code == 200
    t = r.json()["platforms"]["tiktok"]
    assert t["synced"] == 0
    assert "TikTok campaigns error 40001" in t["error"]
    assert "Invalid access token" in t["error"]
    assert _rows(api_db) == []


# ── get_thumbnails_map ────────────────────────────────────────────────────────

def test_get_thumbnails_map_returns_creative_map(api_db):
    ad_names._ensure_table(api_db)  # add the extra creative columns
    insert_rows(api_db, "ad_names", [
        {**ad_name(platform="meta", entity_type="ad", entity_id="a1", name="Ad 1", source="api"),
         "thumbnail_url": "https://img/a1.jpg", "creative_type": "video", "video_id": "v1"},
        {**ad_name(platform="tiktok", entity_type="ad", entity_id="a2", name="Ad 2", source="api"),
         "thumbnail_url": "https://img/a2.jpg", "creative_type": "image", "video_id": ""},
        # A campaign row must be ignored (only entity_type='ad' is returned).
        ad_name(platform="meta", entity_type="campaign", entity_id="c1", name="Camp"),
    ])

    m = ad_names.get_thumbnails_map(api_db)

    # platform-aware key + raw fallback key
    assert m["meta|a1"] == {"thumbnail_url": "https://img/a1.jpg", "creative_type": "video", "video_id": "v1"}
    assert m["a1"]["thumbnail_url"] == "https://img/a1.jpg"
    assert m["tiktok|a2"]["creative_type"] == "image"
    assert m["a2"]["thumbnail_url"] == "https://img/a2.jpg"
    # campaign rows excluded
    assert "c1" not in m
    assert "meta|c1" not in m


def test_get_thumbnails_map_empty_db(api_db):
    assert ad_names.get_thumbnails_map(api_db) == {}


# ── GET /api/ad-names/resolve ─────────────────────────────────────────────────

def test_resolve_returns_names_for_ids(client, api_db):
    insert_rows(api_db, "ad_names", [
        ad_name(platform="meta", entity_type="campaign", entity_id="c1", name="Camp One"),
        ad_name(platform="meta", entity_type="adset", entity_id="s1", name="Set One"),
        ad_name(platform="google", entity_type="campaign", entity_id="g1", name="G Camp"),
    ])

    r = client.get("/api/ad-names/resolve", params={"ids": "c1, s1 , missing"})
    assert r.status_code == 200
    assert r.json()["names"] == {"c1": "Camp One", "s1": "Set One"}


def test_resolve_filters_by_platform(client, api_db):
    insert_rows(api_db, "ad_names", [
        ad_name(platform="meta", entity_type="campaign", entity_id="dup", name="Meta Camp"),
        ad_name(platform="google", entity_type="campaign", entity_id="dup2", name="Google Camp"),
    ])
    r = client.get("/api/ad-names/resolve", params={"ids": "dup,dup2", "platform": "google"})
    assert r.status_code == 200
    assert r.json()["names"] == {"dup2": "Google Camp"}


def test_resolve_empty_ids_returns_empty(client, api_db):
    assert client.get("/api/ad-names/resolve", params={"ids": ""}).json() == {"names": {}}


# ── GET /api/ad-names/video-url ───────────────────────────────────────────────

def test_video_url_success(client, api_db, monkeypatch):
    _clear_meta_env(monkeypatch)
    monkeypatch.setenv("META_ACCESS_TOKEN", "tok-meta")
    monkeypatch.setenv("META_API_VERSION", "v18.0")

    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith="https://graph.facebook.com/v18.0/vid42").mock(
            return_value=httpx.Response(200, json={"source": "https://video.fb/vid42.mp4", "id": "vid42"})
        )
        r = client.get("/api/ad-names/video-url", params={"video_id": "vid42"})

    assert r.status_code == 200
    body = r.json()
    assert body["video_url"] == "https://video.fb/vid42.mp4"
    assert body["video_id"] == "vid42"


def test_video_url_missing_credentials(client, api_db, monkeypatch):
    _clear_meta_env(monkeypatch)
    r = client.get("/api/ad-names/video-url", params={"video_id": "vid42"})
    assert r.status_code == 200
    body = r.json()
    assert body["video_url"] == ""
    assert "missing credentials" in body["error"]


def test_video_url_api_error_returns_message(client, api_db, monkeypatch):
    _clear_meta_env(monkeypatch)
    monkeypatch.setenv("META_ACCESS_TOKEN", "tok-meta")

    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith="https://graph.facebook.com/").mock(
            return_value=httpx.Response(400, json={"error": {"message": "Unknown video id"}})
        )
        r = client.get("/api/ad-names/video-url", params={"video_id": "bad"})

    assert r.status_code == 200
    body = r.json()
    assert body["video_url"] == ""
    assert body["error"] == "Unknown video id"


# ── GET /api/ad-names/thumbnail ───────────────────────────────────────────────

def test_thumbnail_refreshes_from_meta_and_persists(client, api_db, monkeypatch):
    _clear_meta_env(monkeypatch)
    monkeypatch.setenv("META_ACCESS_TOKEN", "tok-meta")
    monkeypatch.setenv("META_API_VERSION", "v18.0")
    ad_names._ensure_table(api_db)  # add the extra creative columns
    insert_rows(api_db, "ad_names", [
        {**ad_name(platform="meta", entity_type="ad", entity_id="a1", name="Ad 1", source="api"),
         "thumbnail_url": "https://old/thumb.jpg", "creative_id": "cr1", "creative_type": "image"},
    ])

    with respx.mock(assert_all_mocked=False) as router:
        router.get(url__startswith="https://graph.facebook.com/v18.0/cr1").mock(
            return_value=httpx.Response(200, json={"image_url": "https://fresh/thumb.jpg"})
        )
        r = client.get("/api/ad-names/thumbnail", params={"ad_id": "a1", "platform": "meta"})

    assert r.status_code == 200
    body = r.json()
    assert body["thumbnail_url"] == "https://fresh/thumb.jpg"
    assert body["creative_id"] == "cr1"
    assert body["refreshed"] is True
    # The refreshed URL was persisted back to the DB.
    stored = _rows(api_db, "entity_id='a1'")[0]["thumbnail_url"]
    assert stored == "https://fresh/thumb.jpg"


def test_thumbnail_falls_back_to_stored_url_when_refetch_fails(client, api_db, monkeypatch):
    _clear_meta_env(monkeypatch)
    monkeypatch.setenv("META_ACCESS_TOKEN", "tok-meta")
    ad_names._ensure_table(api_db)  # add the extra creative columns
    insert_rows(api_db, "ad_names", [
        {**ad_name(platform="meta", entity_type="ad", entity_id="a1", name="Ad 1", source="api"),
         "thumbnail_url": "https://stored/thumb.jpg", "creative_id": "cr1"},
    ])

    with respx.mock(assert_all_mocked=False) as router:
        # Meta returns no usable image/thumbnail field -> code keeps stored URL.
        router.get(url__startswith="https://graph.facebook.com/").mock(
            return_value=httpx.Response(200, json={"id": "cr1"})
        )
        r = client.get("/api/ad-names/thumbnail", params={"ad_id": "a1", "platform": "meta"})

    assert r.status_code == 200
    body = r.json()
    assert body["thumbnail_url"] == "https://stored/thumb.jpg"
    assert body["refreshed"] is False


def test_thumbnail_no_creative_id_returns_stored(client, api_db, monkeypatch):
    _clear_meta_env(monkeypatch)
    monkeypatch.setenv("META_ACCESS_TOKEN", "tok-meta")
    # No creative_id => the refresh branch is skipped, stored URL returned as-is.
    ad_names._ensure_table(api_db)  # add the extra creative columns
    insert_rows(api_db, "ad_names", [
        {**ad_name(platform="meta", entity_type="ad", entity_id="a1", name="Ad 1", source="api"),
         "thumbnail_url": "https://stored/thumb.jpg"},
    ])
    r = client.get("/api/ad-names/thumbnail", params={"ad_id": "a1", "platform": "meta"})
    assert r.status_code == 200
    body = r.json()
    assert body["thumbnail_url"] == "https://stored/thumb.jpg"
    assert body["refreshed"] is False


def test_thumbnail_not_found(client, api_db, monkeypatch):
    _clear_meta_env(monkeypatch)
    r = client.get("/api/ad-names/thumbnail", params={"ad_id": "nope", "platform": "meta"})
    assert r.status_code == 200
    assert r.json() == {"thumbnail_url": "", "error": "not found"}
