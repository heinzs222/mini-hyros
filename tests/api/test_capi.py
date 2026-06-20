"""Tests for the Conversions API pushback endpoints (/api/capi).

Covers:
  - GET  /api/capi/status  — credential detection + sync stats over a seeded DB
  - GET  /api/capi/log     — push history (DB read, with filters)
  - POST /api/capi/push    — manual push per platform (respx-mocked success,
                             missing-credentials path, unsupported platform)
  - POST /api/capi/auto-sync — bulk push of un-synced conversions

All outbound HTTP (Meta / Google / TikTok) is mocked with respx; nothing
ever hits the real network.
"""

from __future__ import annotations

import respx
from httpx import Response

from attributionops.db import sql_rows
from tests.helpers import insert_rows, touchpoint


META_OK = {"events_received": 1, "messages": [], "fbtrace_id": "abc"}
GOOGLE_OK = {"results": [{"gclid": "g123"}], "partialFailureError": None}
TIKTOK_OK = {"code": 0, "message": "OK", "request_id": "req1"}


def _set_meta(monkeypatch):
    monkeypatch.setenv("META_ACCESS_TOKEN", "meta-tok")
    monkeypatch.setenv("META_PIXEL_ID", "px123")


def _set_google(monkeypatch):
    monkeypatch.setenv("GOOGLE_ADS_DEVELOPER_TOKEN", "dev-tok")
    monkeypatch.setenv("GOOGLE_ADS_CUSTOMER_ID", "111")
    monkeypatch.setenv("GOOGLE_ADS_CONVERSION_ACTION", "222")
    monkeypatch.setenv("GOOGLE_ADS_ACCESS_TOKEN", "acc-tok")


def _set_tiktok(monkeypatch):
    monkeypatch.setenv("TIKTOK_ACCESS_TOKEN", "tt-tok")
    monkeypatch.setenv("TIKTOK_PIXEL_ID", "ttpx")


def _clear_all_creds(monkeypatch):
    for var in (
        "META_ACCESS_TOKEN", "META_PIXEL_ID",
        "GOOGLE_ADS_DEVELOPER_TOKEN", "GOOGLE_ADS_CUSTOMER_ID",
        "GOOGLE_ADS_CONVERSION_ACTION", "GOOGLE_ADS_ACCESS_TOKEN",
        "TIKTOK_ACCESS_TOKEN", "TIKTOK_PIXEL_ID",
    ):
        monkeypatch.delenv(var, raising=False)


# ── status ────────────────────────────────────────────────────────────────────

def test_status_no_credentials(client, api_db, monkeypatch):
    _clear_all_creds(monkeypatch)
    r = client.get("/api/capi/status")
    assert r.status_code == 200
    body = r.json()
    assert set(body["platforms"]) == {"meta", "google", "tiktok"}
    for p in ("meta", "google", "tiktok"):
        assert body["platforms"][p]["configured"] is False
        assert isinstance(body["platforms"][p]["env_vars"], list)
    # Fresh DB: no conversions, nothing synced.
    assert body["total_conversions"] == 0
    assert body["synced"] == 0
    assert body["unsynced"] == 0


def test_status_reports_configured_and_stats(client, api_db, monkeypatch):
    _set_meta(monkeypatch)
    _set_tiktok(monkeypatch)
    monkeypatch.delenv("GOOGLE_ADS_DEVELOPER_TOKEN", raising=False)
    monkeypatch.delenv("GOOGLE_ADS_CUSTOMER_ID", raising=False)

    # Seed conversions and a capi_log so stats/unsynced are non-trivial.
    insert_rows(api_db, "conversions", [
        {"conversion_id": "cv1", "ts": "2026-01-01T00:00:00Z", "type": "Purchase",
         "value": "10", "order_id": "o1", "customer_key": "ck1"},
        {"conversion_id": "cv2", "ts": "2026-01-02T00:00:00Z", "type": "Purchase",
         "value": "20", "order_id": "o2", "customer_key": "ck2"},
    ])
    # The /log endpoint creates capi_log; call status first to create the table,
    # then push a success via auto-sync-style log by writing through a real push.
    # Simpler: trigger table creation then insert directly.
    client.get("/api/capi/status")  # ensures capi_log table exists
    insert_rows(api_db, "capi_log", [
        {"id": "l1", "ts": "2026-01-03T00:00:00Z", "platform": "meta",
         "event_name": "Purchase", "customer_key": "ck1", "order_id": "o1",
         "value": "10", "status": "success", "response": "{}", "error": ""},
        {"id": "l2", "ts": "2026-01-03T00:01:00Z", "platform": "meta",
         "event_name": "Purchase", "customer_key": "ck2", "order_id": "o2",
         "value": "20", "status": "failed", "response": "{}", "error": "boom"},
    ])

    body = client.get("/api/capi/status").json()
    assert body["platforms"]["meta"]["configured"] is True
    assert body["platforms"]["tiktok"]["configured"] is True
    assert body["platforms"]["google"]["configured"] is False
    # Stats grouped by platform/status.
    assert body["platforms"]["meta"]["stats"]["success"] == 1
    assert body["platforms"]["meta"]["stats"]["failed"] == 1
    assert body["total_conversions"] == 2
    assert body["synced"] == 1
    assert body["unsynced"] == 1


# ── log ──────────────────────────────────────────────────────────────────────

def test_log_empty_on_fresh_db(client, api_db):
    body = client.get("/api/capi/log").json()
    assert body == {"rows": [], "count": 0}


def test_log_returns_rows_and_filters_by_platform(client, api_db):
    client.get("/api/capi/log")  # create the capi_log table
    insert_rows(api_db, "capi_log", [
        {"id": "a", "ts": "2026-01-05T00:00:00Z", "platform": "meta",
         "event_name": "Purchase", "customer_key": "ck", "order_id": "o1",
         "value": "10", "status": "success", "response": "{}", "error": ""},
        {"id": "b", "ts": "2026-01-06T00:00:00Z", "platform": "tiktok",
         "event_name": "CompletePayment", "customer_key": "ck", "order_id": "o2",
         "value": "20", "status": "failed", "response": "{}", "error": "x"},
    ])

    all_rows = client.get("/api/capi/log").json()
    assert all_rows["count"] == 2
    # Default ordering is ts DESC -> tiktok (later ts) first.
    assert all_rows["rows"][0]["platform"] == "tiktok"

    filtered = client.get("/api/capi/log", params={"platform": "meta"}).json()
    assert filtered["count"] == 1
    assert filtered["rows"][0]["platform"] == "meta"

    limited = client.get("/api/capi/log", params={"limit": 1}).json()
    assert limited["count"] == 1


# ── push: success per platform ───────────────────────────────────────────────

def test_push_meta_success_logs_row(client, api_db, monkeypatch):
    _set_meta(monkeypatch)
    event = {
        "platform": "meta",
        "event_name": "Purchase",
        "ts": "2026-01-10T12:00:00Z",
        "customer_key": "buyer@example.com",
        "order_id": "ord-meta-1",
        "value": 49.99,
        "currency": "USD",
        "fbclid": "fb1",
    }
    with respx.mock(assert_all_mocked=False) as router:
        route = router.post(url__startswith="https://graph.facebook.com/").mock(
            return_value=Response(200, json=META_OK)
        )
        r = client.post("/api/capi/push", json=event)

    assert r.status_code == 200
    assert route.called
    body = r.json()
    assert body["ok"] is True
    assert body["status_code"] == 200
    assert body["events_received"] == 1

    rows = sql_rows(api_db, "SELECT * FROM capi_log WHERE order_id = ?", ["ord-meta-1"])
    assert len(rows) == 1
    assert rows[0]["platform"] == "meta"
    assert rows[0]["status"] == "success"


def test_push_facebook_alias_routes_to_meta(client, api_db, monkeypatch):
    _set_meta(monkeypatch)
    event = {
        "platform": "facebook",
        "ts": "2026-01-10T12:00:00Z",
        "customer_key": "x@y.com",
        "order_id": "ord-fb-1",
        "value": 5,
    }
    with respx.mock(assert_all_mocked=False) as router:
        router.post(url__startswith="https://graph.facebook.com/").mock(
            return_value=Response(200, json=META_OK)
        )
        body = client.post("/api/capi/push", json=event).json()
    assert body["ok"] is True
    rows = sql_rows(api_db, "SELECT platform FROM capi_log WHERE order_id = ?", ["ord-fb-1"])
    assert rows[0]["platform"] == "facebook"


def test_push_google_success(client, api_db, monkeypatch):
    _set_google(monkeypatch)
    event = {
        "platform": "google",
        "ts": "2026-01-11T08:30:00Z",
        "customer_key": "g@y.com",
        "order_id": "ord-g-1",
        "value": 100,
        "gclid": "GCLID-123",
    }
    with respx.mock(assert_all_mocked=False) as router:
        route = router.post(url__startswith="https://googleads.googleapis.com/").mock(
            return_value=Response(200, json=GOOGLE_OK)
        )
        body = client.post("/api/capi/push", json=event).json()
    assert route.called
    assert body["ok"] is True
    assert body["status_code"] == 200
    rows = sql_rows(api_db, "SELECT status FROM capi_log WHERE order_id = ?", ["ord-g-1"])
    assert rows[0]["status"] == "success"


def test_push_google_without_gclid_is_failure(client, api_db, monkeypatch):
    _set_google(monkeypatch)
    event = {
        "platform": "google",
        "ts": "2026-01-11T08:30:00Z",
        "customer_key": "g@y.com",
        "order_id": "ord-g-nogclid",
        "value": 100,
    }
    # No outbound call should happen (early return on missing gclid).
    body = client.post("/api/capi/push", json=event).json()
    assert body["ok"] is False
    assert "gclid" in body["error"]
    rows = sql_rows(api_db, "SELECT status, error FROM capi_log WHERE order_id = ?", ["ord-g-nogclid"])
    assert rows[0]["status"] == "failed"


def test_push_tiktok_success(client, api_db, monkeypatch):
    _set_tiktok(monkeypatch)
    event = {
        "platform": "tiktok",
        "event_name": "CompletePayment",
        "ts": "2026-01-12T10:00:00Z",
        "customer_key": "t@y.com",
        "order_id": "ord-tt-1",
        "value": 12.5,
        "ttclid": "TT-1",
    }
    with respx.mock(assert_all_mocked=False) as router:
        route = router.post(url__startswith="https://business-api.tiktok.com/").mock(
            return_value=Response(200, json=TIKTOK_OK)
        )
        body = client.post("/api/capi/push", json=event).json()
    assert route.called
    assert body["ok"] is True  # code == 0
    rows = sql_rows(api_db, "SELECT status FROM capi_log WHERE order_id = ?", ["ord-tt-1"])
    assert rows[0]["status"] == "success"


def test_push_tiktok_nonzero_code_is_failure(client, api_db, monkeypatch):
    _set_tiktok(monkeypatch)
    event = {
        "platform": "tiktok",
        "ts": "2026-01-12T10:00:00Z",
        "customer_key": "t@y.com",
        "order_id": "ord-tt-err",
        "value": 1,
    }
    with respx.mock(assert_all_mocked=False) as router:
        router.post(url__startswith="https://business-api.tiktok.com/").mock(
            return_value=Response(200, json={"code": 40001, "message": "bad"})
        )
        body = client.post("/api/capi/push", json=event).json()
    assert body["ok"] is False
    rows = sql_rows(api_db, "SELECT status FROM capi_log WHERE order_id = ?", ["ord-tt-err"])
    assert rows[0]["status"] == "failed"


# ── push: error / edge paths ─────────────────────────────────────────────────

def test_push_unsupported_platform(client, api_db):
    r = client.post("/api/capi/push", json={"platform": "snapchat", "order_id": "z"})
    assert r.status_code == 400
    assert "Unsupported platform" in r.json()["detail"]


def test_push_meta_missing_credentials(client, api_db, monkeypatch):
    _clear_all_creds(monkeypatch)
    event = {
        "platform": "meta",
        "ts": "2026-01-10T12:00:00Z",
        "customer_key": "x@y.com",
        "order_id": "ord-nocreds",
        "value": 9,
    }
    # No network expected; the module short-circuits on missing env.
    r = client.post("/api/capi/push", json=event)
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is False
    assert "META_ACCESS_TOKEN" in body["error"]
    rows = sql_rows(api_db, "SELECT status, error FROM capi_log WHERE order_id = ?", ["ord-nocreds"])
    assert rows[0]["status"] == "failed"
    assert "META_ACCESS_TOKEN" in rows[0]["error"]


def test_push_tiktok_missing_credentials(client, api_db, monkeypatch):
    _clear_all_creds(monkeypatch)
    event = {
        "platform": "tiktok",
        "ts": "2026-01-10T12:00:00Z",
        "customer_key": "x@y.com",
        "order_id": "ord-tt-nocreds",
        "value": 9,
    }
    body = client.post("/api/capi/push", json=event).json()
    assert body["ok"] is False
    assert "TIKTOK_ACCESS_TOKEN" in body["error"]


def test_push_google_missing_credentials(client, api_db, monkeypatch):
    _clear_all_creds(monkeypatch)
    event = {
        "platform": "google",
        "ts": "2026-01-10T12:00:00Z",
        "customer_key": "x@y.com",
        "order_id": "ord-g-nocreds",
        "value": 9,
        "gclid": "g",
    }
    body = client.post("/api/capi/push", json=event).json()
    assert body["ok"] is False
    assert "Google Ads env vars required" in body["error"]


def test_push_exception_path_logs_error(client, api_db, monkeypatch):
    """A malformed ts triggers the except branch (datetime parse error)."""
    _set_meta(monkeypatch)
    event = {
        "platform": "meta",
        "ts": "not-a-timestamp",
        "customer_key": "x@y.com",
        "order_id": "ord-bad-ts",
        "value": 9,
    }
    # The bad ts raises before any HTTP call, so no outbound route is needed.
    body = client.post("/api/capi/push", json=event).json()
    assert body["ok"] is False
    assert "error" in body
    rows = sql_rows(api_db, "SELECT status FROM capi_log WHERE order_id = ?", ["ord-bad-ts"])
    assert rows[0]["status"] == "error"


# ── auto-sync ────────────────────────────────────────────────────────────────

def _seed_conversion_with_touchpoint(api_db, *, order_id, customer_key, platform,
                                      gclid="", fbclid="", ttclid="", value="25",
                                      ctype="Purchase", ts="2026-02-01T00:00:00Z"):
    insert_rows(api_db, "conversions", [{
        "conversion_id": "conv-" + order_id, "ts": ts, "type": ctype,
        "value": value, "order_id": order_id, "customer_key": customer_key,
    }])
    tp = touchpoint(ts, customer_key, platform=platform)
    tp["gclid"] = gclid
    tp["fbclid"] = fbclid
    tp["ttclid"] = ttclid
    insert_rows(api_db, "touchpoints", [tp])


def test_auto_sync_pushes_unsynced_meta_conversion(client, api_db, monkeypatch):
    _set_meta(monkeypatch)
    _seed_conversion_with_touchpoint(
        api_db, order_id="o-meta", customer_key="ck-meta", platform="meta", fbclid="fb")

    with respx.mock(assert_all_mocked=False) as router:
        route = router.post(url__startswith="https://graph.facebook.com/").mock(
            return_value=Response(200, json=META_OK)
        )
        r = client.post("/api/capi/auto-sync")

    assert r.status_code == 200
    assert route.called
    body = r.json()
    assert body["total"] == 1
    assert body["pushed"] == 1
    assert body["failed"] == 0
    assert body["details"][0] == {"order_id": "o-meta", "platform": "meta", "status": "success"}

    rows = sql_rows(api_db, "SELECT * FROM capi_log WHERE order_id = ?", ["o-meta"])
    assert len(rows) == 1
    assert rows[0]["status"] == "success"


def test_auto_sync_records_failed_push(client, api_db, monkeypatch):
    """A push that returns ok=False is counted as failed and logged."""
    _set_meta(monkeypatch)
    _seed_conversion_with_touchpoint(
        api_db, order_id="o-fail", customer_key="ck-fail", platform="meta", fbclid="fb")

    with respx.mock(assert_all_mocked=False) as router:
        # Non-200 -> _push_meta returns ok=False.
        router.post(url__startswith="https://graph.facebook.com/").mock(
            return_value=Response(400, json={"error": {"message": "bad request"}})
        )
        body = client.post("/api/capi/auto-sync").json()

    assert body["total"] == 1
    assert body["pushed"] == 0
    assert body["failed"] == 1
    assert body["details"][0]["status"] == "failed"
    rows = sql_rows(api_db, "SELECT status FROM capi_log WHERE order_id = ?", ["o-fail"])
    assert rows[0]["status"] == "failed"


def test_auto_sync_skips_conversions_without_known_platform(client, api_db, monkeypatch):
    _set_meta(monkeypatch)
    # Conversion whose touchpoint platform is unsupported -> skipped.
    _seed_conversion_with_touchpoint(
        api_db, order_id="o-skip", customer_key="ck-skip", platform="snapchat")

    # Skipped conversion makes no outbound call; allow the route to go unused.
    with respx.mock(assert_all_mocked=False, assert_all_called=False) as router:
        router.post(url__startswith="https://graph.facebook.com/").mock(
            return_value=Response(200, json=META_OK)
        )
        body = client.post("/api/capi/auto-sync").json()

    assert body["total"] == 1
    assert body["skipped"] == 1
    assert body["pushed"] == 0
    # Nothing logged for a skipped conversion.
    assert sql_rows(api_db, "SELECT * FROM capi_log WHERE order_id = ?", ["o-skip"]) == []


def test_auto_sync_excludes_already_synced(client, api_db, monkeypatch):
    _set_meta(monkeypatch)
    _seed_conversion_with_touchpoint(
        api_db, order_id="o-done", customer_key="ck-done", platform="meta", fbclid="fb")
    # Mark it already-synced in capi_log.
    client.get("/api/capi/log")  # create table
    insert_rows(api_db, "capi_log", [{
        "id": "prev", "ts": "2026-02-02T00:00:00Z", "platform": "meta",
        "event_name": "Purchase", "customer_key": "ck-done", "order_id": "o-done",
        "value": "25", "status": "success", "response": "{}", "error": "",
    }])

    with respx.mock(assert_all_mocked=False, assert_all_called=False) as router:
        route = router.post(url__startswith="https://graph.facebook.com/").mock(
            return_value=Response(200, json=META_OK)
        )
        body = client.post("/api/capi/auto-sync").json()

    # The only conversion is already synced -> nothing to push, no outbound call.
    assert body["total"] == 0
    assert body["pushed"] == 0
    assert route.called is False


def test_auto_sync_empty_db(client, api_db, monkeypatch):
    _set_meta(monkeypatch)
    body = client.post("/api/capi/auto-sync").json()
    assert body == {"total": 0, "pushed": 0, "failed": 0, "skipped": 0, "details": []}
