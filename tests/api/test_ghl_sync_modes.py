"""Regression tests for lightweight GHL attribution sync mode."""

from __future__ import annotations

import respx
from httpx import Response

from attributionops.db import sql_rows
import api.ghl_sync as gs

GHL = "https://services.leadconnectorhq.com"


def test_sync_can_skip_forms_and_opportunities(client, api_db):
    gs.save_ghl_credentials(api_db, "pit-test", "loc_1")
    contacts_page = {
        "contacts": [
            {
                "id": "contact-1",
                "email": "lead@example.com",
                "dateAdded": "2026-07-05T12:00:00.000Z",
                "source": "facebook",
                "attributionSource": {
                    "utmSource": "facebook",
                    "utmMedium": "paid_social",
                    "utmFbclid": "fb-click-1",
                },
            }
        ],
        "meta": {},
    }

    with respx.mock(assert_all_mocked=True) as router:
        router.get(url__startswith=f"{GHL}/contacts/").mock(
            return_value=Response(200, json=contacts_page)
        )
        response = client.post(
            "/api/ghl/sync"
            "?start_date=2026-07-04&end_date=2026-07-11"
            "&include_forms=false&include_opportunities=false"
        )

    assert response.status_code == 200
    body = response.json()
    assert body["errors"] == []
    assert body["leads"] == 1
    assert body["optins"] == 0
    assert body["orders"] == 0
    assert body["open_opportunities"] == 0
    assert body["include_forms"] is False
    assert body["include_opportunities"] is False

    rows = sql_rows(api_db, "SELECT type, customer_key FROM conversions")
    assert len(rows) == 1
    assert rows[0]["type"] == "Lead"
