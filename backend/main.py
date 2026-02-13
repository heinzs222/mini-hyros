from __future__ import annotations

import asyncio
import json
import os
import sys
from contextlib import asynccontextmanager
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

# Add parent dir so we can import attributionops
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from attributionops.config import default_db_path
from attributionops.report import ReportInputs, build_hyros_like_report
from attributionops.tools.ads import ads_get_spend, ads_get_reported_value, ads_list_platforms
from attributionops.tools.attribution import attribution_run, attribution_day_totals
from attributionops.tools.integrations import integrations_status
from attributionops.tools.tracking import tracking_health_check
from attributionops.tools.forecasting import forecasting_run
from attributionops.tools.warehouse import warehouse_query
from attributionops.db import query as db_query

from api.webhooks import router as webhooks_router
from api.video_metrics import router as video_router
from api.connections import router as connections_router
from api.ghl import router as ghl_router
from api.capi import router as capi_router
from api.ltv import router as ltv_router
from api.journey import router as journey_router
from api.funnel import router as funnel_router
from api.cohort import router as cohort_router
from api.refunds import router as refunds_router
from api.email_sms import router as email_sms_router
from api.ai_recommendations import router as ai_router

# ── WebSocket manager ──────────────────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        self.active.remove(ws)

    async def broadcast(self, data: dict):
        for ws in list(self.active):
            try:
                await ws.send_json(data)
            except Exception:
                self.active.remove(ws)

manager = ConnectionManager()


# ── App ────────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure dummy DB exists
    db = default_db_path()
    if not Path(db).exists():
        print(f"[warn] DB not found at {db} – run generate_dummy_data.py first")
    yield

app = FastAPI(title="AttributionOps – Mini Hyros", version="0.1.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(webhooks_router, prefix="/api/webhooks", tags=["webhooks"])
app.include_router(video_router, prefix="/api/video", tags=["video"])
app.include_router(connections_router, prefix="/api/connections", tags=["connections"])
app.include_router(ghl_router, prefix="/api/webhooks", tags=["ghl"])
app.include_router(capi_router, prefix="/api/capi", tags=["capi"])
app.include_router(ltv_router, prefix="/api/ltv", tags=["ltv"])
app.include_router(journey_router, prefix="/api/journey", tags=["journey"])
app.include_router(funnel_router, prefix="/api/funnel", tags=["funnel"])
app.include_router(cohort_router, prefix="/api/cohort", tags=["cohort"])
app.include_router(refunds_router, prefix="/api/refunds", tags=["refunds"])
app.include_router(email_sms_router, prefix="/api/email-sms", tags=["email-sms"])
app.include_router(ai_router, prefix="/api/ai", tags=["ai"])


def _db() -> str:
    return os.environ.get("ATTRIBUTIONOPS_DB_PATH", default_db_path())


def _default_dates() -> tuple[str, str]:
    end = date.today()
    start = end - timedelta(days=30)
    return start.isoformat(), end.isoformat()


# ── REST endpoints ─────────────────────────────────────────────────────────────

@app.get("/api/health")
async def health():
    return {"status": "ok", "db": _db()}


@app.get("/api/report")
async def get_report(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    model: str = Query(default="last_click"),
    lookback_days: int = Query(default=30),
    active_tab: str = Query(default="traffic_source"),
    conversion_type: str = Query(default="Purchase"),
    use_click_date: bool = Query(default=False),
):
    s, e = _default_dates()
    if not start_date:
        start_date = s
    if not end_date:
        end_date = e
    inputs = ReportInputs(
        report_name="Mini Hyros Report",
        start_date=start_date,
        end_date=end_date,
        preset="Custom",
        currency="USD",
        attribution_model=model,
        lookback_days=lookback_days,
        conversion_type=conversion_type,
        use_date_of_click_attribution=use_click_date,
        active_tab=active_tab,
    )
    return build_hyros_like_report(_db(), inputs)


@app.get("/api/report/children")
async def get_report_children(
    parent_tab: str = Query(...),
    parent_id: str = Query(...),
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    model: str = Query(default="last_click"),
    lookback_days: int = Query(default=30),
    conversion_type: str = Query(default="Purchase"),
    use_click_date: bool = Query(default=False),
):
    """Drill-down: return child rows for a given parent.
    parent_tab=campaign  → returns ad_sets under that campaign
    parent_tab=ad_set    → returns ads under that ad_set
    parent_tab=traffic_source → returns campaigns under that platform
    parent_tab=ad_account → returns campaigns under that account
    """
    s, e = _default_dates()
    if not start_date:
        start_date = s
    if not end_date:
        end_date = e

    db_path = _db()
    date_basis = "click" if use_click_date else "conversion"

    # Parse parent_id (pipe-separated: platform|account_id|campaign_id|adset_id|...)
    parts = parent_id.split("|")

    # Determine child level and filters
    child_tab = ""
    filters: dict[str, str] = {}
    if parent_tab == "traffic_source":
        child_tab = "campaign"
        # parent_id is like "facebook / paid_social" — extract platform
        platform_map = {
            "facebook / paid_social": "meta",
            "google / cpc": "google",
            "tiktok / paid_social": "tiktok",
        }
        filters["platform"] = platform_map.get(parent_id, parts[0] if parts else "")
    elif parent_tab == "ad_account":
        child_tab = "campaign"
        if len(parts) >= 2:
            filters["platform"] = parts[0]
            filters["account_id"] = parts[1]
    elif parent_tab == "campaign":
        child_tab = "ad_set"
        if len(parts) >= 3:
            filters["platform"] = parts[0]
            filters["account_id"] = parts[1]
            filters["campaign_id"] = parts[2]
        elif len(parts) == 1:
            filters["campaign_id"] = parts[0]
    elif parent_tab == "ad_set":
        child_tab = "ad"
        if len(parts) >= 4:
            filters["platform"] = parts[0]
            filters["account_id"] = parts[1]
            filters["campaign_id"] = parts[2]
            filters["adset_id"] = parts[3]
        elif len(parts) == 1:
            filters["adset_id"] = parts[0]
    else:
        return {"rows": [], "child_tab": "", "error": "Invalid parent_tab"}

    # Query touchpoints for children
    from attributionops.db import query as _query, sql_rows
    from attributionops.util import to_float, parse_iso_ts
    from collections import defaultdict

    # Build WHERE clause for touchpoints
    where_parts = ["date(substr(t.ts,1,10)) BETWEEN :start_date AND :end_date"]
    params: dict[str, Any] = {"start_date": start_date, "end_date": end_date}
    for k, v in filters.items():
        if v:
            where_parts.append(f"t.{k} = :{k}")
            params[k] = v

    where_sql = " AND ".join(where_parts)

    # Determine grouping column for children
    if child_tab == "campaign":
        group_col = "t.campaign_id"
        name_col = "t.campaign_id"
        child_id_template = "t.platform || '|' || '' || '|' || t.campaign_id"
    elif child_tab == "ad_set":
        group_col = "t.adset_id"
        name_col = "t.adset_id"
        child_id_template = "t.platform || '|' || '' || '|' || t.campaign_id || '|' || t.adset_id"
    elif child_tab == "ad":
        group_col = "t.ad_id"
        name_col = "t.ad_id"
        child_id_template = "t.platform || '|' || '' || '|' || t.campaign_id || '|' || t.adset_id || '|' || t.ad_id"
    else:
        return {"rows": [], "child_tab": child_tab}

    # Get touchpoint sessions grouped by child
    tp_sql = f"""
        SELECT {group_col} as child_key,
               {child_id_template} as child_id,
               t.platform, t.campaign_id, t.adset_id, t.ad_id,
               COUNT(DISTINCT t.session_id) as sessions,
               COUNT(*) as touchpoint_count
        FROM touchpoints t
        WHERE {where_sql}
          AND COALESCE({group_col}, '') <> ''
        GROUP BY {group_col}
        ORDER BY touchpoint_count DESC
    """
    tp_rows = sql_rows(db_path, tp_sql, params)

    # Get attributed revenue per child
    # We need to run attribution and filter by the child dimension
    try:
        attrib_result = attribution_run(
            db_path,
            model=model,
            start_date=start_date,
            end_date=end_date,
            lookback_days=lookback_days,
            conversion_type=conversion_type,
            value_type="revenue",
            date_basis=date_basis,
        )
        attrib_rows = attrib_result.get("rows", [])
    except Exception:
        attrib_rows = []

    # Build attribution lookup by child key
    attrib_by_child: dict[str, dict[str, float]] = defaultdict(lambda: {
        "total_revenue": 0.0, "revenue": 0.0, "orders": 0.0, "cogs": 0.0, "fees": 0.0
    })
    for ar in attrib_rows:
        # Filter attribution rows to match parent filters
        match = True
        for k, v in filters.items():
            if v and str(ar.get(k, "")) != v:
                match = False
                break
        if not match:
            continue

        if child_tab == "campaign":
            ck = str(ar.get("campaign_id", ""))
        elif child_tab == "ad_set":
            ck = str(ar.get("adset_id", ""))
        elif child_tab == "ad":
            ck = str(ar.get("ad_id", ""))
        else:
            continue

        if ck:
            attrib_by_child[ck]["total_revenue"] += to_float(ar.get("total_revenue"))
            attrib_by_child[ck]["revenue"] += to_float(ar.get("revenue"))
            attrib_by_child[ck]["orders"] += float(str(ar.get("orders") or "0") or 0)
            attrib_by_child[ck]["cogs"] += to_float(ar.get("cogs"))
            attrib_by_child[ck]["fees"] += to_float(ar.get("fees"))

    # Build response rows
    rows = []
    for tp in tp_rows:
        ck = str(tp.get("child_key", ""))
        if not ck:
            continue
        attrib = attrib_by_child.get(ck, {})
        revenue = round(attrib.get("revenue", 0.0), 2)
        total_revenue = round(attrib.get("total_revenue", 0.0), 2)
        cogs = round(attrib.get("cogs", 0.0), 2)
        fees = round(attrib.get("fees", 0.0), 2)
        cost = 0.0  # No spend data in real tracking mode
        profit = round(revenue - cost - cogs - fees, 2)

        has_grandchildren = child_tab in ("campaign", "ad_set")

        rows.append({
            "id": str(tp.get("child_id", ck)),
            "name": ck if ck else "(empty)",
            "level": child_tab,
            "metrics": {
                "clicks": int(tp.get("sessions", 0)),
                "cost": cost,
                "total_revenue": total_revenue,
                "revenue": revenue,
                "profit": profit,
                "net_profit": profit,
                "reported": None,
                "reported_delta": None,
            },
            "children_available": has_grandchildren,
            "children_count": None,
        })

    rows.sort(key=lambda r: r["metrics"]["revenue"], reverse=True)

    return {
        "child_tab": child_tab,
        "parent_tab": parent_tab,
        "parent_id": parent_id,
        "rows": rows,
    }


@app.get("/api/spend")
async def get_spend(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    platform: str = Query(default="all"),
    breakdown: str = Query(default="campaign"),
):
    s, e = _default_dates()
    return ads_get_spend(
        _db(),
        platform=platform,
        start_date=start_date or s,
        end_date=end_date or e,
        breakdown=breakdown,
    )


@app.get("/api/attribution")
async def get_attribution(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    model: str = Query(default="last_click"),
    lookback_days: int = Query(default=30),
    conversion_type: str = Query(default="Purchase"),
    value_type: str = Query(default="revenue"),
):
    s, e = _default_dates()
    return attribution_run(
        _db(),
        model=model,
        start_date=start_date or s,
        end_date=end_date or e,
        lookback_days=lookback_days,
        conversion_type=conversion_type,
        value_type=value_type,
    )


@app.get("/api/platforms")
async def get_platforms():
    return ads_list_platforms(_db())


@app.get("/api/integrations")
async def get_integrations():
    return integrations_status(_db())


@app.get("/api/tracking")
async def get_tracking():
    return tracking_health_check(_db())


@app.get("/api/forecasting")
async def get_forecasting(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    cohort_window: int = Query(default=30),
    method: str = Query(default="simple"),
):
    s, e = _default_dates()
    return forecasting_run(
        _db(),
        start_date=start_date or s,
        end_date=end_date or e,
        cohort_window=cohort_window,
        method=method,
    )


@app.get("/api/reported")
async def get_reported(
    start_date: str = Query(default=""),
    end_date: str = Query(default=""),
    platform: str = Query(default="all"),
    breakdown: str = Query(default="campaign"),
    conversion_type: str = Query(default="Purchase"),
):
    s, e = _default_dates()
    return ads_get_reported_value(
        _db(),
        platform=platform,
        start_date=start_date or s,
        end_date=end_date or e,
        breakdown=breakdown,
        conversion_type=conversion_type,
    )


# ── Tracking pixel script serving ──────────────────────────────────────────────

STATIC_DIR = Path(__file__).resolve().parent / "static"

@app.get("/t/hyros.js")
async def serve_tracking_script():
    """Serve the tracking pixel JavaScript file."""
    js_path = STATIC_DIR / "hyros.js"
    if not js_path.exists():
        raise HTTPException(status_code=404, detail="Tracking script not found")
    return FileResponse(
        js_path,
        media_type="application/javascript",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@app.get("/t/hyros-ghl.js")
async def serve_ghl_script():
    """Serve the GHL-specific tracking extension."""
    js_path = STATIC_DIR / "hyros-ghl.js"
    if not js_path.exists():
        raise HTTPException(status_code=404, detail="GHL script not found")
    return FileResponse(
        js_path,
        media_type="application/javascript",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@app.get("/t/setup")
async def tracking_setup_page():
    """Serve an HTML page with copy-paste tracking script instructions."""
    host = os.environ.get("TRACKING_DOMAIN", "http://localhost:8000")
    token = os.environ.get("SITE_TOKEN", "your-site-token")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Mini Hyros — GHL Tracking Setup</title>
  <style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{ background: #0a0a0f; color: #e0e0e5; font-family: system-ui, -apple-system, sans-serif; padding: 2rem; max-width: 900px; margin: auto; }}
    h1 {{ font-size: 1.5rem; margin-bottom: 0.5rem; color: #fff; }}
    h2 {{ font-size: 1.1rem; margin-top: 2rem; margin-bottom: 0.75rem; color: #a5b4fc; }}
    h3 {{ font-size: 0.95rem; margin-top: 1.5rem; margin-bottom: 0.5rem; color: #e0e0e5; }}
    p {{ font-size: 0.9rem; color: #888; margin-bottom: 0.75rem; line-height: 1.6; }}
    .badge {{ display: inline-block; background: #6366f1; color: #fff; font-size: 0.7rem; padding: 2px 8px; border-radius: 4px; font-weight: 600; margin-left: 8px; }}
    .badge.ghl {{ background: #f97316; }}
    .badge.meta {{ background: #1877f2; }}
    .badge.google {{ background: #34a853; }}
    .badge.tiktok {{ background: #010101; border: 1px solid #333; }}
    pre {{ background: #111118; border: 1px solid #1e1e2e; border-radius: 8px; padding: 1rem; overflow-x: auto; font-size: 0.82rem; line-height: 1.7; position: relative; }}
    code {{ color: #c4b5fd; }}
    .copy-btn {{ position: absolute; top: 8px; right: 8px; background: #6366f1; color: #fff; border: none; padding: 4px 12px; border-radius: 4px; cursor: pointer; font-size: 0.7rem; }}
    .copy-btn:hover {{ background: #4f46e5; }}
    .section {{ background: #111118; border: 1px solid #1e1e2e; border-radius: 12px; padding: 1.5rem; margin-bottom: 1.5rem; }}
    .step {{ display: flex; gap: 12px; margin-bottom: 1rem; }}
    .step-num {{ background: #f97316; color: #fff; min-width: 28px; height: 28px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 0.8rem; font-weight: 700; flex-shrink: 0; }}
    .step-content {{ flex: 1; font-size: 0.9rem; line-height: 1.6; }}
    .tag {{ display: inline-block; background: #22c55e20; color: #22c55e; font-size: 0.7rem; padding: 2px 8px; border-radius: 4px; margin-right: 4px; }}
    .tag.warn {{ background: #eab30820; color: #eab308; }}
    .tag.ghl {{ background: #f9731620; color: #f97316; }}
    hr {{ border: none; border-top: 1px solid #1e1e2e; margin: 2rem 0; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; margin-top: 0.5rem; }}
    th {{ text-align: left; color: #a5b4fc; padding: 8px; border-bottom: 1px solid #1e1e2e; }}
    td {{ padding: 8px; border-bottom: 1px solid #1e1e2e; color: #ccc; }}
    td code {{ color: #c4b5fd; font-size: 0.8rem; }}
    .platform-card {{ background: #111118; border: 1px solid #1e1e2e; border-radius: 12px; padding: 1.25rem; margin-bottom: 1rem; }}
    .platform-card h3 {{ margin-top: 0; }}
    .param-table {{ width: 100%; font-size: 0.82rem; margin-top: 0.5rem; }}
    .param-table td {{ padding: 4px 8px; }}
    .param-table td:first-child {{ color: #c4b5fd; font-family: monospace; white-space: nowrap; }}
    .nav {{ display: flex; gap: 8px; margin-bottom: 1.5rem; flex-wrap: wrap; }}
    .nav a {{ background: #1e1e2e; color: #a5b4fc; text-decoration: none; padding: 6px 14px; border-radius: 6px; font-size: 0.8rem; }}
    .nav a:hover {{ background: #2e2e3e; }}
  </style>
</head>
<body>

<h1>Mini Hyros — GHL Tracking Setup <span class="badge ghl">GoHighLevel</span></h1>
<p>Complete guide to set up tracking on your GHL funnels with your custom UTM parameters for Meta, TikTok, and Google Ads.</p>

<div class="nav">
  <a href="#pixel">1. Tracking Pixel</a>
  <a href="#utms">2. Ad UTM Links</a>
  <a href="#webhooks">3. GHL Webhooks</a>
  <a href="#identify">4. Identify &amp; Conversions</a>
  <a href="#how">How It Works</a>
</div>

<hr id="pixel" />

<h2>1. Add Tracking Pixel to GHL Funnels <span class="badge">REQUIRED</span></h2>
<p>In GHL, go to <strong>Sites &rarr; Funnels &rarr; [Your Funnel] &rarr; Settings &rarr; Custom Code &rarr; Head Tracking Code</strong> and paste this:</p>

<div class="section">
<pre id="snippet-main"><code>&lt;!-- Mini Hyros Tracking Pixel --&gt;
&lt;script src="{host}/t/hyros.js" data-token="{token}" data-endpoint="{host}"&gt;&lt;/script&gt;</code></pre>
<button class="copy-btn" onclick="copySnippet('snippet-main')">Copy</button>
</div>

<p><span class="tag ghl">GHL Path</span> Sites &rarr; Funnels &rarr; [Funnel] &rarr; Settings (gear icon) &rarr; Custom Code &rarr; <strong>Header Code</strong></p>
<p><span class="tag">Note</span> This goes on the <strong>funnel level</strong> so it applies to ALL pages in that funnel. Repeat for each funnel.</p>

<h3>For GHL Websites (not funnels)</h3>
<p>Go to <strong>Sites &rarr; Websites &rarr; [Your Website] &rarr; Settings &rarr; Custom Code &rarr; Head Tracking Code</strong> and paste the same snippet.</p>

<hr id="utms" />

<h2>2. Ad Platform UTM Links <span class="badge">REQUIRED</span></h2>
<p>Use these <strong>exact URL templates</strong> as your ad destination URLs. The pixel auto-detects the platform from these parameters.</p>

<div class="platform-card">
<h3><span class="badge meta">META</span> Facebook / Instagram Ads</h3>
<p>In your Meta ad's <strong>Website URL</strong> or <strong>URL Parameters</strong> field, append:</p>
<pre id="snippet-utm-meta"><code>?fbc_id={{{{adset.id}}}}&amp;h_ad_id={{{{ad.id}}}}</code></pre>
<button class="copy-btn" onclick="copySnippet('snippet-utm-meta')">Copy</button>
<p style="margin-top:0.75rem; font-size:0.82rem;">Full example:</p>
<pre id="snippet-utm-meta-full"><code>https://yourfunnel.com/page?fbc_id={{{{adset.id}}}}&amp;h_ad_id={{{{ad.id}}}}</code></pre>
<button class="copy-btn" onclick="copySnippet('snippet-utm-meta-full')">Copy</button>
<table class="param-table">
  <tr><td>fbc_id</td><td>&rarr; Captures the <strong>Ad Set ID</strong> from Meta</td></tr>
  <tr><td>h_ad_id</td><td>&rarr; Captures the <strong>Ad ID</strong> from Meta</td></tr>
</table>
</div>

<div class="platform-card">
<h3><span class="badge tiktok">TIKTOK</span> TikTok Ads</h3>
<p>In your TikTok ad's <strong>Destination URL</strong>, append:</p>
<pre id="snippet-utm-tiktok"><code>?ttc_id=__AID__&amp;ttclid=__CLICKID__&amp;h_ad_id=__CID__</code></pre>
<button class="copy-btn" onclick="copySnippet('snippet-utm-tiktok')">Copy</button>
<p style="margin-top:0.75rem; font-size:0.82rem;">Full example:</p>
<pre id="snippet-utm-tiktok-full"><code>https://yourfunnel.com/page?ttc_id=__AID__&amp;ttclid=__CLICKID__&amp;h_ad_id=__CID__</code></pre>
<button class="copy-btn" onclick="copySnippet('snippet-utm-tiktok-full')">Copy</button>
<table class="param-table">
  <tr><td>ttc_id</td><td>&rarr; Captures the <strong>Advertiser ID</strong> from TikTok</td></tr>
  <tr><td>ttclid</td><td>&rarr; Captures the <strong>Click ID</strong> from TikTok</td></tr>
  <tr><td>h_ad_id</td><td>&rarr; Captures the <strong>Campaign ID</strong> from TikTok</td></tr>
</table>
</div>

<div class="platform-card">
<h3><span class="badge google">GOOGLE</span> Google Ads</h3>
<p>In your Google ad's <strong>Final URL suffix</strong> or <strong>Tracking template</strong>, use:</p>
<pre id="snippet-utm-google"><code>{{lpurl}}?gc_id={{campaignid}}&amp;g_special_campaign=true</code></pre>
<button class="copy-btn" onclick="copySnippet('snippet-utm-google')">Copy</button>
<p style="margin-top:0.75rem; font-size:0.82rem;">Full example:</p>
<pre id="snippet-utm-google-full"><code>https://yourfunnel.com/page?gc_id={{campaignid}}&amp;g_special_campaign=true</code></pre>
<button class="copy-btn" onclick="copySnippet('snippet-utm-google-full')">Copy</button>
<table class="param-table">
  <tr><td>gc_id</td><td>&rarr; Captures the <strong>Campaign ID</strong> from Google</td></tr>
  <tr><td>g_special_campaign</td><td>&rarr; Flags this as a <strong>Google Ads</strong> visit</td></tr>
</table>
<p style="margin-top:0.5rem;"><span class="tag warn">Google Tracking Template</span> If using a tracking template, set it to: <code>{{lpurl}}?gc_id={{campaignid}}&amp;g_special_campaign=true</code></p>
</div>

<hr id="webhooks" />

<h2>3. Set Up GHL Webhooks <span class="badge ghl">GHL</span></h2>
<p>This sends form submissions, bookings, and payments from GHL directly to your backend for attribution.</p>

<h3>Create Workflow Webhooks</h3>
<div class="section">
  <div class="step"><div class="step-num">1</div><div class="step-content">In GHL, go to <strong>Automation &rarr; Workflows &rarr; Create Workflow</strong></div></div>
  <div class="step"><div class="step-num">2</div><div class="step-content">Set trigger to the event you want to track (see table below)</div></div>
  <div class="step"><div class="step-num">3</div><div class="step-content">Add action: <strong>Webhook (Custom Webhook)</strong></div></div>
  <div class="step"><div class="step-num">4</div><div class="step-content">Set Method: <strong>POST</strong></div></div>
  <div class="step"><div class="step-num">5</div><div class="step-content">Set URL to the webhook endpoint below</div></div>
</div>

<p><strong>Webhook URL:</strong></p>
<div class="section">
<pre id="snippet-webhook"><code>{host}/api/webhooks/ghl</code></pre>
<button class="copy-btn" onclick="copySnippet('snippet-webhook')">Copy</button>
</div>

<h3>Create these workflows:</h3>
<table>
  <tr><th>Workflow Name</th><th>Trigger</th><th>What it tracks</th></tr>
  <tr><td>Track Form Leads</td><td><code>Form Submitted</code></td><td>Lead conversions</td></tr>
  <tr><td>Track Bookings</td><td><code>Appointment Status Changed &rarr; Confirmed</code></td><td>Booking conversions</td></tr>
  <tr><td>Track Payments</td><td><code>Payment Received</code></td><td>Revenue / orders</td></tr>
  <tr><td>Track Pipeline</td><td><code>Opportunity Stage Changed</code></td><td>Pipeline movement + won deals</td></tr>
  <tr><td>Track New Contacts</td><td><code>Contact Created</code></td><td>Identity stitching</td></tr>
</table>

<p style="margin-top: 1rem;"><span class="tag warn">Important</span> In each workflow's webhook action, include <strong>all contact fields</strong> in the body &mdash; especially <code>email</code>, <code>firstName</code>, <code>lastName</code>, <code>phone</code>.</p>

<hr id="identify" />

<h2>4. Identify &amp; Conversion Scripts <span class="badge">THANK YOU PAGE</span></h2>
<p>Add this to your GHL funnel's <strong>thank-you / confirmation page</strong> custom code to identify the visitor and track the conversion.</p>

<h3>Identify (link email to visitor)</h3>
<p>GHL passes contact data via URL params or hidden fields. This script auto-grabs the email:</p>
<div class="section">
<pre id="snippet-identify"><code>&lt;script&gt;
  // Auto-detect email from GHL URL params, form fields, or cookies
  var email = new URLSearchParams(window.location.search).get("email")
           || new URLSearchParams(window.location.search).get("contact_email")
           || document.querySelector('input[name="email"]')?.value
           || "";
  if (email) hyros.identify(email);
&lt;/script&gt;</code></pre>
<button class="copy-btn" onclick="copySnippet('snippet-identify')">Copy</button>
</div>

<h3>Track a Purchase / Conversion <span class="tag warn">Dynamic Values</span></h3>
<p>This script reads the order amount from GHL URL params (<code>?amount=</code>) or a data attribute you set. No hardcoded prices.</p>
<div class="section">
<pre id="snippet-conversion"><code>&lt;script&gt;
  // Read dynamic values from URL params (GHL passes these on redirect)
  var params = new URLSearchParams(window.location.search);
  var amount = parseFloat(params.get("amount") || params.get("value") || params.get("total") || "0");
  var orderId = params.get("order_id") || params.get("invoice_id") || ("order-" + Date.now());
  var email   = params.get("email") || params.get("contact_email") || "";

  if (email) hyros.identify(email);

  hyros.conversion({{
    type: "Purchase",
    value: amount,                  // pulled from URL ?amount=XX
    order_id: orderId,
    email: email
  }});
&lt;/script&gt;</code></pre>
<button class="copy-btn" onclick="copySnippet('snippet-conversion')">Copy</button>
</div>

<p><span class="tag ghl">GHL Tip</span> In your GHL workflow, when redirecting to the thank-you page, append the amount to the URL:</p>
<div class="section">
<pre id="snippet-ghl-redirect"><code>https://yourfunnel.com/thank-you?amount={{{{contact.last_payment_amount}}}}&amp;email={{{{contact.email}}}}&amp;order_id={{{{contact.last_payment_id}}}}</code></pre>
<button class="copy-btn" onclick="copySnippet('snippet-ghl-redirect')">Copy</button>
</div>

<p><span class="tag">Alternative</span> If you know the fixed price for a specific funnel, you can hardcode it:</p>
<div class="section">
<pre id="snippet-conversion-fixed"><code>&lt;script&gt;
  hyros.conversion({{ type: "Purchase", value: 997 }});
&lt;/script&gt;</code></pre>
<button class="copy-btn" onclick="copySnippet('snippet-conversion-fixed')">Copy</button>
</div>

<h3>Custom Events <span class="badge">OPTIONAL</span></h3>
<div class="section">
<pre id="snippet-event"><code>&lt;script&gt;
  // Track a lead from a form submission
  hyros.event("Lead", {{ form: document.title || "form" }});

  // Track with a dynamic value from the page
  var price = document.querySelector('[data-price]')?.getAttribute('data-price');
  if (price) hyros.event("AddToCart", {{ value: parseFloat(price) }});
&lt;/script&gt;</code></pre>
<button class="copy-btn" onclick="copySnippet('snippet-event')">Copy</button>
</div>

<hr id="how" />

<h2>How the GHL + Custom UTM Flow Works</h2>
<div class="section">
  <div class="step"><div class="step-num">1</div><div class="step-content"><strong>Visitor clicks your ad</strong> &mdash; lands on your GHL funnel with custom params (e.g. <code>fbc_id</code>, <code>gc_id</code>, <code>ttclid</code>). The pixel auto-detects the platform and captures the ad/adset/campaign IDs.</div></div>
  <div class="step"><div class="step-num">2</div><div class="step-content"><strong>Pageview is recorded</strong> &mdash; a session + touchpoint are stored with the correct platform, channel, and ad hierarchy.</div></div>
  <div class="step"><div class="step-num">3</div><div class="step-content"><strong>Visitor fills a GHL form</strong> &mdash; the email is captured. If you added <code>hyros.identify()</code>, all past anonymous sessions are linked to that customer.</div></div>
  <div class="step"><div class="step-num">4</div><div class="step-content"><strong>GHL workflow fires webhook</strong> &mdash; form submission / booking / payment data is sent to <code>{host}/api/webhooks/ghl</code> and recorded as a conversion.</div></div>
  <div class="step"><div class="step-num">5</div><div class="step-content"><strong>Attribution engine connects the chain</strong> &mdash; ad click &rarr; form fill &rarr; payment, all linked via the customer's email hash. Dashboard updates in real-time.</div></div>
</div>

<hr />

<h2>UTM Parameter Reference</h2>
<table>
  <tr><th>Platform</th><th>Parameter</th><th>Maps To</th><th>Description</th></tr>
  <tr><td><span class="badge meta">META</span></td><td><code>fbc_id</code></td><td>Ad Set ID</td><td>Meta adset.id</td></tr>
  <tr><td><span class="badge meta">META</span></td><td><code>h_ad_id</code></td><td>Ad ID</td><td>Meta ad.id</td></tr>
  <tr><td><span class="badge tiktok">TIKTOK</span></td><td><code>ttc_id</code></td><td>Advertiser ID</td><td>TikTok __AID__</td></tr>
  <tr><td><span class="badge tiktok">TIKTOK</span></td><td><code>ttclid</code></td><td>Click ID</td><td>TikTok __CLICKID__</td></tr>
  <tr><td><span class="badge tiktok">TIKTOK</span></td><td><code>h_ad_id</code></td><td>Campaign ID</td><td>TikTok __CID__</td></tr>
  <tr><td><span class="badge google">GOOGLE</span></td><td><code>gc_id</code></td><td>Campaign ID</td><td>Google {{campaignid}}</td></tr>
  <tr><td><span class="badge google">GOOGLE</span></td><td><code>g_special_campaign</code></td><td>Flag</td><td>Marks visit as Google Ads</td></tr>
</table>

<hr />

<h2>GHL Event Types We Track</h2>
<table>
  <tr><th>Event</th><th>Conversion Type</th><th>Revenue</th></tr>
  <tr><td>Form Submitted</td><td><code>Lead</code></td><td>$0 (lead value)</td></tr>
  <tr><td>Appointment Booked</td><td><code>Booking</code></td><td>$0 (booking value)</td></tr>
  <tr><td>Opportunity Won</td><td><code>Purchase</code></td><td>Opportunity value</td></tr>
  <tr><td>Payment Received</td><td><code>Purchase</code></td><td>Payment amount</td></tr>
  <tr><td>Contact Created</td><td>Identity only</td><td>&mdash;</td></tr>
</table>

<hr />
<p style="color:#555; font-size:0.8rem; text-align:center; margin-top:2rem;">
  Mini Hyros &mdash; GHL Attribution Tracking<br>
  Backend: <code>{host}</code> | Pixel: <code>{host}/t/hyros.js</code> | Webhook: <code>{host}/api/webhooks/ghl</code><br>
  <a href="{host}/t/ghl-setup" style="color:#6366f1;">Detailed GHL Setup Guide</a>
</p>

<script>
function copySnippet(id) {{
  var el = document.getElementById(id);
  var text = el.innerText || el.textContent;
  navigator.clipboard.writeText(text).then(function() {{
    var btn = el.parentElement.querySelector('.copy-btn');
    btn.textContent = 'Copied!';
    setTimeout(function() {{ btn.textContent = 'Copy'; }}, 1500);
  }});
}}
</script>
</body>
</html>"""
    return HTMLResponse(content=html)


@app.get("/t/ghl-setup")
async def ghl_setup_page():
    """GHL-specific setup instructions page."""
    host = os.environ.get("TRACKING_DOMAIN", "http://localhost:8000")
    token = os.environ.get("SITE_TOKEN", "your-site-token")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Mini Hyros — GoHighLevel Setup</title>
  <style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{ background: #0a0a0f; color: #e0e0e5; font-family: system-ui, -apple-system, sans-serif; padding: 2rem; max-width: 900px; margin: auto; }}
    h1 {{ font-size: 1.5rem; margin-bottom: 0.5rem; color: #fff; }}
    h2 {{ font-size: 1.1rem; margin-top: 2rem; margin-bottom: 0.75rem; color: #a5b4fc; }}
    h3 {{ font-size: 0.95rem; margin-top: 1.5rem; margin-bottom: 0.5rem; color: #e0e0e5; }}
    p {{ font-size: 0.9rem; color: #888; margin-bottom: 0.75rem; line-height: 1.6; }}
    .badge {{ display: inline-block; background: #6366f1; color: #fff; font-size: 0.7rem; padding: 2px 8px; border-radius: 4px; font-weight: 600; margin-left: 8px; }}
    .badge.ghl {{ background: #f97316; }}
    pre {{ background: #111118; border: 1px solid #1e1e2e; border-radius: 8px; padding: 1rem; overflow-x: auto; font-size: 0.82rem; line-height: 1.7; position: relative; }}
    code {{ color: #c4b5fd; }}
    .copy-btn {{ position: absolute; top: 8px; right: 8px; background: #6366f1; color: #fff; border: none; padding: 4px 12px; border-radius: 4px; cursor: pointer; font-size: 0.7rem; }}
    .copy-btn:hover {{ background: #4f46e5; }}
    .section {{ background: #111118; border: 1px solid #1e1e2e; border-radius: 12px; padding: 1.5rem; margin-bottom: 1.5rem; }}
    .step {{ display: flex; gap: 12px; margin-bottom: 1rem; }}
    .step-num {{ background: #f97316; color: #fff; min-width: 28px; height: 28px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 0.8rem; font-weight: 700; flex-shrink: 0; }}
    .step-content {{ flex: 1; }}
    .tag {{ display: inline-block; background: #22c55e20; color: #22c55e; font-size: 0.7rem; padding: 2px 8px; border-radius: 4px; margin-right: 4px; }}
    .tag.warn {{ background: #eab30820; color: #eab308; }}
    .tag.ghl {{ background: #f9731620; color: #f97316; }}
    hr {{ border: none; border-top: 1px solid #1e1e2e; margin: 2rem 0; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; margin-top: 0.5rem; }}
    th {{ text-align: left; color: #a5b4fc; padding: 8px; border-bottom: 1px solid #1e1e2e; }}
    td {{ padding: 8px; border-bottom: 1px solid #1e1e2e; color: #ccc; }}
    td code {{ color: #c4b5fd; font-size: 0.8rem; }}
  </style>
</head>
<body>

<h1>Mini Hyros — GoHighLevel Setup <span class="badge ghl">GHL</span></h1>
<p>Complete guide to track visitors, leads, bookings, and payments from your GHL funnels.</p>

<hr />

<h2>Step 1: Add Tracking Pixel to ALL Funnel Pages <span class="badge">REQUIRED</span></h2>
<p>In GHL, go to <strong>Sites → Funnels → [Your Funnel] → Settings → Custom Code → Head Tracking Code</strong> and paste this:</p>

<div class="section">
<pre id="ghl-pixel"><code>&lt;!-- Mini Hyros Tracking Pixel --&gt;
&lt;script src="{host}/t/hyros.js" data-token="{token}" data-endpoint="{host}"&gt;&lt;/script&gt;
&lt;script src="{host}/t/hyros-ghl.js"&gt;&lt;/script&gt;</code></pre>
<button class="copy-btn" onclick="copySnippet('ghl-pixel')">Copy</button>
</div>

<p><span class="tag ghl">GHL Path</span> Sites → Funnels → [Funnel] → Settings (gear icon) → Custom Code → <strong>Header Code</strong></p>
<p><span class="tag">Note</span> This goes on the <strong>funnel level</strong> so it applies to ALL pages in that funnel. Repeat for each funnel you want to track.</p>

<h3>For GHL Websites (not funnels)</h3>
<p>Go to <strong>Sites → Websites → [Your Website] → Settings → Custom Code → Head Tracking Code</strong> and paste the same snippet above.</p>

<hr />

<h2>Step 2: Set Up GHL Webhooks <span class="badge">REQUIRED</span></h2>
<p>This sends form submissions, bookings, and payments from GHL directly to your backend for attribution.</p>

<h3>Option A: GHL Workflow Webhooks (Recommended)</h3>
<div class="section">
  <div class="step"><div class="step-num">1</div><div class="step-content">In GHL, go to <strong>Automation → Workflows → Create Workflow</strong></div></div>
  <div class="step"><div class="step-num">2</div><div class="step-content">Set trigger to the event you want to track (e.g., <strong>Form Submitted</strong>, <strong>Appointment Booked</strong>, <strong>Payment Received</strong>, <strong>Opportunity Stage Changed</strong>)</div></div>
  <div class="step"><div class="step-num">3</div><div class="step-content">Add action: <strong>Webhook (Custom Webhook)</strong></div></div>
  <div class="step"><div class="step-num">4</div><div class="step-content">Set Method: <strong>POST</strong></div></div>
  <div class="step"><div class="step-num">5</div><div class="step-content">Set URL to the webhook endpoint below</div></div>
</div>

<p><strong>Webhook URL:</strong></p>
<div class="section">
<pre id="ghl-webhook-url"><code>{host}/api/webhooks/ghl</code></pre>
<button class="copy-btn" onclick="copySnippet('ghl-webhook-url')">Copy</button>
</div>

<h3>Create these workflows:</h3>
<table>
  <tr><th>Workflow Name</th><th>Trigger</th><th>What it tracks</th></tr>
  <tr><td>Track Form Leads</td><td><code>Form Submitted</code></td><td>Lead conversions</td></tr>
  <tr><td>Track Bookings</td><td><code>Appointment Status Changed → Confirmed</code></td><td>Booking conversions</td></tr>
  <tr><td>Track Payments</td><td><code>Payment Received</code></td><td>Revenue / orders</td></tr>
  <tr><td>Track Pipeline</td><td><code>Opportunity Stage Changed</code></td><td>Pipeline movement + won deals</td></tr>
  <tr><td>Track New Contacts</td><td><code>Contact Created</code></td><td>Identity stitching</td></tr>
</table>

<p style="margin-top: 1rem;"><span class="tag warn">Important</span> In each workflow's webhook action, make sure to include <strong>all contact fields</strong> in the body — especially <code>email</code>, <code>firstName</code>, <code>lastName</code>, <code>phone</code>.</p>

<h3>Option B: GHL API Webhooks (Advanced)</h3>
<p>If you have GHL API access, go to <strong>Settings → Webhooks</strong> and add:</p>
<div class="section">
<pre id="ghl-api-webhook"><code>Webhook URL: {host}/api/webhooks/ghl
Events: Contact Create, Contact Update, Opportunity Create,
        Opportunity Stage Update, Payment Received, Invoice Paid,
        Form Submission, Appointment Booked</code></pre>
<button class="copy-btn" onclick="copySnippet('ghl-api-webhook')">Copy</button>
</div>

<hr />

<h2>Step 3: UTM Links for Your Ads <span class="badge">REQUIRED</span></h2>
<p>Use these URL templates in your ad platform destination URLs. Replace <code>yourfunnel.com</code> with your GHL funnel domain.</p>

<div class="section">
<pre id="ghl-utms"><code>Facebook / Meta Ads:
  https://yourfunnel.com/page?utm_source=facebook&amp;utm_medium=paid_social&amp;utm_campaign={{{{campaign.name}}}}&amp;utm_content={{{{ad.name}}}}&amp;fbclid={{{{fbclid}}}}

Google Ads:
  https://yourfunnel.com/page?utm_source=google&amp;utm_medium=cpc&amp;utm_campaign={{{{campaignname}}}}&amp;utm_content={{{{creative}}}}&amp;gclid={{{{gclid}}}}

TikTok Ads:
  https://yourfunnel.com/page?utm_source=tiktok&amp;utm_medium=paid_social&amp;utm_campaign=__CAMPAIGN_NAME__&amp;utm_content=__CID__&amp;ttclid=__CLICKID__

YouTube Ads:
  https://yourfunnel.com/page?utm_source=youtube&amp;utm_medium=paid_video&amp;utm_campaign={{{{campaignname}}}}&amp;gclid={{{{gclid}}}}</code></pre>
<button class="copy-btn" onclick="copySnippet('ghl-utms')">Copy</button>
</div>

<hr />

<h2>Step 4 (Optional): Thank-You Page Conversion <span class="badge ghl">GHL</span></h2>
<p>The GHL extension script auto-detects thank-you pages, but for explicit tracking with order values, add this to your GHL funnel's thank-you page custom code:</p>

<div class="section">
<pre id="ghl-thankyou"><code>&lt;script&gt;
  // Fire on your thank-you / order confirmation page
  hyros.conversion({{
    type: "Purchase",
    value: 997,                    // order amount
    order_id: "order-" + Date.now(),
    email: ""                      // leave empty if auto-detected by form
  }});
&lt;/script&gt;</code></pre>
<button class="copy-btn" onclick="copySnippet('ghl-thankyou')">Copy</button>
</div>

<p><span class="tag">Tip</span> The <code>hyros-ghl.js</code> script auto-captures emails from GHL form submissions, so if the visitor filled out a form earlier on the funnel, their email is already linked.</p>

<hr />

<h2>How the GHL Flow Works</h2>
<div class="section">
  <div class="step"><div class="step-num">1</div><div class="step-content"><strong>Visitor clicks your ad</strong> — lands on GHL funnel with UTM params. Pixel captures session + click IDs (fbclid/gclid/ttclid).</div></div>
  <div class="step"><div class="step-num">2</div><div class="step-content"><strong>Visitor fills GHL form</strong> — <code>hyros-ghl.js</code> auto-captures email and fires <code>hyros.identify()</code> + Lead conversion.</div></div>
  <div class="step"><div class="step-num">3</div><div class="step-content"><strong>GHL workflow fires webhook</strong> — form submission / booking / payment data sent to <code>/api/webhooks/ghl</code>.</div></div>
  <div class="step"><div class="step-num">4</div><div class="step-content"><strong>Attribution engine links it all</strong> — the ad click → form fill → payment chain is connected via the customer's email hash.</div></div>
  <div class="step"><div class="step-num">5</div><div class="step-content"><strong>Dashboard shows real data</strong> — you see exactly which ad brought which lead and how much revenue it generated.</div></div>
</div>

<hr />

<h2>GHL Event Types We Track</h2>
<table>
  <tr><th>Event</th><th>Conversion Type</th><th>Revenue</th></tr>
  <tr><td>Form Submitted</td><td><code>Lead</code></td><td>$0 (lead value)</td></tr>
  <tr><td>Appointment Booked</td><td><code>Booking</code></td><td>$0 (booking value)</td></tr>
  <tr><td>Opportunity Won</td><td><code>Purchase</code></td><td>Opportunity value</td></tr>
  <tr><td>Payment Received</td><td><code>Purchase</code></td><td>Payment amount</td></tr>
  <tr><td>Contact Created</td><td>Identity only</td><td>—</td></tr>
</table>

<hr />
<p style="color:#555; font-size:0.8rem; text-align:center; margin-top:2rem;">
  Mini Hyros — GoHighLevel Integration<br>
  Webhook: <code>{host}/api/webhooks/ghl</code> | Pixel: <code>{host}/t/hyros.js</code> | GHL Extension: <code>{host}/t/hyros-ghl.js</code>
</p>

<script>
function copySnippet(id) {{
  var el = document.getElementById(id);
  var text = el.innerText || el.textContent;
  navigator.clipboard.writeText(text).then(function() {{
    var btn = el.parentElement.querySelector('.copy-btn');
    btn.textContent = 'Copied!';
    setTimeout(function() {{ btn.textContent = 'Copy'; }}, 1500);
  }});
}}
</script>
</body>
</html>"""
    return HTMLResponse(content=html)


# ── WebSocket for real-time updates ───────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            # Keep connection alive, client can send pings
            data = await ws.receive_text()
            if data == "ping":
                await ws.send_json({"type": "pong"})
    except WebSocketDisconnect:
        manager.disconnect(ws)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
