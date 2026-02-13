/**
 * Mini Hyros Tracking Pixel
 * 
 * Paste this on EVERY page of your site:
 *   <script src="https://YOUR_DOMAIN/t/hyros.js" data-token="YOUR_SITE_TOKEN"></script>
 *
 * What it does:
 *  - Captures UTM params (utm_source, utm_medium, utm_campaign, utm_content, utm_term)
 *  - Captures click IDs (gclid, fbclid, ttclid) from Meta, Google, TikTok ads
 *  - Generates a persistent visitor ID (first-party cookie, 1 year)
 *  - Generates a session ID (30-min inactivity timeout)
 *  - Sends pageview + session events to your backend
 *  - Provides hyros.identify(email) to link visitor to a customer
 *  - Provides hyros.conversion({...}) to track purchases
 */
(function () {
  "use strict";

  // ── Config ──────────────────────────────────────────────────────────────────
  var script = document.currentScript || document.querySelector('script[data-token]');
  var SITE_TOKEN = (script && script.getAttribute("data-token")) || "";
  var ENDPOINT = (script && script.getAttribute("data-endpoint")) || 
                 (script && script.src ? script.src.replace(/\/t\/hyros\.js.*$/, "") : "") ||
                 window.location.origin;

  var COOKIE_VISITOR = "_hyros_vid";
  var COOKIE_SESSION = "_hyros_sid";
  var COOKIE_PARAMS  = "_hyros_params";
  var VISITOR_TTL_DAYS = 365;
  var SESSION_TTL_MIN  = 30;

  // ── Cookie helpers ──────────────────────────────────────────────────────────
  function setCookie(name, value, days) {
    var d = new Date();
    d.setTime(d.getTime() + days * 86400000);
    document.cookie = name + "=" + encodeURIComponent(value) +
      ";expires=" + d.toUTCString() +
      ";path=/;SameSite=Lax;Secure";
  }

  function getCookie(name) {
    var match = document.cookie.match(new RegExp("(^| )" + name + "=([^;]+)"));
    return match ? decodeURIComponent(match[2]) : "";
  }

  function setSessionCookie(name, value) {
    // 30-min rolling expiry
    setCookie(name, value, SESSION_TTL_MIN / 1440);
  }

  // ── ID generation ───────────────────────────────────────────────────────────
  function uuid() {
    return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (c) {
      var r = (Math.random() * 16) | 0;
      return (c === "x" ? r : (r & 0x3) | 0x8).toString(16);
    });
  }

  // ── URL param helpers ───────────────────────────────────────────────────────
  function getParam(name) {
    var url = new URL(window.location.href);
    return url.searchParams.get(name) || "";
  }

  function getAllTrackingParams() {
    // Custom UTM params:
    //   Meta:    fbc_id={{adset.id}}&h_ad_id={{ad.id}}
    //   TikTok:  ttc_id=_AID&ttclid=CLICKID&h_ad_id=CID_
    //   Google:  gc_id={campaignid}&g_special_campaign=true
    var fbc_id  = getParam("fbc_id");   // Meta adset ID
    var ttc_id  = getParam("ttc_id");   // TikTok advertiser ID
    var gc_id   = getParam("gc_id");    // Google campaign ID
    var h_ad_id = getParam("h_ad_id");  // ad ID (Meta) or campaign ID (TikTok)
    var g_special_campaign = getParam("g_special_campaign"); // Google flag

    // Auto-detect platform from custom params
    var detected_platform = "";
    if (fbc_id) {
      detected_platform = "meta";
    } else if (ttc_id) {
      detected_platform = "tiktok";
    } else if (gc_id || g_special_campaign) {
      detected_platform = "google";
    }

    // Map custom params to standard fields based on detected platform
    var mapped_adset_id   = getParam("adset_id") || getParam("adsetid") || fbc_id || "";
    var mapped_ad_id      = getParam("ad_id") || getParam("adid") || "";
    var mapped_campaign_id = getParam("campaign_id") || getParam("campaignid") || gc_id || "";

    // h_ad_id meaning depends on platform:
    //   Meta   → ad ID
    //   TikTok → campaign ID
    if (h_ad_id) {
      if (detected_platform === "meta") {
        mapped_ad_id = mapped_ad_id || h_ad_id;
      } else if (detected_platform === "tiktok") {
        mapped_campaign_id = mapped_campaign_id || h_ad_id;
      } else {
        // Fallback: treat as ad ID
        mapped_ad_id = mapped_ad_id || h_ad_id;
      }
    }

    return {
      utm_source:   getParam("utm_source"),
      utm_medium:   getParam("utm_medium"),
      utm_campaign: getParam("utm_campaign"),
      utm_content:  getParam("utm_content"),
      utm_term:     getParam("utm_term"),
      gclid:        getParam("gclid"),
      fbclid:       getParam("fbclid"),
      ttclid:       getParam("ttclid"),
      wbraid:       getParam("wbraid"),
      gbraid:       getParam("gbraid"),
      ad_id:        mapped_ad_id,
      adset_id:     mapped_adset_id,
      campaign_id:  mapped_campaign_id,
      creative_id:  getParam("creative_id") || "",
      // Custom params (raw)
      fbc_id:       fbc_id,
      ttc_id:       ttc_id,
      gc_id:        gc_id,
      h_ad_id:      h_ad_id,
      g_special_campaign: g_special_campaign,
      detected_platform:  detected_platform,
    };
  }

  // ── Persist first-touch params ──────────────────────────────────────────────
  // Store the FIRST set of UTM/click params we see (first-touch attribution data).
  // Also store the LATEST (for last-touch). Both are sent on every event.
  function persistParams(params) {
    var dominated = Object.keys(params).some(function (k) { return params[k]; });
    if (!dominated) return; // no tracking params in this visit

    // Always overwrite "last touch" params
    setCookie(COOKIE_PARAMS + "_last", JSON.stringify(params), VISITOR_TTL_DAYS);

    // Only set "first touch" if not already set
    if (!getCookie(COOKIE_PARAMS + "_first")) {
      setCookie(COOKIE_PARAMS + "_first", JSON.stringify(params), VISITOR_TTL_DAYS);
    }
  }

  function getStoredParams(type) {
    var raw = getCookie(COOKIE_PARAMS + "_" + type);
    if (!raw) return {};
    try { return JSON.parse(raw); } catch (e) { return {}; }
  }

  // ── Visitor & Session IDs ───────────────────────────────────────────────────
  var visitorId = getCookie(COOKIE_VISITOR);
  if (!visitorId) {
    visitorId = uuid();
    setCookie(COOKIE_VISITOR, visitorId, VISITOR_TTL_DAYS);
  }

  var sessionId = getCookie(COOKIE_SESSION);
  if (!sessionId) {
    sessionId = uuid();
  }
  // Always refresh session cookie (rolling 30-min window)
  setSessionCookie(COOKIE_SESSION, sessionId);

  // ── Capture current page params ─────────────────────────────────────────────
  var currentParams = getAllTrackingParams();
  persistParams(currentParams);

  // Merge: use current params if present, fall back to stored last-touch
  var lastParams = getStoredParams("last");
  var firstParams = getStoredParams("first");

  function mergedParams() {
    var last = getStoredParams("last");
    var current = getAllTrackingParams();
    var merged = {};
    var keys = ["utm_source","utm_medium","utm_campaign","utm_content","utm_term",
                "gclid","fbclid","ttclid","wbraid","gbraid",
                "ad_id","adset_id","campaign_id","creative_id",
                "fbc_id","ttc_id","gc_id","h_ad_id","g_special_campaign","detected_platform"];
    for (var i = 0; i < keys.length; i++) {
      merged[keys[i]] = current[keys[i]] || last[keys[i]] || "";
    }
    return merged;
  }

  // ── Device info ─────────────────────────────────────────────────────────────
  function getDevice() {
    var ua = navigator.userAgent || "";
    if (/Mobi|Android/i.test(ua)) return "mobile";
    if (/Tablet|iPad/i.test(ua)) return "tablet";
    return "desktop";
  }

  // ── Send beacon ─────────────────────────────────────────────────────────────
  function send(path, data) {
    var url = ENDPOINT + path;
    var payload = JSON.stringify(data);

    // Prefer sendBeacon for page unload resilience, fall back to fetch
    if (navigator.sendBeacon) {
      var blob = new Blob([payload], { type: "application/json" });
      navigator.sendBeacon(url, blob);
    } else {
      fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: payload,
        keepalive: true,
      }).catch(function () {});
    }
  }

  // ── Pageview ────────────────────────────────────────────────────────────────
  function trackPageview() {
    var params = mergedParams();
    send("/api/webhooks/track", {
      event:        "pageview",
      site_token:   SITE_TOKEN,
      visitor_id:   visitorId,
      session_id:   sessionId,
      customer_key: getCookie("_hyros_ck") || "",
      utm_source:   params.utm_source,
      utm_medium:   params.utm_medium,
      utm_campaign: params.utm_campaign,
      utm_content:  params.utm_content,
      utm_term:     params.utm_term,
      gclid:        params.gclid,
      fbclid:       params.fbclid,
      ttclid:       params.ttclid,
      ad_id:        params.ad_id,
      adset_id:     params.adset_id,
      campaign_id:  params.campaign_id,
      creative_id:  params.creative_id,
      fbc_id:       params.fbc_id,
      ttc_id:       params.ttc_id,
      gc_id:        params.gc_id,
      h_ad_id:      params.h_ad_id,
      g_special_campaign: params.g_special_campaign,
      detected_platform:  params.detected_platform,
      landing_page: window.location.pathname + window.location.search,
      referrer:     document.referrer || "",
      device:       getDevice(),
      page_title:   document.title || "",
      screen_width: screen.width,
      timestamp:    new Date().toISOString(),
    });
  }

  // ── Public API ──────────────────────────────────────────────────────────────
  window.hyros = window.hyros || {};

  /**
   * Identify a visitor by email. Call this when you have the user's email
   * (login, form submit, checkout). Links all past + future sessions to this person.
   *
   * Usage: hyros.identify("customer@example.com")
   */
  window.hyros.identify = function (email) {
    if (!email) return;
    send("/api/webhooks/identify", {
      email:       email,
      visitor_id:  visitorId,
      session_id:  sessionId,
      site_token:  SITE_TOKEN,
      timestamp:   new Date().toISOString(),
    });
    // Store hashed key locally for subsequent events
    // The backend will return the customer_key, but we also set a local cookie
    // as a fallback so conversion events can include it
    var ck = email.trim().toLowerCase();
    setCookie("_hyros_ck", ck, VISITOR_TTL_DAYS);
    setCookie("_hyros_email", email.trim(), VISITOR_TTL_DAYS);
  };

  /**
   * Track a conversion (purchase, lead, signup, etc).
   * Call this on your thank-you / order confirmation page.
   *
   * Usage: hyros.conversion({
   *   type: "Purchase",        // or "Lead", "Signup", etc.
   *   value: 97.00,            // revenue amount
   *   order_id: "ORD-12345",   // optional
   *   email: "user@example.com" // optional, will auto-identify
   * })
   */
  window.hyros.conversion = function (opts) {
    opts = opts || {};
    if (opts.email) {
      window.hyros.identify(opts.email);
    }

    var params = mergedParams();
    var firstP = getStoredParams("first");

    send("/api/webhooks/conversion", {
      event:          "conversion",
      site_token:     SITE_TOKEN,
      visitor_id:     visitorId,
      session_id:     sessionId,
      customer_key:   getCookie("_hyros_ck") || opts.email || "",
      type:           opts.type || "Purchase",
      value:          opts.value || 0,
      order_id:       opts.order_id || "",
      currency:       opts.currency || "USD",
      // Last-touch attribution params
      utm_source:     params.utm_source,
      utm_medium:     params.utm_medium,
      utm_campaign:   params.utm_campaign,
      utm_content:    params.utm_content,
      gclid:          params.gclid,
      fbclid:         params.fbclid,
      ttclid:         params.ttclid,
      ad_id:          params.ad_id,
      adset_id:       params.adset_id,
      campaign_id:    params.campaign_id,
      creative_id:    params.creative_id,
      fbc_id:         params.fbc_id,
      ttc_id:         params.ttc_id,
      gc_id:          params.gc_id,
      h_ad_id:        params.h_ad_id,
      g_special_campaign: params.g_special_campaign,
      detected_platform:  params.detected_platform,
      // First-touch params (for first-click attribution)
      first_utm_source:   firstP.utm_source || "",
      first_utm_campaign: firstP.utm_campaign || "",
      first_gclid:        firstP.gclid || "",
      first_fbclid:       firstP.fbclid || "",
      first_ttclid:       firstP.ttclid || "",
      landing_page:   window.location.pathname,
      referrer:       document.referrer || "",
      device:         getDevice(),
      timestamp:      new Date().toISOString(),
    });
  };

  /**
   * Manually fire a custom event.
   *
   * Usage: hyros.event("AddToCart", { value: 49.99, product: "Widget" })
   */
  window.hyros.event = function (name, data) {
    var params = mergedParams();
    send("/api/webhooks/track", {
      event:        name || "custom",
      site_token:   SITE_TOKEN,
      visitor_id:   visitorId,
      session_id:   sessionId,
      customer_key: getCookie("_hyros_ck") || "",
      utm_source:   params.utm_source,
      utm_medium:   params.utm_medium,
      utm_campaign: params.utm_campaign,
      gclid:        params.gclid,
      fbclid:       params.fbclid,
      ttclid:       params.ttclid,
      landing_page: window.location.pathname,
      device:       getDevice(),
      custom_data:  data || {},
      timestamp:    new Date().toISOString(),
    });
  };

  // Expose IDs for debugging
  window.hyros.visitorId = visitorId;
  window.hyros.sessionId = sessionId;
  window.hyros.getParams = mergedParams;
  window.hyros.getFirstTouch = function () { return getStoredParams("first"); };
  window.hyros.getLastTouch = function () { return getStoredParams("last"); };

  // ── Auto-fire pageview ──────────────────────────────────────────────────────
  trackPageview();

  // Track on SPA navigations (pushState / popstate)
  var _pushState = history.pushState;
  history.pushState = function () {
    _pushState.apply(history, arguments);
    setTimeout(trackPageview, 50);
  };
  window.addEventListener("popstate", function () {
    setTimeout(trackPageview, 50);
  });

})();
