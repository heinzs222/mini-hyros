/**
 * Mini Hyros Tracking Pixel
 * 
 * Paste this on EVERY page of your site:
 *   <script src="https://YOUR_DOMAIN/t/hyros.js" data-token="YOUR_SITE_TOKEN"></script>
 *
 * Optional Google Ads/Stape pushback:
 *   Add data-stape-endpoint="https://YOUR-STAPE-SERVER" to send Purchase
 *   conversions to server-side GTM via Stape.
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
  var script = document.currentScript || document.querySelector('script[data-token]') || document.querySelector('script[src*="/t/hyros.js"]') || document.querySelector('script[src*="/v1/lst/universal-script"]');
  var scriptUrl = script && script.src ? new URL(script.src, window.location.href) : null;
  var SITE_TOKEN = (script && script.getAttribute("data-token")) || (scriptUrl ? (scriptUrl.searchParams.get("ph") || scriptUrl.searchParams.get("site_token") || "") : "");
  var ENDPOINT = (script && script.getAttribute("data-endpoint")) || (scriptUrl ? scriptUrl.origin : "") || window.location.origin;
  var STAPE_ENDPOINT = cleanBaseUrl((script && script.getAttribute("data-stape-endpoint")) || (scriptUrl ? (scriptUrl.searchParams.get("stape_endpoint") || "") : ""));
  var STAPE_VERSION = (script && script.getAttribute("data-stape-version")) || (scriptUrl ? (scriptUrl.searchParams.get("stape_version") || "") : "") || "2";

  var COOKIE_VISITOR = "_hyros_vid";
  var COOKIE_SESSION = "_hyros_sid";
  var COOKIE_PARAMS  = "_hyros_params";
  var VISITOR_TTL_DAYS = 365;
  var SESSION_TTL_MIN  = 30;

  // ── Cookie helpers ──────────────────────────────────────────────────────────
  function setCookie(name, value, days) {
    var d = new Date();
    d.setTime(d.getTime() + days * 86400000);
    var secure = window.location.protocol === "https:" ? ";Secure" : "";
    document.cookie = name + "=" + encodeURIComponent(value) +
      ";expires=" + d.toUTCString() +
      ";path=/;SameSite=Lax" + secure;
  }

  function getCookie(name) {
    var match = document.cookie.match(new RegExp("(^| )" + name + "=([^;]+)"));
    return match ? decodeURIComponent(match[2]) : "";
  }

  function setStoredValue(name, value) {
    try { window.localStorage.setItem(name, value); } catch (e) {}
  }

  function getStoredValue(name) {
    try { return window.localStorage.getItem(name) || ""; } catch (e) { return ""; }
  }

  function setSessionCookie(name, value) {
    // 30-min rolling expiry
    setCookie(name, value, SESSION_TTL_MIN / 1440);
  }

  function cleanBaseUrl(value) {
    return String(value || "").trim().replace(/\/+$/, "");
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
    var sent = false;

    // Prefer sendBeacon for page unload resilience. If browser policies
    // block ping/beacon requests (or sendBeacon fails), fall back to fetch.
    if (navigator.sendBeacon) {
      try {
        var blob = new Blob([payload], { type: "application/json" });
        sent = navigator.sendBeacon(url, blob) === true;
      } catch (e) {
        sent = false;
      }
    }
    if (!sent) {
      fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: payload,
        keepalive: true,
      }).catch(function () {});
    }
  }

  function sendStapeEvent(eventName, data) {
    if (!STAPE_ENDPOINT || !eventName) return;

    var url;
    try {
      url = new URL("/data", STAPE_ENDPOINT);
    } catch (e) {
      return;
    }

    function setParam(key, value) {
      if (value === undefined || value === null || value === "") return;
      url.searchParams.set(key, String(value));
    }

    setParam("v", STAPE_VERSION);
    setParam("event", eventName);

    [
      "value",
      "currency",
      "transaction_id",
      "order_id",
      "event_id",
      "visitor_id",
      "session_id",
      "utm_source",
      "utm_medium",
      "utm_campaign",
      "utm_content",
      "utm_term",
      "gclid",
      "wbraid",
      "gbraid",
      "fbclid",
      "ttclid",
      "ad_id",
      "adset_id",
      "campaign_id",
      "creative_id",
      "landing_page",
      "referrer",
      "device"
    ].forEach(function (key) {
      setParam(key, data[key]);
    });

    try {
      var img = new Image(1, 1);
      img.referrerPolicy = "no-referrer-when-downgrade";
      img.src = url.toString();
    } catch (e) {
      fetch(url.toString(), { method: "GET", mode: "no-cors", keepalive: true }).catch(function () {});
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
      wbraid:       params.wbraid,
      gbraid:       params.gbraid,
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
    var conversionType = opts.type || "Purchase";
    var conversionValue = opts.value || 0;
    var orderId = opts.order_id || "";
    var conversionPayload = {
      event:          "conversion",
      site_token:     SITE_TOKEN,
      visitor_id:     visitorId,
      session_id:     sessionId,
      customer_key:   getCookie("_hyros_ck") || opts.email || "",
      type:           conversionType,
      value:          conversionValue,
      order_id:       orderId,
      currency:       opts.currency || "USD",
      // Last-touch attribution params
      utm_source:     params.utm_source,
      utm_medium:     params.utm_medium,
      utm_campaign:   params.utm_campaign,
      utm_content:    params.utm_content,
      gclid:          params.gclid,
      wbraid:         params.wbraid,
      gbraid:         params.gbraid,
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
    };

    send("/api/webhooks/conversion", conversionPayload);

    if (String(conversionType).toLowerCase() === "purchase") {
      sendStapeEvent("purchase", {
        value: conversionValue,
        currency: conversionPayload.currency,
        transaction_id: orderId,
        order_id: orderId,
        event_id: orderId || (sessionId + "|" + conversionPayload.timestamp),
        visitor_id: visitorId,
        session_id: sessionId,
        utm_source: params.utm_source,
        utm_medium: params.utm_medium,
        utm_campaign: params.utm_campaign,
        utm_content: params.utm_content,
        utm_term: params.utm_term,
        gclid: params.gclid,
        wbraid: params.wbraid,
        gbraid: params.gbraid,
        fbclid: params.fbclid,
        ttclid: params.ttclid,
        ad_id: params.ad_id,
        adset_id: params.adset_id,
        campaign_id: params.campaign_id,
        creative_id: params.creative_id,
        landing_page: window.location.pathname,
        referrer: document.referrer || "",
        device: getDevice(),
      });
    }
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

  function normalizeEmail(value) {
    return String(value || "").trim().toLowerCase();
  }

  function normalizePhone(value) {
    return String(value || "").replace(/[^\d+]/g, "");
  }

  function parseAmount(value) {
    if (typeof value === "number") {
      return isFinite(value) ? value : 0;
    }
    var parsed = parseFloat(String(value || "").replace(/[^0-9.-]/g, ""));
    return isFinite(parsed) ? parsed : 0;
  }

  function findEmailInRoot(root) {
    if (!root || !root.querySelectorAll) return "";
    var preferred = root.querySelectorAll('input[type="email"], input[name*="email"], input[name*="Email"], input[id*="email"], input[id*="Email"], input[autocomplete="email"]');
    for (var i = 0; i < preferred.length; i++) {
      var email = normalizeEmail(preferred[i].value);
      if (/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) return email;
    }
    var allInputs = root.querySelectorAll("input, textarea");
    for (var j = 0; j < allInputs.length; j++) {
      var value = normalizeEmail(allInputs[j].value);
      if (/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value)) return value;
    }
    return normalizeEmail(getCookie("_hyros_email"));
  }

  function findPhoneInRoot(root) {
    if (!root || !root.querySelectorAll) return "";
    var inputs = root.querySelectorAll('input[type="tel"], input[name*="phone"], input[name*="Phone"], input[id*="phone"], input[id*="Phone"], input[autocomplete="tel"]');
    for (var i = 0; i < inputs.length; i++) {
      var phone = normalizePhone(inputs[i].value);
      if (phone.replace(/\D/g, "").length >= 7) return phone;
    }
    return "";
  }

  function findNameInRoot(root) {
    if (!root || !root.querySelectorAll) return "";
    var nameInputs = root.querySelectorAll('input[name*="name"], input[name*="Name"], input[id*="name"], input[id*="Name"]');
    var parts = [];
    for (var i = 0; i < nameInputs.length; i++) {
      if (nameInputs[i].value) parts.push(String(nameInputs[i].value).trim());
    }
    return parts.join(" ").trim();
  }

  function resolveElementName(el) {
    if (!el || !el.getAttribute) return document.title || "Form";
    return el.getAttribute("data-form-name") || el.getAttribute("data-calendar-name") || el.getAttribute("data-form-id") || el.getAttribute("data-calendar-id") || el.getAttribute("name") || el.getAttribute("id") || document.title || "Form";
  }

  function resolveOrderId(root) {
    var params = new URLSearchParams(window.location.search);
    var candidates = [
      params.get("order_id"),
      params.get("order"),
      params.get("transaction_id"),
      params.get("transaction"),
      params.get("payment_intent"),
      root && root.getAttribute ? root.getAttribute("data-order-id") : "",
      root && root.getAttribute ? root.getAttribute("data-transaction-id") : "",
      root && root.querySelector ? ((root.querySelector('[data-order-id], [data-transaction-id], .order-number, .transaction-id') || {}).textContent || "") : "",
    ];
    for (var i = 0; i < candidates.length; i++) {
      var value = String(candidates[i] || "").trim();
      if (value) return value;
    }
    return "";
  }

  function resolveOrderValue(root) {
    var params = new URLSearchParams(window.location.search);
    var direct = parseAmount(params.get("value") || params.get("amount") || params.get("total"));
    if (direct > 0) return direct;
    if (root && root.getAttribute) {
      var attrValue = parseAmount(root.getAttribute("data-price") || root.getAttribute("data-total") || root.getAttribute("data-value"));
      if (attrValue > 0) return attrValue;
    }
    if (root && root.querySelector) {
      var priceEl = root.querySelector('[data-price], [data-total], .price, .total, .order-total, [class*="price"], [class*="total"]');
      if (priceEl) {
        var textValue = parseAmount(priceEl.getAttribute && (priceEl.getAttribute("data-price") || priceEl.getAttribute("data-total")) || priceEl.textContent || "");
        if (textValue > 0) return textValue;
      }
      var hiddenValue = root.querySelector('input[name="amount"], input[name="price"], input[name="total"], input[name="value"]');
      if (hiddenValue && hiddenValue.value) {
        var hiddenAmount = parseAmount(hiddenValue.value);
        if (hiddenAmount > 0) return hiddenAmount;
      }
    }
    return 0;
  }

  function resolvePurchaseFromDataLayer() {
    if (!Array.isArray(window.dataLayer)) return null;
    for (var i = window.dataLayer.length - 1; i >= 0; i--) {
      var item = window.dataLayer[i];
      if (!item || typeof item !== "object") continue;
      var ecommerce = item.ecommerce || {};
      var purchase = ecommerce.purchase || {};
      var actionField = purchase.actionField || purchase;
      var orderId = actionField.id || ecommerce.transaction_id || item.transaction_id || "";
      var value = parseAmount(actionField.revenue || purchase.value || ecommerce.value || item.value || "");
      var email = normalizeEmail(item.email || ecommerce.email || purchase.email || "");
      var currency = String(actionField.currency || ecommerce.currency || item.currency || "USD");
      if (String(item.event || "").toLowerCase() === "purchase" || orderId || value > 0) {
        return { order_id: String(orderId || ""), value: value, email: email, currency: currency };
      }
    }
    return null;
  }

  function resolvePurchaseFromShopify() {
    var checkout = window.Shopify && window.Shopify.checkout;
    if (!checkout) return null;
    return {
      order_id: String(checkout.order_id || checkout.order_number || ""),
      value: parseAmount(checkout.total_price || checkout.total_price_amount || checkout.subtotal_price || ""),
      email: normalizeEmail(checkout.email || ""),
      currency: String(checkout.currency || "USD"),
    };
  }

  function resolvePurchaseData(root) {
    var dataLayerPurchase = resolvePurchaseFromDataLayer();
    if (dataLayerPurchase && (dataLayerPurchase.order_id || dataLayerPurchase.value > 0 || dataLayerPurchase.email)) {
      return dataLayerPurchase;
    }
    var shopifyPurchase = resolvePurchaseFromShopify();
    if (shopifyPurchase && (shopifyPurchase.order_id || shopifyPurchase.value > 0 || shopifyPurchase.email)) {
      return shopifyPurchase;
    }
    return {
      order_id: resolveOrderId(root),
      value: resolveOrderValue(root),
      email: findEmailInRoot(root || document),
      currency: String((new URLSearchParams(window.location.search)).get("currency") || "USD"),
    };
  }

  function markOnce(key) {
    if (!key) return true;
    var storageKey = "_hyros_once_" + key;
    if (getStoredValue(storageKey)) return false;
    setStoredValue(storageKey, String(Date.now()));
    return true;
  }

  function hookIdentityInputs() {
    var inputs = document.querySelectorAll('input[type="email"], input[name*="email"], input[name*="Email"], input[id*="email"], input[id*="Email"], input[autocomplete="email"]');
    inputs.forEach(function (input) {
      if (input._hyrosIdentifyHooked) return;
      input._hyrosIdentifyHooked = true;
      var capture = function () {
        var email = normalizeEmail(input.value);
        if (/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
          window.hyros.identify(email);
        }
      };
      input.addEventListener("change", capture);
      input.addEventListener("blur", capture);
    });
  }

  function hookForms() {
    var forms = document.querySelectorAll('form, [data-form-id], .hl-form, #inline-form, .form-builder');
    forms.forEach(function (form) {
      if (form.matches && form.matches('.order-form, .payment-form, [data-product-id], #order-form, .hl-order-form, .checkout-form, form[action*="checkout"], form[action*="payment"], form[id*="checkout"], form[class*="checkout"], [data-calendar-id], .hl-calendar, .calendar-widget')) return;
      if (form.querySelector && !form.querySelector('input[type="email"], input[name*="email"], input[name*="Email"], input[id*="email"], input[id*="Email"], input[type="tel"], input[name*="phone"], input[name*="Phone"], input[id*="phone"], input[id*="Phone"]')) return;
      if (form._hyrosHooked) return;
      form._hyrosHooked = true;
      form.addEventListener("submit", function () {
        var email = findEmailInRoot(form);
        var phone = findPhoneInRoot(form);
        var name = findNameInRoot(form);
        if (!email && !phone) return;
        if (email) {
          window.hyros.identify(email);
        }
        window.hyros.event("FormSubmit", {
          form_name: resolveElementName(form),
          email: email,
          phone: phone,
          name: name,
          page: window.location.pathname,
        });
        if (email) {
          window.hyros.conversion({
            type: "Lead",
            value: 0,
            order_id: "lead-" + Date.now(),
            email: email,
          });
        }
      });
    });
  }

  function hookBookings() {
    var buttons = document.querySelectorAll('.calendar-widget button[type="submit"], .hl-calendar button[type="submit"], [data-calendar-id] button[type="submit"], .booking-confirm-btn, #confirm-booking');
    buttons.forEach(function (btn) {
      if (btn._hyrosHooked) return;
      btn._hyrosHooked = true;
      btn.addEventListener("click", function () {
        var container = btn.closest("form") || btn.closest("[data-calendar-id]") || btn.parentElement || document;
        var email = findEmailInRoot(container);
        if (email) {
          window.hyros.identify(email);
        }
        window.hyros.event("BookingConfirmed", {
          calendar: resolveElementName(container),
          email: email,
          page: window.location.pathname,
        });
        window.hyros.conversion({
          type: "Booking",
          value: 0,
          order_id: "booking-" + Date.now(),
          email: email,
        });
      });
    });
  }

  function hookCheckoutForms() {
    var forms = document.querySelectorAll('.order-form, .payment-form, [data-product-id], #order-form, .hl-order-form, .checkout-form, form[action*="checkout"], form[action*="payment"], form[id*="checkout"], form[class*="checkout"]');
    forms.forEach(function (form) {
      if (form._hyrosPayHooked) return;
      form._hyrosPayHooked = true;
      form.addEventListener("submit", function () {
        var email = findEmailInRoot(form);
        if (email) {
          window.hyros.identify(email);
        }
        window.hyros.event("CheckoutSubmit", {
          order_id: resolveOrderId(form),
          value: resolveOrderValue(form),
          email: email,
          page: window.location.pathname,
        });
      });
    });
  }

  function detectThankYouPage() {
    var url = window.location.href.toLowerCase();
    var path = window.location.pathname.toLowerCase();
    var title = (document.title || "").toLowerCase();
    var isThankYou = path.indexOf("thank") > -1 || path.indexOf("thankyou") > -1 || path.indexOf("thank-you") > -1 || path.indexOf("confirmation") > -1 || path.indexOf("success") > -1 || path.indexOf("order-confirmed") > -1 || path.indexOf("order-received") > -1 || url.indexOf("thank") > -1 || title.indexOf("thank you") > -1 || title.indexOf("order confirmed") > -1 || title.indexOf("booking confirmed") > -1;
    if (!isThankYou) return;
    var purchase = resolvePurchaseData(document);
    var email = normalizeEmail(purchase.email || findEmailInRoot(document) || getCookie("_hyros_email"));
    if (email) {
      window.hyros.identify(email);
    }
    window.hyros.event("ThankYouPage", {
      page: window.location.pathname,
      email: email,
      order_id: purchase.order_id || "",
      value: purchase.value || 0,
    });
    var bookingLike = path.indexOf("booking") > -1 || title.indexOf("booking") > -1 || title.indexOf("appointment") > -1;
    var purchaseLike = purchase.value > 0 || !!purchase.order_id || path.indexOf("order") > -1 || title.indexOf("receipt") > -1 || title.indexOf("payment") > -1;
    var dedupeKey = purchase.order_id || (window.location.pathname + "|" + (purchase.value || 0) + "|" + (email || ""));
    if (bookingLike && markOnce("booking|" + dedupeKey)) {
      window.hyros.conversion({
        type: "Booking",
        value: 0,
        order_id: purchase.order_id || ("booking-ty-" + Date.now()),
        email: email,
      });
      return;
    }
    if (purchaseLike && markOnce("purchase|" + dedupeKey)) {
      window.hyros.conversion({
        type: "Purchase",
        value: purchase.value || 0,
        order_id: purchase.order_id || ("order-ty-" + Date.now()),
        email: email,
        currency: purchase.currency || "USD",
      });
    }
  }

  function initAutoCapture() {
    hookIdentityInputs();
    hookForms();
    hookBookings();
    hookCheckoutForms();
    detectThankYouPage();
  }

  function startObserver() {
    if (startObserver._started || !document.body) return;
    startObserver._started = true;
    observer.observe(document.body, { childList: true, subtree: true });
  }

  // Expose IDs for debugging
  window.hyros.visitorId = visitorId;
  window.hyros.sessionId = sessionId;
  window.hyros.stapeEndpoint = STAPE_ENDPOINT;
  window.hyros.getParams = mergedParams;
  window.hyros.getFirstTouch = function () { return getStoredParams("first"); };
  window.hyros.getLastTouch = function () { return getStoredParams("last"); };

  // ── Auto-fire pageview ──────────────────────────────────────────────────────
  trackPageview();
  initAutoCapture();

  var observer = new MutationObserver(function (mutations) {
    var shouldRehook = false;
    for (var i = 0; i < mutations.length; i++) {
      if (mutations[i].addedNodes.length > 0) {
        shouldRehook = true;
        break;
      }
    }
    if (shouldRehook) {
      setTimeout(initAutoCapture, 100);
    }
  });

  startObserver();
  document.addEventListener("DOMContentLoaded", function () {
    initAutoCapture();
    startObserver();
  });

  // Track on SPA navigations (pushState / popstate)
  var _pushState = history.pushState;
  history.pushState = function () {
    _pushState.apply(history, arguments);
    setTimeout(function () {
      trackPageview();
      initAutoCapture();
    }, 50);
  };
  window.addEventListener("popstate", function () {
    setTimeout(function () {
      trackPageview();
      initAutoCapture();
    }, 50);
  });

})();
