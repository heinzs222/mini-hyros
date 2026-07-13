/*
  Mini Hyros Google Ads spend push.

  Paste this into Google Ads > Tools > Bulk actions > Scripts.
  Run it inside the client account that owns the campaigns, not only the MCC.
*/

var MINI_HYROS_ENDPOINT = "https://mini-hyros.onrender.com";
var MINI_HYROS_TOKEN =
  "PASTE_GOOGLE_ADS_SCRIPT_TOKEN_HERE";

// Include authoritative campaign totals so Performance Max / Smart / Demand
// Gen spend is not omitted. The backend stores only the positive difference
// between each campaign total and its matching ad_group_ad rows, which avoids
// double-counting standard campaigns.
var INCLUDE_CAMPAIGN_TOTAL_ROWS = true;

// DATE_MODE options:
// - "LOOKBACK": push exactly LOOKBACK_DAYS calendar days, including today.
// - "CUSTOM": push the exact CUSTOM_START_DATE to CUSTOM_END_DATE range.
// - "YESTERDAY", "LAST_7_DAYS", "THIS_MONTH", "LAST_MONTH": common presets.
var DATE_MODE = "LOOKBACK";
var LOOKBACK_DAYS = 30;
var CUSTOM_START_DATE = "2026-05-17";
var CUSTOM_END_DATE = "2026-06-16";

var BATCH_SIZE = 1000;

function main() {
  if (!MINI_HYROS_TOKEN || MINI_HYROS_TOKEN.indexOf("PASTE_") === 0) {
    throw new Error("Set MINI_HYROS_TOKEN to the GOOGLE_ADS_SCRIPT_TOKEN from Render before running.");
  }
  var account = AdsApp.currentAccount();
  var accountId = account.getCustomerId().replace(/-/g, "");
  var tz = account.getTimeZone();
  var dateRange = getDateRange(tz);
  var start = dateRange.start;
  var end = dateRange.end;

  Logger.log("Mini Hyros: running Google Ads spend push for " + start + " to " + end + " (" + dateRange.mode + ").");
  // Hyros compatibility uses ad_group_ad rows. Campaign totals are optional
  // because adding them produced a higher cost than the source Hyros account.
  var rows = fetchSpendRows(accountId, start, end);
  if (INCLUDE_CAMPAIGN_TOTAL_ROWS) {
    rows = rows.concat(fetchCampaignSpendRows(accountId, start, end));
  }

  if (!rows.length) {
    postRows(accountId, start, end, [], true);
    Logger.log("Mini Hyros: no Google Ads spend rows found for " + start + " to " + end + ".");
    return;
  }

  // Batch by whole date groups (never split a date's ad rows from its
  // campaign-total rows across two network calls) — the backend reconciles
  // ad-level vs campaign-level cost per (date, campaign_id) within a single
  // request, so both must always arrive together.
  //
  // Every batch is self-replacing: it sends replace=true scoped to only the
  // dates it actually contains (its own min/max). Sending the whole run's
  // start/end with replace on the first batch alone would delete the entire
  // range and then only re-insert batch-0's dates, wiping later dates that
  // arrive in subsequent batches.
  var batches = chunkRowsByDate(rows, BATCH_SIZE);
  for (var i = 0; i < batches.length; i++) {
    var range = batchDateRange(batches[i]);
    postRows(accountId, range.start, range.end, batches[i], true);
  }

  Logger.log("Mini Hyros: pushed " + rows.length + " Google Ads spend rows for " + start + " to " + end + ".");
}

function getDateRange(tz) {
  var mode = String(DATE_MODE || "LOOKBACK").toUpperCase();
  // Derive the account's calendar date first, then do arithmetic on ISO dates.
  // Mixing Date#getDate() in the script timezone with Utilities.formatDate in
  // the Google Ads account timezone can shift a boundary by one day.
  var today = Utilities.formatDate(new Date(), tz, "yyyy-MM-dd");
  var start;
  var end;

  if (mode === "CUSTOM") {
    start = normalizeIsoDate(CUSTOM_START_DATE);
    end = normalizeIsoDate(CUSTOM_END_DATE);
  } else if (mode === "YESTERDAY") {
    start = addIsoDays(today, -1);
    end = start;
  } else if (mode === "LAST_7_DAYS") {
    start = addIsoDays(today, -7);
    end = addIsoDays(today, -1);
  } else if (mode === "THIS_MONTH") {
    start = today.slice(0, 8) + "01";
    end = today;
  } else if (mode === "LAST_MONTH") {
    end = addIsoDays(today.slice(0, 8) + "01", -1);
    start = end.slice(0, 8) + "01";
  } else {
    mode = "LOOKBACK";
    var days = Math.max(1, Number(LOOKBACK_DAYS || 30));
    end = today;
    start = addIsoDays(today, -(days - 1));
  }

  if (!start || !end) {
    throw new Error("Invalid date range. Use yyyy-MM-dd, for example 2026-05-17.");
  }

  if (start > end) {
    var tmp = start;
    start = end;
    end = tmp;
  }

  return {
    mode: mode,
    start: start,
    end: end
  };
}

function chunkRowsByDate(rows, targetBatchSize) {
  var byDate = {};
  var order = [];
  for (var i = 0; i < rows.length; i++) {
    var d = rows[i].date || "";
    if (!byDate[d]) {
      byDate[d] = [];
      order.push(d);
    }
    byDate[d].push(rows[i]);
  }

  var batches = [];
  var current = [];
  for (var j = 0; j < order.length; j++) {
    var group = byDate[order[j]];
    if (current.length && current.length + group.length > targetBatchSize) {
      batches.push(current);
      current = [];
    }
    current = current.concat(group);
  }
  if (current.length) batches.push(current);
  return batches;
}

function batchDateRange(rows) {
  // Return the min/max yyyy-MM-dd date within a single batch. Dates are
  // zero-padded ISO strings, so lexicographic comparison equals date order.
  var min = null;
  var max = null;
  for (var i = 0; i < rows.length; i++) {
    var d = rows[i].date || "";
    if (!d) continue;
    if (min === null || d < min) min = d;
    if (max === null || d > max) max = d;
  }
  return { start: min || "", end: max || "" };
}

function normalizeIsoDate(value) {
  var text = String(value || "").trim();
  var match = text.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return "";
  var parsed = new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])));
  var normalized = Utilities.formatDate(parsed, "UTC", "yyyy-MM-dd");
  return normalized === text ? text : "";
}

function addIsoDays(value, days) {
  var text = normalizeIsoDate(value);
  if (!text) return "";
  var parts = text.split("-");
  var date = new Date(Date.UTC(Number(parts[0]), Number(parts[1]) - 1, Number(parts[2])));
  date.setUTCDate(date.getUTCDate() + Number(days || 0));
  return Utilities.formatDate(date, "UTC", "yyyy-MM-dd");
}

function fetchSpendRows(accountId, start, end) {
  var query = [
    "SELECT",
    "segments.date,",
    "campaign.id, campaign.name,",
    "ad_group.id, ad_group.name,",
    "ad_group_ad.ad.id, ad_group_ad.ad.name,",
    "metrics.clicks, metrics.impressions, metrics.cost_micros",
    "FROM ad_group_ad",
    "WHERE segments.date BETWEEN '" + start + "' AND '" + end + "'"
  ].join(" ");

  var output = [];
  var iterator = AdsApp.search(query);
  while (iterator.hasNext()) {
    var row = iterator.next();
    output.push({
      date: safe(row.segments && row.segments.date),
      account_id: accountId,
      campaign_id: safe(row.campaign && row.campaign.id),
      campaign_name: safe(row.campaign && row.campaign.name),
      adset_id: safe(row.adGroup && row.adGroup.id),
      adset_name: safe(row.adGroup && row.adGroup.name),
      ad_id: safe(row.adGroupAd && row.adGroupAd.ad && row.adGroupAd.ad.id),
      ad_name: safe(row.adGroupAd && row.adGroupAd.ad && row.adGroupAd.ad.name),
      clicks: Number(row.metrics && row.metrics.clicks || 0),
      impressions: Number(row.metrics && row.metrics.impressions || 0),
      cost: Number(row.metrics && row.metrics.costMicros || 0) / 1000000
    });
  }
  return output;
}

function fetchCampaignSpendRows(accountId, start, end) {
  // Campaign-level totals include cost for every campaign type (Performance
  // Max, Smart, Demand Gen, Search, Display, Video), unlike ad_group_ad which
  // only exists for standard ad groups. These rows have no adset_id/ad_id so
  // the backend treats them as campaign totals and only stores the delta over
  // the matching ad_group_ad rows (avoids double-counting standard campaigns).
  var query = [
    "SELECT",
    "segments.date,",
    "campaign.id, campaign.name,",
    "metrics.clicks, metrics.impressions, metrics.cost_micros",
    "FROM campaign",
    "WHERE segments.date BETWEEN '" + start + "' AND '" + end + "'"
  ].join(" ");

  var output = [];
  var iterator = AdsApp.search(query);
  while (iterator.hasNext()) {
    var row = iterator.next();
    output.push({
      date: safe(row.segments && row.segments.date),
      account_id: accountId,
      campaign_id: safe(row.campaign && row.campaign.id),
      campaign_name: safe(row.campaign && row.campaign.name),
      adset_id: "",
      adset_name: "",
      ad_id: "",
      ad_name: "",
      clicks: Number(row.metrics && row.metrics.clicks || 0),
      impressions: Number(row.metrics && row.metrics.impressions || 0),
      cost: Number(row.metrics && row.metrics.costMicros || 0) / 1000000
    });
  }
  return output;
}

function postRows(accountId, start, end, rows, replace) {
  var url = MINI_HYROS_ENDPOINT.replace(/\/$/, "") + "/api/spend/google-ads-script";
  var payload = {
    token: MINI_HYROS_TOKEN,
    account_id: accountId,
    start_date: start,
    end_date: end,
    replace: replace,
    rows: rows
  };

  var response = UrlFetchApp.fetch(url, {
    method: "post",
    contentType: "application/json",
    headers: {
      "X-Mini-Hyros-Token": MINI_HYROS_TOKEN
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });

  var code = response.getResponseCode();
  var body = response.getContentText();
  if (code < 200 || code >= 300) {
    throw new Error("Mini Hyros spend push failed: HTTP " + code + " " + body);
  }
  Logger.log("Mini Hyros response: " + body);
}

function safe(value) {
  if (value === null || value === undefined) return "";
  return String(value);
}
