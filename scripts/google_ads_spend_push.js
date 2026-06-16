/*
  Mini Hyros Google Ads spend push.

  Paste this into Google Ads > Tools > Bulk actions > Scripts.
  Run it inside the client account that owns the campaigns, not only the MCC.
*/

var MINI_HYROS_ENDPOINT = "https://mini-hyros.onrender.com";
var MINI_HYROS_TOKEN = "PASTE_SITE_OR_GOOGLE_ADS_SCRIPT_TOKEN_HERE";

// DATE_MODE options:
// - "LOOKBACK": push today plus the previous LOOKBACK_DAYS days.
// - "CUSTOM": push the exact CUSTOM_START_DATE to CUSTOM_END_DATE range.
// - "YESTERDAY", "LAST_7_DAYS", "THIS_MONTH", "LAST_MONTH": common presets.
var DATE_MODE = "LOOKBACK";
var LOOKBACK_DAYS = 30;
var CUSTOM_START_DATE = "2026-05-17";
var CUSTOM_END_DATE = "2026-06-16";

var BATCH_SIZE = 1000;

function main() {
  var account = AdsApp.currentAccount();
  var accountId = account.getCustomerId().replace(/-/g, "");
  var tz = account.getTimeZone();
  var dateRange = getDateRange(tz);
  var start = dateRange.start;
  var end = dateRange.end;

  Logger.log("Mini Hyros: running Google Ads spend push for " + start + " to " + end + " (" + dateRange.mode + ").");
  var rows = fetchSpendRows(accountId, start, end);

  if (!rows.length) {
    postRows(accountId, start, end, [], true);
    Logger.log("Mini Hyros: no Google Ads spend rows found for " + start + " to " + end + ".");
    return;
  }

  for (var offset = 0; offset < rows.length; offset += BATCH_SIZE) {
    var batch = rows.slice(offset, offset + BATCH_SIZE);
    postRows(accountId, start, end, batch, offset === 0);
  }

  Logger.log("Mini Hyros: pushed " + rows.length + " Google Ads spend rows for " + start + " to " + end + ".");
}

function getDateRange(tz) {
  var mode = String(DATE_MODE || "LOOKBACK").toUpperCase();
  var today = stripTime(new Date());
  var startDate;
  var endDate;

  if (mode === "CUSTOM") {
    startDate = parseDateString(CUSTOM_START_DATE);
    endDate = parseDateString(CUSTOM_END_DATE);
  } else if (mode === "YESTERDAY") {
    startDate = shiftDate(today, -1);
    endDate = shiftDate(today, -1);
  } else if (mode === "LAST_7_DAYS") {
    startDate = shiftDate(today, -6);
    endDate = today;
  } else if (mode === "THIS_MONTH") {
    startDate = new Date(today.getFullYear(), today.getMonth(), 1);
    endDate = today;
  } else if (mode === "LAST_MONTH") {
    startDate = new Date(today.getFullYear(), today.getMonth() - 1, 1);
    endDate = new Date(today.getFullYear(), today.getMonth(), 0);
  } else {
    mode = "LOOKBACK";
    endDate = today;
    startDate = shiftDate(today, -Number(LOOKBACK_DAYS || 30));
  }

  if (!startDate || !endDate || isNaN(startDate.getTime()) || isNaN(endDate.getTime())) {
    throw new Error("Invalid date range. Use yyyy-MM-dd, for example 2026-05-17.");
  }

  if (startDate.getTime() > endDate.getTime()) {
    var tmp = startDate;
    startDate = endDate;
    endDate = tmp;
  }

  return {
    mode: mode,
    start: Utilities.formatDate(startDate, tz, "yyyy-MM-dd"),
    end: Utilities.formatDate(endDate, tz, "yyyy-MM-dd")
  };
}

function parseDateString(value) {
  var text = String(value || "").trim();
  var match = text.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return null;
  return new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
}

function shiftDate(date, days) {
  var copy = new Date(date.getTime());
  copy.setDate(copy.getDate() + Number(days || 0));
  return copy;
}

function stripTime(date) {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate());
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
