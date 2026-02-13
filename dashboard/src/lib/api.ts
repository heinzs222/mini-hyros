const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

export async function fetchReport(params: {
  start_date?: string;
  end_date?: string;
  model?: string;
  lookback_days?: number;
  active_tab?: string;
  conversion_type?: string;
  use_click_date?: boolean;
}) {
  const sp = new URLSearchParams();
  if (params.start_date) sp.set("start_date", params.start_date);
  if (params.end_date) sp.set("end_date", params.end_date);
  if (params.model) sp.set("model", params.model);
  if (params.lookback_days) sp.set("lookback_days", String(params.lookback_days));
  if (params.active_tab) sp.set("active_tab", params.active_tab);
  if (params.conversion_type) sp.set("conversion_type", params.conversion_type);
  if (params.use_click_date) sp.set("use_click_date", "true");
  const res = await fetch(`${API_BASE}/api/report?${sp.toString()}`);
  if (!res.ok) throw new Error(`Report fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchChildren(params: {
  parent_tab: string;
  parent_id: string;
  start_date?: string;
  end_date?: string;
  model?: string;
  lookback_days?: number;
  conversion_type?: string;
  use_click_date?: boolean;
}) {
  const sp = new URLSearchParams();
  sp.set("parent_tab", params.parent_tab);
  sp.set("parent_id", params.parent_id);
  if (params.start_date) sp.set("start_date", params.start_date);
  if (params.end_date) sp.set("end_date", params.end_date);
  if (params.model) sp.set("model", params.model);
  if (params.lookback_days) sp.set("lookback_days", String(params.lookback_days));
  if (params.conversion_type) sp.set("conversion_type", params.conversion_type);
  if (params.use_click_date) sp.set("use_click_date", "true");
  const res = await fetch(`${API_BASE}/api/report/children?${sp.toString()}`);
  if (!res.ok) throw new Error(`Children fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchVideoMetrics(params: {
  start_date?: string;
  end_date?: string;
  platform?: string;
  breakdown?: string;
}) {
  const sp = new URLSearchParams();
  if (params.start_date) sp.set("start_date", params.start_date);
  if (params.end_date) sp.set("end_date", params.end_date);
  if (params.platform) sp.set("platform", params.platform);
  if (params.breakdown) sp.set("breakdown", params.breakdown);
  const res = await fetch(`${API_BASE}/api/video/metrics?${sp.toString()}`);
  if (!res.ok) throw new Error(`Video metrics fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchVideoSummary(params: {
  start_date?: string;
  end_date?: string;
}) {
  const sp = new URLSearchParams();
  if (params.start_date) sp.set("start_date", params.start_date);
  if (params.end_date) sp.set("end_date", params.end_date);
  const res = await fetch(`${API_BASE}/api/video/summary?${sp.toString()}`);
  if (!res.ok) throw new Error(`Video summary fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchConnections() {
  const res = await fetch(`${API_BASE}/api/connections/status`);
  if (!res.ok) throw new Error(`Connections fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchSetupGuide() {
  const res = await fetch(`${API_BASE}/api/connections/setup-guide`);
  if (!res.ok) throw new Error(`Setup guide fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchTracking() {
  const res = await fetch(`${API_BASE}/api/tracking`);
  if (!res.ok) throw new Error(`Tracking fetch failed: ${res.status}`);
  return res.json();
}

// ── LTV ──────────────────────────────────────────────────────────────────────
export async function fetchLtvBySource(breakdown = "platform", windows = "30,60,90,365") {
  const res = await fetch(`${API_BASE}/api/ltv/by-source?breakdown=${breakdown}&windows=${windows}`);
  if (!res.ok) throw new Error(`LTV fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchLtvSummary() {
  const res = await fetch(`${API_BASE}/api/ltv/summary`);
  if (!res.ok) throw new Error(`LTV summary fetch failed: ${res.status}`);
  return res.json();
}

// ── Customer Journey ─────────────────────────────────────────────────────────
export async function fetchJourneyStats() {
  const res = await fetch(`${API_BASE}/api/journey/stats`);
  if (!res.ok) throw new Error(`Journey stats fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchCommonPaths(limit = 10) {
  const res = await fetch(`${API_BASE}/api/journey/common-paths?limit=${limit}&min_conversions=1`);
  if (!res.ok) throw new Error(`Common paths fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchCustomerJourney(customerKey: string) {
  const res = await fetch(`${API_BASE}/api/journey/customer?customer_key=${customerKey}`);
  if (!res.ok) throw new Error(`Customer journey fetch failed: ${res.status}`);
  return res.json();
}

// ── Funnel ───────────────────────────────────────────────────────────────────
export async function fetchFunnelReport(startDate = "", endDate = "") {
  const sp = new URLSearchParams();
  if (startDate) sp.set("start_date", startDate);
  if (endDate) sp.set("end_date", endDate);
  const res = await fetch(`${API_BASE}/api/funnel/report?${sp.toString()}`);
  if (!res.ok) throw new Error(`Funnel report fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchFunnelBySource(breakdown = "platform") {
  const res = await fetch(`${API_BASE}/api/funnel/by-source?breakdown=${breakdown}`);
  if (!res.ok) throw new Error(`Funnel by source fetch failed: ${res.status}`);
  return res.json();
}

// ── Cohort ───────────────────────────────────────────────────────────────────
export async function fetchCohortAnalysis(granularity = "month") {
  const res = await fetch(`${API_BASE}/api/cohort/analysis?granularity=${granularity}`);
  if (!res.ok) throw new Error(`Cohort analysis fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchCohortRetention(granularity = "month") {
  const res = await fetch(`${API_BASE}/api/cohort/retention?granularity=${granularity}`);
  if (!res.ok) throw new Error(`Cohort retention fetch failed: ${res.status}`);
  return res.json();
}

// ── CAPI ─────────────────────────────────────────────────────────────────────
export async function fetchCapiStatus() {
  const res = await fetch(`${API_BASE}/api/capi/status`);
  if (!res.ok) throw new Error(`CAPI status fetch failed: ${res.status}`);
  return res.json();
}

export async function triggerCapiSync() {
  const res = await fetch(`${API_BASE}/api/capi/auto-sync`, { method: "POST" });
  if (!res.ok) throw new Error(`CAPI sync failed: ${res.status}`);
  return res.json();
}

export async function fetchCapiLog(limit = 20) {
  const res = await fetch(`${API_BASE}/api/capi/log?limit=${limit}`);
  if (!res.ok) throw new Error(`CAPI log fetch failed: ${res.status}`);
  return res.json();
}

// ── Refunds ──────────────────────────────────────────────────────────────────
export async function fetchRefundSummary() {
  const res = await fetch(`${API_BASE}/api/refunds/summary`);
  if (!res.ok) throw new Error(`Refund summary fetch failed: ${res.status}`);
  return res.json();
}

// ── AI Recommendations ───────────────────────────────────────────────────────
export async function fetchRecommendations(startDate = "", endDate = "") {
  const sp = new URLSearchParams();
  if (startDate) sp.set("start_date", startDate);
  if (endDate) sp.set("end_date", endDate);
  const res = await fetch(`${API_BASE}/api/ai/recommendations?${sp.toString()}`);
  if (!res.ok) throw new Error(`Recommendations fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchInsights() {
  const res = await fetch(`${API_BASE}/api/ai/insights`);
  if (!res.ok) throw new Error(`Insights fetch failed: ${res.status}`);
  return res.json();
}

// ── Ad Names ─────────────────────────────────────────────────────────────────
export async function fetchAdNames(platform = "", entityType = "") {
  const sp = new URLSearchParams();
  if (platform) sp.set("platform", platform);
  if (entityType) sp.set("entity_type", entityType);
  const res = await fetch(`${API_BASE}/api/ad-names?${sp.toString()}`);
  if (!res.ok) throw new Error(`Ad names fetch failed: ${res.status}`);
  return res.json();
}

export async function upsertAdName(mapping: {
  platform: string;
  entity_type: string;
  entity_id: string;
  name: string;
  parent_id?: string;
}) {
  const res = await fetch(`${API_BASE}/api/ad-names`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(mapping),
  });
  if (!res.ok) throw new Error(`Ad name upsert failed: ${res.status}`);
  return res.json();
}

export async function deleteAdName(mapping: {
  platform: string;
  entity_type: string;
  entity_id: string;
}) {
  const res = await fetch(`${API_BASE}/api/ad-names`, {
    method: "DELETE",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(mapping),
  });
  if (!res.ok) throw new Error(`Ad name delete failed: ${res.status}`);
  return res.json();
}

export async function syncAdNames(platform = "all") {
  const res = await fetch(`${API_BASE}/api/ad-names/sync?platform=${platform}`, { method: "POST" });
  if (!res.ok) throw new Error(`Ad names sync failed: ${res.status}`);
  return res.json();
}

// ── WebSocket ────────────────────────────────────────────────────────────────
export function createWebSocket(onMessage: (data: any) => void): WebSocket | null {
  if (typeof window === "undefined") return null;
  const wsUrl = API_BASE.replace("http", "ws") + "/ws";
  const ws = new WebSocket(wsUrl);
  ws.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      onMessage(data);
    } catch {}
  };
  ws.onclose = () => {
    setTimeout(() => createWebSocket(onMessage), 3000);
  };
  return ws;
}
