function defaultApiBase(): string {
  if (process.env.NEXT_PUBLIC_API_URL) return process.env.NEXT_PUBLIC_API_URL;
  if (typeof window !== "undefined") {
    const host = window.location.hostname;
    if (host !== "localhost" && host !== "127.0.0.1") {
      return "https://mini-hyros.onrender.com";
    }
  }
  return "http://localhost:8000";
}

const API_BASE = defaultApiBase();
const AUTH_TOKEN_KEY = "hyros_auth_token";
const configuredTimeoutMs = Number(process.env.NEXT_PUBLIC_API_TIMEOUT_MS || 90000);
const API_TIMEOUT_MS = Number.isFinite(configuredTimeoutMs) && configuredTimeoutMs > 0
  ? configuredTimeoutMs
  : 90000;

function readAuthToken(): string {
  if (typeof window === "undefined") return "";
  return window.localStorage.getItem(AUTH_TOKEN_KEY) || "";
}

export function setAuthToken(token: string) {
  if (typeof window === "undefined") return;
  const cleaned = String(token || "").trim();
  if (!cleaned) {
    window.localStorage.removeItem(AUTH_TOKEN_KEY);
    return;
  }
  window.localStorage.setItem(AUTH_TOKEN_KEY, cleaned);
}

export function clearAuthToken() {
  if (typeof window === "undefined") return;
  window.localStorage.removeItem(AUTH_TOKEN_KEY);
}

/**
 * Error thrown for non-OK HTTP responses. Carries the numeric HTTP `status`
 * so callers can distinguish a real 401/403 (redirect to login) from a
 * network error / timeout (backend cold start — retry, do NOT redirect).
 */
export class ApiError extends Error {
  status: number;
  constructor(message: string, status: number) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

// ── Global request-activity signal ───────────────────────────────────────────
// Every network call funnels through apiFetch, so a tiny pub/sub counter here
// lets a single <TopProgressBar/> reflect ANY in-flight request (report, compare,
// leads, sync, feature panels) without each caller wiring up its own spinner.
let activeRequests = 0;
const activityListeners = new Set<() => void>();

export function subscribeApiActivity(listener: () => void): () => void {
  activityListeners.add(listener);
  return () => {
    activityListeners.delete(listener);
  };
}

export function getApiActivitySnapshot(): number {
  return activeRequests;
}

function bumpActivity(delta: number) {
  activeRequests = Math.max(0, activeRequests + delta);
  for (const listener of activityListeners) {
    try {
      listener();
    } catch {}
  }
}

// Absolute upper bound on ANY request, even when the caller manages its own
// deadline via an AbortSignal. Longer than every legitimate operation deadline
// (the longest sync deadline is 180s); this only exists so a stalled connection
// can never hang a request — and everything gated on it — forever.
const HARD_CEILING_MS = 300_000;

export async function apiFetch(input: string, init: RequestInit = {}, signal?: AbortSignal) {
  const headers = new Headers(init.headers || {});
  const token = readAuthToken();
  if (token) {
    headers.set("Authorization", `Bearer ${token}`);
  }

  const providedSignal = signal ?? init.signal;
  // Callers such as the dashboard sync coordinator provide operation-specific
  // deadlines. Do not let the generic request timeout preempt those deadlines.
  const timeoutController = providedSignal ? null : new AbortController();
  const timeoutId = timeoutController
    ? setTimeout(() => timeoutController.abort(), API_TIMEOUT_MS)
    : null;
  const controller = new AbortController();

  const abort = () => controller.abort();
  timeoutController?.signal.addEventListener("abort", abort, { once: true });
  providedSignal?.addEventListener("abort", abort, { once: true });
  // Safety-net ceiling for caller-managed signals (which skip the generic
  // timeout above): a request that outlives every sane deadline is aborted so
  // it can't wedge the in-flight guards that pause auto-refresh.
  const ceilingId = providedSignal ? setTimeout(abort, HARD_CEILING_MS) : null;

  bumpActivity(1);
  try {
    return await fetch(input, { ...init, headers, signal: controller.signal });
  } catch (err: any) {
    if (controller.signal.aborted && !providedSignal?.aborted) {
      throw new Error(`Request timed out: ${input}`);
    }
    throw err;
  } finally {
    // CRITICAL: balance the increment above. Without this the activity counter
    // only ever grows and the top progress bar animates forever.
    bumpActivity(-1);
    if (timeoutId !== null) clearTimeout(timeoutId);
    if (ceilingId !== null) clearTimeout(ceilingId);
    timeoutController?.signal.removeEventListener("abort", abort);
    providedSignal?.removeEventListener("abort", abort);
  }
}

export async function loginWithPassword(username: string, password: string) {
  const res = await apiFetch(`${API_BASE}/api/auth/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password }),
  });
  if (!res.ok) throw new Error(`Login failed: ${res.status}`);
  const data = await res.json();
  if (data?.token) setAuthToken(String(data.token));
  return data;
}

export async function fetchAuthMe() {
  const res = await apiFetch(`${API_BASE}/api/auth/me`);
  if (!res.ok) throw new ApiError(`Auth status failed: ${res.status}`, res.status);
  return res.json();
}

export async function logout() {
  const res = await apiFetch(`${API_BASE}/api/auth/logout`, { method: "POST" });
  clearAuthToken();
  if (!res.ok) throw new Error(`Logout failed: ${res.status}`);
  return res.json();
}

export async function fetchReport(params: {
  start_date?: string;
  end_date?: string;
  model?: string;
  lookback_days?: number;
  active_tab?: string;
  conversion_type?: string;
  use_click_date?: boolean;
  no_cache?: boolean;
}, signal?: AbortSignal) {
  const sp = new URLSearchParams();
  if (params.start_date) sp.set("start_date", params.start_date);
  if (params.end_date) sp.set("end_date", params.end_date);
  if (params.start_date || params.end_date) sp.set("defer_history", "true");
  if (params.model) sp.set("model", params.model);
  if (params.lookback_days) sp.set("lookback_days", String(params.lookback_days));
  if (params.active_tab) sp.set("active_tab", params.active_tab);
  if (params.conversion_type) sp.set("conversion_type", params.conversion_type);
  if (params.use_click_date) sp.set("use_click_date", "true");
  if (params.no_cache) sp.set("no_cache", "true");
  const res = await apiFetch(`${API_BASE}/api/report?${sp.toString()}`, {}, signal);
  if (!res.ok) throw new ApiError(`Report fetch failed: ${res.status}`, res.status);
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
}, signal?: AbortSignal) {
  const sp = new URLSearchParams();
  sp.set("parent_tab", params.parent_tab);
  sp.set("parent_id", params.parent_id);
  if (params.start_date) sp.set("start_date", params.start_date);
  if (params.end_date) sp.set("end_date", params.end_date);
  if (params.model) sp.set("model", params.model);
  if (params.lookback_days) sp.set("lookback_days", String(params.lookback_days));
  if (params.conversion_type) sp.set("conversion_type", params.conversion_type);
  if (params.use_click_date) sp.set("use_click_date", "true");
  const res = await apiFetch(`${API_BASE}/api/report/children?${sp.toString()}`, {}, signal);
  if (!res.ok) throw new Error(`Children fetch failed: ${res.status}`);
  return res.json();
}

export async function syncSpend(params: {
  platform?: string;
  start_date?: string;
  end_date?: string;
} = {}, signal?: AbortSignal) {
  const sp = new URLSearchParams();
  sp.set("platform", params.platform || "all");
  if (params.start_date) sp.set("start_date", params.start_date);
  if (params.end_date) sp.set("end_date", params.end_date);

  const qs = sp.toString();
  const url = `${API_BASE}/api/spend/sync${qs ? `?${qs}` : ""}`;
  const res = await apiFetch(url, { method: "POST" }, signal);
  if (!res.ok) throw new Error(`Spend sync failed: ${res.status}`);
  return res.json();
}

export async function importSpendCsv(payload: {
  platform?: string;
  account_id?: string;
  csv_text: string;
  replace?: boolean;
}) {
  const res = await apiFetch(`${API_BASE}/api/spend/import_csv`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      platform: payload.platform || "google",
      account_id: payload.account_id || "",
      csv_text: payload.csv_text,
      replace: payload.replace !== false,
    }),
  });
  if (!res.ok) throw new Error(`Spend import failed: ${res.status}`);
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
  const res = await apiFetch(`${API_BASE}/api/video/metrics?${sp.toString()}`);
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
  const res = await apiFetch(`${API_BASE}/api/video/summary?${sp.toString()}`);
  if (!res.ok) throw new Error(`Video summary fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchConnections(validate = false) {
  const res = await apiFetch(`${API_BASE}/api/connections/status${validate ? "?validate=true" : ""}`);
  if (!res.ok) throw new Error(`Connections fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchSetupGuide() {
  const res = await apiFetch(`${API_BASE}/api/connections/setup-guide`);
  if (!res.ok) throw new Error(`Setup guide fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchTracking() {
  const res = await apiFetch(`${API_BASE}/api/tracking`);
  if (!res.ok) throw new Error(`Tracking fetch failed: ${res.status}`);
  return res.json();
}

// ── LTV ──────────────────────────────────────────────────────────────────────
export async function fetchLtvBySource(breakdown = "platform", windows = "30,60,90,365", signal?: AbortSignal) {
  const res = await apiFetch(`${API_BASE}/api/ltv/by-source?breakdown=${breakdown}&windows=${windows}`, {}, signal);
  if (!res.ok) throw new Error(`LTV fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchLtvSummary(signal?: AbortSignal) {
  const res = await apiFetch(`${API_BASE}/api/ltv/summary`, {}, signal);
  if (!res.ok) throw new Error(`LTV summary fetch failed: ${res.status}`);
  return res.json();
}

// ── Customer Journey ─────────────────────────────────────────────────────────
export async function fetchJourneyStats(signal?: AbortSignal) {
  const res = await apiFetch(`${API_BASE}/api/journey/stats`, {}, signal);
  if (!res.ok) throw new Error(`Journey stats fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchCommonPaths(limit = 10, min_conversions = 1, signal?: AbortSignal) {
  const res = await apiFetch(`${API_BASE}/api/journey/common-paths?limit=${limit}&min_conversions=${min_conversions}`, {}, signal);
  if (!res.ok) throw new Error(`Common paths fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchLeadJourneys(params: {
  start_date?: string;
  end_date?: string;
  limit?: number;
  include_purchases?: boolean;
} = {}, signal?: AbortSignal) {
  const sp = new URLSearchParams();
  if (params.start_date) sp.set("start_date", params.start_date);
  if (params.end_date) sp.set("end_date", params.end_date);
  if (params.limit) sp.set("limit", String(params.limit));
  if (params.include_purchases === false) sp.set("include_purchases", "false");
  const res = await apiFetch(`${API_BASE}/api/journey/leads?${sp.toString()}`, {}, signal);
  if (!res.ok) throw new Error(`Lead journeys fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchCustomerJourney(customerKey: string) {
  const res = await apiFetch(`${API_BASE}/api/journey/customer?customer_key=${customerKey}`);
  if (!res.ok) throw new Error(`Customer journey fetch failed: ${res.status}`);
  return res.json();
}

// ── Funnel ───────────────────────────────────────────────────────────────────
export async function fetchFunnelReport(startDate = "", endDate = "", signal?: AbortSignal) {
  const sp = new URLSearchParams();
  if (startDate) sp.set("start_date", startDate);
  if (endDate) sp.set("end_date", endDate);
  const res = await apiFetch(`${API_BASE}/api/funnel/report?${sp.toString()}`, {}, signal);
  if (!res.ok) throw new Error(`Funnel report fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchFunnelBySource(breakdown = "platform", signal?: AbortSignal) {
  const res = await apiFetch(`${API_BASE}/api/funnel/by-source?breakdown=${breakdown}`, {}, signal);
  if (!res.ok) throw new Error(`Funnel by source fetch failed: ${res.status}`);
  return res.json();
}

// ── Cohort ───────────────────────────────────────────────────────────────────
export async function fetchCohortAnalysis(granularity = "month", signal?: AbortSignal) {
  const res = await apiFetch(`${API_BASE}/api/cohort/analysis?granularity=${granularity}`, {}, signal);
  if (!res.ok) throw new Error(`Cohort analysis fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchCohortRetention(granularity = "month") {
  const res = await apiFetch(`${API_BASE}/api/cohort/retention?granularity=${granularity}`);
  if (!res.ok) throw new Error(`Cohort retention fetch failed: ${res.status}`);
  return res.json();
}

// ── CAPI ─────────────────────────────────────────────────────────────────────
export async function fetchCapiStatus(signal?: AbortSignal) {
  const res = await apiFetch(`${API_BASE}/api/capi/status`, {}, signal);
  if (!res.ok) throw new Error(`CAPI status fetch failed: ${res.status}`);
  return res.json();
}

export async function triggerCapiSync() {
  const res = await apiFetch(`${API_BASE}/api/capi/auto-sync`, { method: "POST" });
  if (!res.ok) throw new Error(`CAPI sync failed: ${res.status}`);
  return res.json();
}

export async function fetchCapiLog(limit = 20, signal?: AbortSignal) {
  const res = await apiFetch(`${API_BASE}/api/capi/log?limit=${limit}`, {}, signal);
  if (!res.ok) throw new Error(`CAPI log fetch failed: ${res.status}`);
  return res.json();
}

// ── Refunds ──────────────────────────────────────────────────────────────────
export async function fetchRefundSummary() {
  const res = await apiFetch(`${API_BASE}/api/refunds/summary`);
  if (!res.ok) throw new Error(`Refund summary fetch failed: ${res.status}`);
  return res.json();
}

// ── AI Recommendations ───────────────────────────────────────────────────────
export async function fetchRecommendations(startDate = "", endDate = "") {
  const sp = new URLSearchParams();
  if (startDate) sp.set("start_date", startDate);
  if (endDate) sp.set("end_date", endDate);
  const res = await apiFetch(`${API_BASE}/api/ai/recommendations?${sp.toString()}`);
  if (!res.ok) throw new Error(`Recommendations fetch failed: ${res.status}`);
  return res.json();
}

export async function fetchInsights() {
  const res = await apiFetch(`${API_BASE}/api/ai/insights`);
  if (!res.ok) throw new Error(`Insights fetch failed: ${res.status}`);
  return res.json();
}

// ── Ad Names ─────────────────────────────────────────────────────────────────
export async function fetchAdNames(platform = "", entityType = "", signal?: AbortSignal) {
  const sp = new URLSearchParams();
  if (platform) sp.set("platform", platform);
  if (entityType) sp.set("entity_type", entityType);
  const res = await apiFetch(`${API_BASE}/api/ad-names?${sp.toString()}`, {}, signal);
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
  const res = await apiFetch(`${API_BASE}/api/ad-names`, {
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
  const res = await apiFetch(`${API_BASE}/api/ad-names`, {
    method: "DELETE",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(mapping),
  });
  if (!res.ok) throw new Error(`Ad name delete failed: ${res.status}`);
  return res.json();
}

export async function syncAdNames(platform = "all", signal?: AbortSignal) {
  const res = await apiFetch(`${API_BASE}/api/ad-names/sync?platform=${platform}`, { method: "POST" }, signal);
  if (!res.ok) throw new Error(`Ad names sync failed: ${res.status}`);
  return res.json();
}

// ── Stripe ──────────────────────────────────────────────────────────────────
export async function syncStripe(params: {
  start_date?: string;
  end_date?: string;
} = {}, signal?: AbortSignal) {
  const sp = new URLSearchParams();
  if (params.start_date) sp.set("start_date", params.start_date);
  if (params.end_date) sp.set("end_date", params.end_date);
  const url = `${API_BASE}/api/stripe/sync${sp.toString() ? `?${sp.toString()}` : ""}`;
  const res = await apiFetch(url, { method: "POST" }, signal);
  if (!res.ok) throw new Error(`Stripe sync failed: ${res.status}`);
  return res.json();
}

export async function fetchStripeStatus() {
  const res = await apiFetch(`${API_BASE}/api/stripe/status`);
  if (!res.ok) throw new Error(`Stripe status failed: ${res.status}`);
  return res.json();
}

// ── GoHighLevel ───────────────────────────────────────────────────────────────
export async function syncGhl(params: {
  start_date?: string;
  end_date?: string;
  include_forms?: boolean;
  include_opportunities?: boolean;
} = {}, signal?: AbortSignal) {
  const sp = new URLSearchParams();
  if (params.start_date) sp.set("start_date", params.start_date);
  if (params.end_date) sp.set("end_date", params.end_date);
  if (typeof params.include_forms === "boolean") sp.set("include_forms", String(params.include_forms));
  if (typeof params.include_opportunities === "boolean") sp.set("include_opportunities", String(params.include_opportunities));
  const url = `${API_BASE}/api/ghl/sync${sp.toString() ? `?${sp.toString()}` : ""}`;
  const res = await apiFetch(url, { method: "POST" }, signal);
  if (!res.ok) throw new Error(`GHL sync failed: ${res.status}`);
  return res.json();
}

export async function fetchGhlStatus() {
  const res = await apiFetch(`${API_BASE}/api/ghl/status`);
  if (!res.ok) throw new Error(`GHL status failed: ${res.status}`);
  return res.json();
}

export async function connectGhl(payload: { api_token: string; location_id: string }) {
  const res = await apiFetch(`${API_BASE}/api/ghl/connect`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok && res.status !== 400) throw new Error(`GHL connect failed: ${res.status}`);
  return res.json();
}

// ── Platform Auth ─────────────────────────────────────────────────────────────
export async function fetchTikTokStatus(signal?: AbortSignal) {
  const res = await apiFetch(`${API_BASE}/api/platform-auth/tiktok/status`, {}, signal);
  if (!res.ok) throw new Error(`TikTok status failed: ${res.status}`);
  return res.json();
}

export async function fetchTikTokConnectUrl() {
  const res = await apiFetch(`${API_BASE}/api/platform-auth/tiktok/connect`);
  if (!res.ok) throw new Error(`TikTok connect failed: ${res.status}`);
  return res.json();
}

export async function refreshTikTokToken() {
  const res = await apiFetch(`${API_BASE}/api/platform-auth/tiktok/refresh`, { method: "POST" });
  if (!res.ok) throw new Error(`TikTok refresh failed: ${res.status}`);
  return res.json();
}

// ── WebSocket ────────────────────────────────────────────────────────────────
export interface ManagedWebSocket {
  socket: WebSocket;
  /** Closes the socket without scheduling a reconnect (use on unmount). */
  close: () => void;
}

/** Connection lifecycle surfaced to consumers via the optional status callback. */
export type WebSocketStatus = "connecting" | "open" | "reconnecting" | "disconnected";

/** Auth-rejection close code: the server refuses the token — do NOT reconnect. */
const WS_AUTH_CLOSE_CODE = 4401;
const WS_RECONNECT_INITIAL_MS = 3000;
const WS_RECONNECT_MAX_MS = 60000;

export function createWebSocket(
  onMessage: (data: any) => void,
  onStatusChange?: (status: WebSocketStatus) => void,
): ManagedWebSocket | null {
  if (typeof window === "undefined") return null;

  // Shared state across reconnect attempts so an intentional close stops
  // the auto-reconnect loop and any pending reconnect timeout. `delay` grows
  // exponentially and resets to the initial value on a successful open.
  const state = {
    manualClose: false,
    reconnectTimer: null as ReturnType<typeof setTimeout> | null,
    delay: WS_RECONNECT_INITIAL_MS,
  };
  let currentSocket: WebSocket;

  const setStatus = (status: WebSocketStatus) => {
    try {
      onStatusChange?.(status);
    } catch {}
  };

  const connect = (): WebSocket => {
    const token = readAuthToken();
    const wsBase = API_BASE.replace("http", "ws") + "/ws";
    const wsUrl = token ? `${wsBase}?token=${encodeURIComponent(token)}` : wsBase;
    const ws = new WebSocket(wsUrl);
    ws.onopen = () => {
      // A successful connection resets the backoff window.
      state.delay = WS_RECONNECT_INITIAL_MS;
      setStatus("open");
    };
    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        onMessage(data);
      } catch {}
    };
    ws.onclose = (event) => {
      if (state.manualClose) return;
      // Auth rejection (bad/expired token): stop reconnecting entirely and
      // surface a disconnected state instead of hammering the server.
      if (event.code === WS_AUTH_CLOSE_CODE) {
        setStatus("disconnected");
        return;
      }
      // Exponential backoff (start 3s, double to a 60s cap) with jitter so
      // reconnects from many tabs don't stampede the backend at once.
      const base = Math.min(state.delay, WS_RECONNECT_MAX_MS);
      const wait = base + Math.random() * base * 0.25;
      state.delay = Math.min(base * 2, WS_RECONNECT_MAX_MS);
      setStatus("reconnecting");
      state.reconnectTimer = setTimeout(() => {
        if (state.manualClose) return;
        currentSocket = connect();
      }, wait);
    };
    return ws;
  };

  setStatus("connecting");
  currentSocket = connect();

  return {
    get socket() {
      return currentSocket;
    },
    close() {
      state.manualClose = true;
      if (state.reconnectTimer) {
        clearTimeout(state.reconnectTimer);
        state.reconnectTimer = null;
      }
      try {
        currentSocket.close();
      } catch {}
    },
  };
}

// ── Campaign Settings ─────────────────────────────────────────────────────────
export interface CampaignSetting {
  platform: string;
  campaign_id: string;
  name: string;
  tracked: boolean;
  lifetime_spend: number;
  last_seen: string;
}

export interface CampaignTrackingItem {
  platform: string;
  campaign_id: string;
  tracked: boolean;
}

export async function fetchCampaigns(signal?: AbortSignal) {
  const res = await apiFetch(`${API_BASE}/api/campaigns`, {}, signal);
  if (!res.ok) throw new ApiError(`Campaigns fetch failed: ${res.status}`, res.status);
  return res.json();
}

export async function setCampaignTracking(platform: string, campaign_id: string, tracked: boolean) {
  const res = await apiFetch(`${API_BASE}/api/campaigns/tracking`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ platform, campaign_id, tracked }),
  });
  if (!res.ok) throw new ApiError(`Campaign tracking update failed: ${res.status}`, res.status);
  return res.json();
}

export async function setCampaignTrackingBatch(items: CampaignTrackingItem[]) {
  const res = await apiFetch(`${API_BASE}/api/campaigns/tracking/batch`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ items }),
  });
  if (!res.ok) throw new ApiError(`Campaign tracking batch update failed: ${res.status}`, res.status);
  return res.json();
}
