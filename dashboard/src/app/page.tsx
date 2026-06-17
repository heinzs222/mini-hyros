"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import dynamic from "next/dynamic";
import { fetchReport, createWebSocket, fetchAuthMe, logout as logoutApi, syncSpend, syncAdNames, syncStripe, type ManagedWebSocket } from "@/lib/api";
import { daysAgo } from "@/lib/utils";
import SummaryCards from "@/components/SummaryCards";
import AttributionTable from "@/components/AttributionTable";
import TrackingHealth from "@/components/TrackingHealth";
import PlatformComparisonTable from "@/components/PlatformComparisonTable";
import FunnelSnapshotTable from "@/components/FunnelSnapshotTable";
import LiveFeed from "@/components/LiveFeed";
import {
  BarChart3,
  DollarSign,
  RefreshCw,
  Settings,
  Zap,
  TrendingUp,
  Filter,
  Route,
  Grid3x3,
  Send,
  Tag,
  LogOut,
} from "lucide-react";

// Code-split heavy client components so recharts and the feature panels load on demand.
const ChartLoading = () => <div className="h-72 rounded-xl border border-[var(--card-border)] bg-[var(--card)] animate-pulse" />;
const PanelLoading = () => <div className="text-center py-12 text-gray-500 text-sm">Loading...</div>;

const PerformanceChart = dynamic(() => import("@/components/PerformanceChart"), { ssr: false, loading: ChartLoading });
const TrafficValueChart = dynamic(() => import("@/components/TrafficValueChart"), { ssr: false, loading: ChartLoading });
const CumulativePerformanceChart = dynamic(() => import("@/components/CumulativePerformanceChart"), { ssr: false, loading: ChartLoading });
const PlatformMixChart = dynamic(() => import("@/components/PlatformMixChart"), { ssr: false, loading: ChartLoading });

const LtvPanel = dynamic(() => import("@/components/LtvPanel"), { ssr: false, loading: PanelLoading });
const FunnelPanel = dynamic(() => import("@/components/FunnelPanel"), { ssr: false, loading: PanelLoading });
const JourneyPanel = dynamic(() => import("@/components/JourneyPanel"), { ssr: false, loading: PanelLoading });
const CohortPanel = dynamic(() => import("@/components/CohortPanel"), { ssr: false, loading: PanelLoading });
const CapiPanel = dynamic(() => import("@/components/CapiPanel"), { ssr: false, loading: PanelLoading });
const AdNamesPanel = dynamic(() => import("@/components/AdNamesPanel"), { ssr: false, loading: PanelLoading });
const SpendImportPanel = dynamic(() => import("@/components/SpendImportPanel"), { ssr: false, loading: PanelLoading });

interface LiveEvent {
  _id?: number;
  type: string;
  ts: string;
  order_id?: string;
  gross?: number;
  session_id?: string;
  utm_source?: string;
  customer_key?: string;
}

const MODELS = [
  { value: "last_click", label: "Last Click" },
  { value: "first_click", label: "First Click" },
  { value: "linear", label: "Linear" },
  { value: "time_decay", label: "Time Decay" },
  { value: "data_driven_proxy", label: "Data-Driven" },
];

const DAY_MS = 24 * 60 * 60 * 1000;

type CompareMode = "none" | "previous_period" | "custom_range" | "model";

function shiftIsoDate(dateIso: string, deltaDays: number): string {
  const date = new Date(`${dateIso}T00:00:00`);
  date.setDate(date.getDate() + deltaDays);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function inclusiveSpanDays(startIso: string, endIso: string): number {
  const start = new Date(`${startIso}T00:00:00`).getTime();
  const end = new Date(`${endIso}T00:00:00`).getTime();
  return Math.max(1, Math.round((end - start) / DAY_MS) + 1);
}

function normalizeDateRange(startIso: string, endIso: string): [string, string] {
  if (!startIso) return [endIso, endIso];
  if (!endIso) return [startIso, startIso];
  return startIso <= endIso ? [startIso, endIso] : [endIso, startIso];
}

function modelLabel(value: string): string {
  return MODELS.find((m) => m.value === value)?.label || value;
}

function compactSyncError(message: string): string {
  return String(message || "")
    .replace(/\s+/g, " ")
    .replace(/access_token=[^&\s]+/gi, "access_token=REDACTED")
    .trim()
    .slice(0, 500);
}

const REPORT_CACHE_KEY = "hyros_report_cache";

// Shared stable empty array so nullish fallbacks keep a constant identity across
// renders (prevents child re-renders when liveEvents state updates).
const EMPTY: any[] = [];

export default function DashboardPage() {
  const router = useRouter();
  const [report, setReport] = useState<any>(null);
  const [compareReport, setCompareReport] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [authChecked, setAuthChecked] = useState(false);
  const [authEnabled, setAuthEnabled] = useState(false);
  const [authUser, setAuthUser] = useState("");
  const [activeTab, setActiveTab] = useState("campaign");
  const [model, setModel] = useState("last_click");
  const [compareMode, setCompareMode] = useState<CompareMode>("previous_period");
  const [compareModel, setCompareModel] = useState("first_click");
  const [compareLabel, setCompareLabel] = useState("");
  const [primaryStartDate, setPrimaryStartDate] = useState(daysAgo(30));
  const [primaryEndDate, setPrimaryEndDate] = useState(daysAgo(0));
  const [compareStartDate, setCompareStartDate] = useState(daysAgo(60));
  const [compareEndDate, setCompareEndDate] = useState(daysAgo(31));
  const [useClickDate, setUseClickDate] = useState(false);
  const [liveEvents, setLiveEvents] = useState<LiveEvent[]>([]);
  const [wsConnected, setWsConnected] = useState(false);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [syncingSpend, setSyncingSpend] = useState(false);
  const [lastAutoSyncAt, setLastAutoSyncAt] = useState("");
  const [syncErrors, setSyncErrors] = useState<string[]>([]);
  const [platformFilter, setPlatformFilter] = useState("all");
  const [mainTab, setMainTab] = useState("attribution");
  const wsRef = useRef<ManagedWebSocket | null>(null);
  const refreshTimerRef = useRef<NodeJS.Timeout | null>(null);
  const spendSyncTimerRef = useRef<NodeJS.Timeout | null>(null);
  const syncingSpendRef = useRef(false);
  const loadReportRef = useRef<() => Promise<void>>(async () => {});
  const reportRequestSeqRef = useRef(0);
  // Guard so the 30s interval / WS refetch never overlaps an in-flight report request.
  const reportInFlightRef = useRef(false);
  // Coalesces bursts of WS new_order events into a single trailing refetch.
  const wsRefetchTimerRef = useRef<NodeJS.Timeout | null>(null);
  // Monotonic id assigned to each live event on receipt (stable list key).
  const liveEventSeqRef = useRef(0);
  // Current main tab, mirrored into a ref so timers/WS handlers can gate on it.
  const mainTabRef = useRef("attribution");

  const loadReport = useCallback(async () => {
    // Only the attribution tab consumes the full report; skip elsewhere.
    if (mainTab !== "attribution") return;
    // In-flight guard: do not stack report requests.
    if (reportInFlightRef.current) return;

    const requestSeq = reportRequestSeqRef.current + 1;
    reportRequestSeqRef.current = requestSeq;
    reportInFlightRef.current = true;

    try {
      const [startDate, endDate] = normalizeDateRange(primaryStartDate, primaryEndDate);

      const primaryParams = {
        start_date: startDate,
        end_date: endDate,
        model,
        active_tab: activeTab,
        use_click_date: useClickDate,
      };
      const cacheKey = JSON.stringify(primaryParams);
      let cacheHit = false;

      // Show cached data only when it exactly matches the current report query.
      const cached = typeof window !== "undefined" ? window.localStorage.getItem(REPORT_CACHE_KEY) : null;
      if (cached) {
        try {
          const { data, key } = JSON.parse(cached);
          if (key === cacheKey && data) {
            setReport(data);
            setLoading(false);
            cacheHit = true;
          }
        } catch {}
      }

      if (!cacheHit) setLoading(true);
      setError(null);
      setCompareReport(null);

      let comparePromise: Promise<any> | null = null;
      let nextCompareLabel = "";

      if (compareMode === "previous_period") {
        const spanDays = inclusiveSpanDays(startDate, endDate);
        const previousEnd = shiftIsoDate(startDate, -1);
        const previousStart = shiftIsoDate(previousEnd, -(spanDays - 1));
        comparePromise = fetchReport({
          start_date: previousStart,
          end_date: previousEnd,
          model,
          active_tab: activeTab,
          use_click_date: useClickDate,
        });
        nextCompareLabel = `${previousStart} to ${previousEnd}`;
      } else if (compareMode === "custom_range") {
        const [customStart, customEnd] = normalizeDateRange(compareStartDate, compareEndDate);
        if (customStart && customEnd) {
          comparePromise = fetchReport({
            start_date: customStart,
            end_date: customEnd,
            model,
            active_tab: activeTab,
            use_click_date: useClickDate,
          });
          nextCompareLabel = `${customStart} to ${customEnd}`;
        }
      } else if (compareMode === "model" && compareModel !== model) {
        comparePromise = fetchReport({
          start_date: startDate,
          end_date: endDate,
          model: compareModel,
          active_tab: activeTab,
          use_click_date: useClickDate,
        });
        nextCompareLabel = `${modelLabel(compareModel)} model`;
      }

      const [data, compareData] = await Promise.all([
        fetchReport(primaryParams),
        comparePromise ?? Promise.resolve(null),
      ]);

      if (requestSeq !== reportRequestSeqRef.current) return;
      setReport(data);
      setCompareReport(compareData);
      setCompareLabel(compareData ? nextCompareLabel : "");
      // Cache for instant display on next login
      if (data && typeof window !== "undefined") {
        try { window.localStorage.setItem(REPORT_CACHE_KEY, JSON.stringify({ key: cacheKey, data, ts: Date.now() })); } catch {}
      }
    } catch (err: any) {
      if (requestSeq !== reportRequestSeqRef.current) return;
      if (String(err?.message || "").includes("401")) {
        router.replace("/login");
        return;
      }
      setError(err.message || "Failed to load report");
    } finally {
      reportInFlightRef.current = false;
      if (requestSeq === reportRequestSeqRef.current) {
        setLoading(false);
      }
    }
  }, [
    mainTab,
    activeTab,
    model,
    primaryStartDate,
    primaryEndDate,
    useClickDate,
    compareMode,
    compareModel,
    compareStartDate,
    compareEndDate,
    router,
  ]);

  const syncSpendData = useCallback(async (range?: { start_date?: string; end_date?: string }) => {
    if (syncingSpendRef.current) return null;

    // Background auto-sync stays short. Manual sync uses the selected report window.
    const syncEnd = range?.end_date || daysAgo(0);
    const syncStart = range?.start_date || daysAgo(7);
    syncingSpendRef.current = true;
    setSyncingSpend(true);

    try {
      const [spendResult, namesResult, stripeResult] = await Promise.allSettled([
        syncSpend({ platform: "all", start_date: syncStart, end_date: syncEnd }),
        syncAdNames("all"),
        syncStripe({ start_date: syncStart, end_date: syncEnd }),
      ]);
      const errors: string[] = [];
      const addSyncErrors = (scope: string, result: PromiseSettledResult<any>) => {
        if (result.status === "rejected") {
          errors.push(`${scope}: ${result.reason?.message || result.reason || "Sync failed"}`);
          return;
        }
        for (const item of result.value?.errors || []) {
          errors.push(`${scope}: ${item}`);
        }
      };
      addSyncErrors("Spend", spendResult);
      addSyncErrors("Names", namesResult);
      addSyncErrors("Stripe", stripeResult);
      setSyncErrors(errors);
      setLastAutoSyncAt(new Date().toISOString());
      return spendResult.status === "fulfilled" ? spendResult.value : null;
    } catch (err) {
      console.warn("Auto sync failed:", err);
      return null;
    } finally {
      syncingSpendRef.current = false;
      setSyncingSpend(false);
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    const checkAuth = async () => {
      try {
        const status = await fetchAuthMe();
        if (cancelled) return;
        if (!status?.authenticated) {
          router.replace("/login");
          return;
        }
        setAuthEnabled(Boolean(status?.auth_enabled));
        setAuthUser(String(status?.user?.username || ""));
        setAuthChecked(true);
      } catch {
        if (!cancelled) router.replace("/login");
      }
    };
    void checkAuth();
    return () => {
      cancelled = true;
    };
  }, [router]);

  useEffect(() => {
    loadReportRef.current = loadReport;
  }, [loadReport]);

  useEffect(() => {
    mainTabRef.current = mainTab;
  }, [mainTab]);

  useEffect(() => {
    if (!authChecked) return;

    const runBackgroundSync = async () => {
      await syncSpendData();
      // Silently refresh report after sync completes
      await loadReportRef.current();
    };

    // Sync in the background, then refresh the current report after sync completes.
    void runBackgroundSync();

    spendSyncTimerRef.current = setInterval(() => {
      void runBackgroundSync();
    }, 10 * 60_000);

    return () => {
      if (spendSyncTimerRef.current) {
        clearInterval(spendSyncTimerRef.current);
        spendSyncTimerRef.current = null;
      }
    };
  }, [authChecked, syncSpendData]);

  useEffect(() => {
    if (!authChecked) return;
    loadReport();
  }, [loadReport, authChecked]);

  // Auto-refresh every 30s — only on the attribution tab, paused while the tab is
  // hidden, and never while a report request is already in flight.
  useEffect(() => {
    if (!autoRefresh || !authChecked || mainTab !== "attribution") return;

    const tick = () => {
      if (typeof document !== "undefined" && document.hidden) return;
      if (reportInFlightRef.current) return;
      void loadReport();
    };

    refreshTimerRef.current = setInterval(tick, 30_000);

    const onVisibilityChange = () => {
      if (typeof document !== "undefined" && !document.hidden) {
        // Became visible again — do one immediate refetch.
        if (!reportInFlightRef.current) void loadReport();
      }
    };
    if (typeof document !== "undefined") {
      document.addEventListener("visibilitychange", onVisibilityChange);
    }

    return () => {
      if (refreshTimerRef.current) {
        clearInterval(refreshTimerRef.current);
        refreshTimerRef.current = null;
      }
      if (typeof document !== "undefined") {
        document.removeEventListener("visibilitychange", onVisibilityChange);
      }
    };
  }, [autoRefresh, loadReport, authChecked, mainTab]);

  // WebSocket for real-time events
  useEffect(() => {
    if (!authChecked) return;
    const managed = createWebSocket((data: LiveEvent) => {
      const eventId = (liveEventSeqRef.current += 1);
      setLiveEvents((prev) => [...prev.slice(-49), { ...data, _id: eventId }]);
      // Coalesce bursts of new_order events into a single trailing refetch
      // ~1s after the last event (instead of one timer per event).
      if (data.type === "new_order") {
        if (wsRefetchTimerRef.current) clearTimeout(wsRefetchTimerRef.current);
        wsRefetchTimerRef.current = setTimeout(() => {
          wsRefetchTimerRef.current = null;
          if (mainTabRef.current !== "attribution") return;
          void loadReportRef.current();
        }, 1000);
      }
    });
    if (managed) {
      wsRef.current = managed;
      managed.socket.onopen = () => setWsConnected(true);
      managed.socket.onclose = () => setWsConnected(false);
    }
    return () => {
      if (wsRefetchTimerRef.current) {
        clearTimeout(wsRefetchTimerRef.current);
        wsRefetchTimerRef.current = null;
      }
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
    };
  }, [authChecked]);

  const handleLogout = async () => {
    try {
      await logoutApi();
    } finally {
      router.replace("/login");
    }
  };

  const handleTabChange = useCallback((tab: string) => {
    setActiveTab(tab);
    setPlatformFilter("all");
  }, []);

  const [windowStart, windowEnd] = normalizeDateRange(primaryStartDate, primaryEndDate);
  const activeWindowLabel = `${windowStart} to ${windowEnd}`;

  // Memoize the derived arrays handed to the heavy (memoized) report children so a
  // liveEvents state update (WS message) does not re-render the report subtree.
  const timeSeries = useMemo(() => report?.charts?.time_series || EMPTY, [report]);
  const compareTimeSeries = useMemo(() => compareReport?.charts?.time_series || EMPTY, [compareReport]);
  const platformRows = useMemo(() => report?.platform_comparison?.rows || EMPTY, [report]);
  const comparePlatformRows = useMemo(() => compareReport?.platform_comparison?.rows || EMPTY, [compareReport]);
  const funnelRows = useMemo(() => report?.funnels?.rows || EMPTY, [report]);
  const compareFunnelRows = useMemo(() => compareReport?.funnels?.rows || EMPTY, [compareReport]);
  const tableRows = useMemo(() => report?.table?.rows || EMPTY, [report]);
  const compareTableRows = useMemo(() => compareReport?.table?.rows || EMPTY, [compareReport]);

  if (!authChecked) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <RefreshCw size={24} className="animate-spin text-brand-500" />
      </div>
    );
  }

  return (
    <div className="min-h-screen">
      {/* Header */}
      <header className="sticky top-0 z-50 border-b border-[var(--card-border)] bg-[var(--background)]/80 backdrop-blur-xl">
        <div className="max-w-[1600px] mx-auto px-4 sm:px-6 py-3 flex items-center gap-4">
          <div className="flex items-center gap-2">
            <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-brand-500 to-purple-600 flex items-center justify-center">
              <Zap size={16} className="text-white" />
            </div>
            <div>
              <h1 className="text-sm font-bold text-white tracking-tight">Mini Hyros</h1>
              <p className="text-[10px] text-gray-500">Attribution Dashboard</p>
            </div>
          </div>

          {/* Controls */}
          <div className="ml-auto flex flex-wrap items-end gap-2">
            {/* Date range */}
            <div className="flex items-end gap-1.5">
              <div>
                <div className="text-[10px] text-gray-500 mb-1">From</div>
                <input type="date" value={primaryStartDate} onChange={(e) => setPrimaryStartDate(e.target.value)}
                  className="bg-[var(--card)] border border-[var(--card-border)] rounded-lg px-2.5 py-1.5 text-xs text-gray-300 focus:outline-none focus:border-brand-500 w-[130px]"
                />
              </div>
              <div>
                <div className="text-[10px] text-gray-500 mb-1">To</div>
                <input type="date" value={primaryEndDate} onChange={(e) => setPrimaryEndDate(e.target.value)}
                  className="bg-[var(--card)] border border-[var(--card-border)] rounded-lg px-2.5 py-1.5 text-xs text-gray-300 focus:outline-none focus:border-brand-500 w-[130px]"
                />
              </div>
            </div>

            {/* Attribution model */}
            <div>
              <div className="text-[10px] text-gray-500 mb-1">Model</div>
              <select value={model} onChange={(e) => setModel(e.target.value)}
                className="bg-[var(--card)] border border-[var(--card-border)] rounded-lg px-2.5 py-1.5 text-xs text-gray-300 focus:outline-none focus:border-brand-500 w-[130px]"
              >
                {MODELS.map((m) => <option key={m.value} value={m.value}>{m.label}</option>)}
              </select>
            </div>

            <div>
              <div className="text-[10px] text-gray-500 mb-1">Basis</div>
              <select
                value={useClickDate ? "click" : "conversion"}
                onChange={(e) => setUseClickDate(e.target.value === "click")}
                className="bg-[var(--card)] border border-[var(--card-border)] rounded-lg px-2.5 py-1.5 text-xs text-gray-300 focus:outline-none focus:border-brand-500 w-[145px]"
                aria-label="Attribution date basis"
              >
                <option value="conversion">Conversion Date</option>
                <option value="click">Click Date</option>
              </select>
            </div>

            {/* Sync + Refresh */}
            <button
              onClick={() => setAutoRefresh((enabled) => !enabled)}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-semibold transition-colors h-[34px] ${
                autoRefresh
                  ? "bg-emerald-500/15 text-emerald-300 hover:bg-emerald-500/20"
                  : "bg-white/5 text-gray-400 hover:bg-white/10"
              }`}
              title={autoRefresh ? "Disable live refresh" : "Enable live refresh"}
              aria-pressed={autoRefresh}
            >
              <RefreshCw size={12} />
              {autoRefresh ? "Live On" : "Live Off"}
            </button>

            <button
              onClick={async () => { await syncSpendData({ start_date: windowStart, end_date: windowEnd }); await loadReport(); }}
              disabled={syncingSpend}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-brand-600 hover:bg-brand-700 text-white text-xs font-semibold transition-colors disabled:opacity-50 h-[34px]"
            >
              <RefreshCw size={12} className={syncingSpend ? "animate-spin" : ""} />
              {syncingSpend ? "Syncing..." : "Sync"}
            </button>

            {authEnabled && (
              <button onClick={handleLogout}
                className="flex items-center gap-1 px-3 py-1.5 rounded-lg bg-white/5 hover:bg-white/10 text-gray-400 text-xs font-medium transition-colors h-[34px]"
                title={authUser ? `Logout ${authUser}` : "Logout"}
              >
                <LogOut size={12} />
              </button>
            )}
          </div>
        </div>
      </header>

      {/* Tab Navigation */}
      <nav className="border-b border-[var(--card-border)] bg-[var(--background)]">
        <div className="max-w-[1600px] mx-auto px-4 sm:px-6 flex gap-1 overflow-x-auto">
          {[
            { key: "attribution", label: "Attribution", icon: <BarChart3 size={13} /> },
            { key: "funnel", label: "Funnel", icon: <Filter size={13} /> },
            { key: "ltv", label: "LTV", icon: <TrendingUp size={13} /> },
            { key: "journey", label: "Journey", icon: <Route size={13} /> },
            { key: "cohort", label: "Cohorts", icon: <Grid3x3 size={13} /> },
            { key: "capi", label: "CAPI Sync", icon: <Send size={13} /> },
            { key: "spend", label: "Spend", icon: <DollarSign size={13} /> },
            { key: "names", label: "Ad Names", icon: <Tag size={13} /> },
          ].map((tab) => (
            <button
              key={tab.key}
              onClick={() => setMainTab(tab.key)}
              className={`flex items-center gap-1.5 px-3 py-2.5 text-xs font-medium border-b-2 transition-colors whitespace-nowrap ${
                mainTab === tab.key
                  ? "border-brand-500 text-brand-400"
                  : "border-transparent text-gray-500 hover:text-gray-300"
              }`}
            >
              {tab.icon} {tab.label}
            </button>
          ))}
        </div>
      </nav>

      {/* Main content */}
      <main className="max-w-[1600px] mx-auto px-4 sm:px-6 py-6 space-y-6">
        {error && (
          <div className="rounded-xl border border-red-500/30 bg-red-500/5 p-4 text-sm text-red-400">
            {error} — Check API URL, CORS, and backend logs.
          </div>
        )}

        {/* Feature Tabs */}
        {mainTab === "funnel" && <FunnelPanel />}
        {mainTab === "ltv" && <LtvPanel />}
        {mainTab === "journey" && <JourneyPanel startDate={windowStart} endDate={windowEnd} />}
        {mainTab === "cohort" && <CohortPanel />}
        {mainTab === "capi" && <CapiPanel />}
        {mainTab === "spend" && (
          <SpendImportPanel startDate={windowStart} endDate={windowEnd} onImported={loadReport} />
        )}
        {mainTab === "names" && <AdNamesPanel />}

        {/* Sync status bar */}
        {mainTab === "attribution" && (
          <div className="flex items-center gap-3 text-[11px]">
            <span className="text-gray-600">
              {syncingSpend ? (
                <span className="text-brand-400 flex items-center gap-1"><RefreshCw size={10} className="animate-spin" /> Syncing all platforms...</span>
              ) : lastAutoSyncAt ? (
                <span className="text-gray-500">Last sync: {new Date(lastAutoSyncAt).toLocaleTimeString()} · Auto every 10m</span>
              ) : (
                <span className="text-gray-600">Syncing on load...</span>
              )}
            </span>
            {syncErrors.length > 0 && (
              <div className="min-w-0 flex-1 max-h-24 overflow-auto rounded-lg border border-yellow-500/25 bg-yellow-500/10 px-2.5 py-2 text-[10px] text-yellow-300">
                <div className="font-semibold uppercase tracking-wide">Sync needs attention</div>
                <div className="mt-1 space-y-1">
                  {syncErrors.map((syncError, index) => (
                    <div key={`${syncError}-${index}`} className="break-words">
                      {compactSyncError(syncError)}
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}

        {mainTab === "attribution" && report && (
          <>
            <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)]/70 p-3">
              <div className="flex flex-wrap items-center gap-2 text-[11px]">
                <span className="px-2 py-1 rounded bg-white/5 text-gray-300">Model: {modelLabel(model)}</span>
                <span className="px-2 py-1 rounded bg-white/5 text-gray-300">Window: {activeWindowLabel}</span>
                <span className="px-2 py-1 rounded bg-white/5 text-gray-300">Basis: {useClickDate ? "Click Date" : "Conversion Date"}</span>
              </div>
            </div>

            {/* Summary KPI Cards */}
            <SummaryCards
              totals={report.summary_totals}
              compareTotals={compareReport?.summary_totals}
              compareLabel={compareLabel}
              showCompareBanner={false}
            />

            {/* Main attribution grid */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 items-start">
              <div className="lg:col-span-2 space-y-6">
                <PlatformComparisonTable
                  rows={platformRows}
                  compareRows={comparePlatformRows}
                  compareLabel={compareLabel}
                />

                <PerformanceChart
                  data={timeSeries}
                  compareData={compareTimeSeries}
                  compareLabel={compareLabel}
                />
                <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
                  <TrafficValueChart
                    data={timeSeries}
                    compareData={compareTimeSeries}
                    compareLabel={compareLabel}
                  />
                  <CumulativePerformanceChart
                    data={timeSeries}
                    compareData={compareTimeSeries}
                    compareLabel={compareLabel}
                  />
                </div>
                <PlatformMixChart
                  rows={platformRows}
                  compareRows={comparePlatformRows}
                  compareLabel={compareLabel}
                />
              </div>
              <div className="space-y-4">
                <FunnelSnapshotTable
                  rows={funnelRows}
                  compareRows={compareFunnelRows}
                  compareLabel={compareLabel}
                />
                <TrackingHealth
                  tracking={report.tracking}
                  freshness={report.diagnostics?.data_freshness}
                  wsConnected={wsConnected}
                />
                <LiveFeed events={liveEvents} connected={wsConnected} />
              </div>
            </div>

            <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)]/70 p-3">
              <div className="flex flex-wrap items-end gap-2">
                <div className="min-w-[170px]">
                  <div className="text-[10px] text-gray-500 uppercase tracking-wide mb-1">Compare</div>
                  <select
                    value={compareMode}
                    onChange={(e) => setCompareMode(e.target.value as CompareMode)}
                    className="w-full bg-[var(--card)] border border-[var(--card-border)] rounded-lg px-2.5 py-1.5 text-xs text-gray-300 focus:outline-none focus:border-brand-500"
                  >
                    <option value="none">No Comparison</option>
                    <option value="previous_period">Previous Period</option>
                    <option value="custom_range">Custom Range</option>
                    <option value="model">Another Model</option>
                  </select>
                </div>

                {compareMode === "custom_range" && (
                  <div className="min-w-[290px]">
                    <div className="text-[10px] text-gray-500 uppercase tracking-wide mb-1">Comparison Range</div>
                    <div className="grid grid-cols-2 gap-2">
                      <input
                        type="date"
                        value={compareStartDate}
                        onChange={(e) => setCompareStartDate(e.target.value)}
                        className="w-full bg-[var(--card)] border border-[var(--card-border)] rounded-lg px-2.5 py-1.5 text-xs text-gray-300 focus:outline-none focus:border-brand-500"
                        aria-label="Comparison start date"
                      />
                      <input
                        type="date"
                        value={compareEndDate}
                        onChange={(e) => setCompareEndDate(e.target.value)}
                        className="w-full bg-[var(--card)] border border-[var(--card-border)] rounded-lg px-2.5 py-1.5 text-xs text-gray-300 focus:outline-none focus:border-brand-500"
                        aria-label="Comparison end date"
                      />
                    </div>
                  </div>
                )}

                {compareMode === "model" && (
                  <div className="min-w-[170px]">
                    <div className="text-[10px] text-gray-500 uppercase tracking-wide mb-1">Comparison Model</div>
                    <select
                      value={compareModel}
                      onChange={(e) => setCompareModel(e.target.value)}
                      className="w-full bg-[var(--card)] border border-[var(--card-border)] rounded-lg px-2.5 py-1.5 text-xs text-gray-300 focus:outline-none focus:border-brand-500"
                    >
                      {MODELS.map((m) => (
                        <option key={m.value} value={m.value}>{m.label}</option>
                      ))}
                    </select>
                  </div>
                )}

                <div className="ml-auto text-[11px]">
                  <span className={`px-2 py-1 rounded ${compareReport ? "bg-blue-500/15 text-blue-300" : "bg-white/5 text-gray-500"}`}>
                    {compareReport ? `Comparing vs ${compareLabel}` : "Comparison Off"}
                  </span>
                </div>
              </div>
            </div>

            {/* Attribution Table */}
            <AttributionTable
              columns={report.table.columns}
              rows={tableRows}
              totals={report.table.totals_row}
              compareRows={compareTableRows}
              compareLabel={compareLabel}
              activeTab={activeTab}
              onTabChange={handleTabChange}
              startDate={windowStart}
              endDate={windowEnd}
              model={model}
              lookbackDays={30}
              useClickDate={useClickDate}
              platformFilter={platformFilter}
              onPlatformFilterChange={setPlatformFilter}
            />

            {/* Top Winners & Losers */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-4">
                <h3 className="text-sm font-semibold text-emerald-400 mb-3 flex items-center gap-2">
                  <BarChart3 size={14} /> Top Winners
                </h3>
                <div className="space-y-2">
                  {(report.charts?.top_winners || []).map((w: any, i: number) => (
                    <div key={i} className="flex items-center justify-between text-xs">
                      <span className="text-gray-300 truncate max-w-[200px]">{w.name}</span>
                      <span className="text-emerald-400 font-medium">
                        ${Number(w.profit).toLocaleString(undefined, { minimumFractionDigits: 2 })}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
              <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-4">
                <h3 className="text-sm font-semibold text-red-400 mb-3 flex items-center gap-2">
                  <BarChart3 size={14} /> Top Losers
                </h3>
                <div className="space-y-2">
                  {(report.charts?.top_losers || []).map((w: any, i: number) => (
                    <div key={i} className="flex items-center justify-between text-xs">
                      <span className="text-gray-300 truncate max-w-[200px]">{w.name}</span>
                      <span className="text-red-400 font-medium">
                        ${Number(w.profit).toLocaleString(undefined, { minimumFractionDigits: 2 })}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            {/* Action Plan */}
            {report.action_plan && report.action_plan.length > 0 && (
              <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-4">
                <h3 className="text-sm font-semibold text-gray-300 mb-3 flex items-center gap-2">
                  <Settings size={14} /> Recommended Actions
                </h3>
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                  {report.action_plan.map((a: any, i: number) => (
                    <div
                      key={i}
                      className="rounded-lg border border-[var(--card-border)] p-3 bg-white/[0.01]"
                    >
                      <div className="text-xs font-semibold text-gray-200 mb-1">{a.title}</div>
                      <div className="text-[11px] text-gray-500 mb-2">{a.why}</div>
                      <div className="flex gap-2 text-[10px]">
                        <span className={`px-1.5 py-0.5 rounded ${a.expected_impact === "high" ? "bg-emerald-500/10 text-emerald-400" : "bg-yellow-500/10 text-yellow-400"}`}>
                          Impact: {a.expected_impact}
                        </span>
                        <span className="px-1.5 py-0.5 rounded bg-blue-500/10 text-blue-400">
                          Effort: {a.effort}
                        </span>
                      </div>
                      <ul className="mt-2 space-y-0.5">
                        {a.steps.map((s: string, j: number) => (
                          <li key={j} className="text-[11px] text-gray-500 flex gap-1">
                            <span className="text-gray-600">•</span> {s}
                          </li>
                        ))}
                      </ul>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Diagnostics */}
            {report.diagnostics && (
              <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-4 text-xs text-gray-500">
                <h3 className="text-sm font-semibold text-gray-400 mb-2">Diagnostics</h3>
                <div className="space-y-1">
                  <p>Model: {report.report_meta?.attribution_model} | Lookback: {report.report_meta?.filters_applied?.lookback_days}d | Date basis: {report.report_meta?.use_date_of_click_attribution ? "Click" : "Conversion"}</p>
                  <p>Last event: {report.diagnostics.data_freshness?.last_event_ts || "—"} | Last spend: {report.diagnostics.data_freshness?.last_spend_ts || "—"}</p>
                  {report.diagnostics.anomalies?.map((a: any, i: number) => (
                    <p key={i} className="text-yellow-500">⚠ {a.what} — {a.likely_cause}</p>
                  ))}
                </div>
              </div>
            )}
          </>
        )}

        {mainTab === "attribution" && loading && !report && (
          <div className="flex items-center justify-center py-24">
            <RefreshCw size={24} className="animate-spin text-brand-500" />
          </div>
        )}
      </main>
    </div>
  );
}
