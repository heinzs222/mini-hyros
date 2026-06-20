"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import dynamic from "next/dynamic";
import { fetchReport, createWebSocket, fetchAuthMe, logout as logoutApi, syncSpend, syncAdNames, syncStripe, type ManagedWebSocket } from "@/lib/api";
import { daysAgo } from "@/lib/utils";
import Sidebar, { Section } from "@/components/Sidebar";
import DateRangePicker from "@/components/DateRangePicker";
import DashboardView from "@/components/DashboardView";
import LeadsView from "@/components/LeadsView";
import ConnectionsView from "@/components/ConnectionsView";
import SummaryCards from "@/components/SummaryCards";
import AttributionTable from "@/components/AttributionTable";
import TrackingHealth from "@/components/TrackingHealth";
import PlatformComparisonTable from "@/components/PlatformComparisonTable";
import FunnelSnapshotTable from "@/components/FunnelSnapshotTable";
import LiveFeed from "@/components/LiveFeed";
import ModelSelect from "@/components/ModelSelect";
import { useToast } from "@/components/Toast";
import {
  BarChart3,
  DollarSign,
  RefreshCw,
  Settings,
  TrendingUp,
  Filter,
  Route,
  Grid3x3,
  Send,
  Tag,
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

function previousPeriod(startIso: string, endIso: string): { start: string; end: string } {
  const spanDays = inclusiveSpanDays(startIso, endIso);
  const previousEnd = shiftIsoDate(startIso, -1);
  const previousStart = shiftIsoDate(previousEnd, -(spanDays - 1));
  return { start: previousStart, end: previousEnd };
}

function monthDayLabel(iso: string): string {
  if (!iso) return "";
  return new Date(`${iso}T00:00:00`).toLocaleDateString("en-US", { month: "short", day: "numeric" });
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

const REPORT_TABS = [
  { key: "attribution", label: "Attribution", icon: <BarChart3 size={14} /> },
  { key: "funnel", label: "Funnel", icon: <Filter size={14} /> },
  { key: "ltv", label: "LTV", icon: <TrendingUp size={14} /> },
  { key: "journey", label: "Journey", icon: <Route size={14} /> },
  { key: "cohort", label: "Cohorts", icon: <Grid3x3 size={14} /> },
  { key: "capi", label: "CAPI Sync", icon: <Send size={14} /> },
  { key: "spend", label: "Spend", icon: <DollarSign size={14} /> },
  { key: "names", label: "Ad Names", icon: <Tag size={14} /> },
];

const SECTION_TITLES: Record<Section, string> = {
  dashboard: "Dashboard",
  reports: "Performance Report",
  leads: "",
  settings: "Settings",
};

export default function DashboardPage() {
  const router = useRouter();
  const toast = useToast();
  const toastRef = useRef(toast);
  useEffect(() => { toastRef.current = toast; });
  const [report, setReport] = useState<any>(null);
  const [compareReport, setCompareReport] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [authChecked, setAuthChecked] = useState(false);
  const [authEnabled, setAuthEnabled] = useState(false);
  const [authUser, setAuthUser] = useState("");

  const [section, setSection] = useState<Section>("dashboard");
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);

  const [activeTab, setActiveTab] = useState("campaign");
  const [model, setModel] = useState("last_click");
  const [compareEnabled, setCompareEnabled] = useState(true);
  const [autoCompare, setAutoCompare] = useState(true);
  const [compareMode, setCompareMode] = useState<CompareMode>("previous_period");
  const [compareModel, setCompareModel] = useState("first_click");
  const [compareLabel, setCompareLabel] = useState("");
  const [primaryStartDate, setPrimaryStartDate] = useState(daysAgo(6));
  const [primaryEndDate, setPrimaryEndDate] = useState(daysAgo(0));
  const [compareStartDate, setCompareStartDate] = useState(daysAgo(13));
  const [compareEndDate, setCompareEndDate] = useState(daysAgo(7));
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

  const effectiveCompareMode: CompareMode = !compareEnabled
    ? "none"
    : autoCompare
    ? "previous_period"
    : compareMode === "none"
    ? "previous_period"
    : compareMode;

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

      if (effectiveCompareMode === "previous_period") {
        const { start: previousStart, end: previousEnd } = previousPeriod(startDate, endDate);
        comparePromise = fetchReport({
          start_date: previousStart,
          end_date: previousEnd,
          model,
          active_tab: activeTab,
          use_click_date: useClickDate,
        });
        nextCompareLabel = `${previousStart} to ${previousEnd}`;
      } else if (effectiveCompareMode === "custom_range") {
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
      } else if (effectiveCompareMode === "model" && compareModel !== model) {
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
    effectiveCompareMode,
    compareModel,
    compareStartDate,
    compareEndDate,
    router,
  ]);

  const syncSpendData = useCallback(async (
    range?: { start_date?: string; end_date?: string },
    opts?: { notify?: boolean },
  ) => {
    if (syncingSpendRef.current) return null;

    const notify = Boolean(opts?.notify);
    const syncEnd = range?.end_date || daysAgo(0);
    const syncStart = range?.start_date || daysAgo(7);
    syncingSpendRef.current = true;
    setSyncingSpend(true);
    const toastId = notify
      ? toastRef.current.loading("Syncing all platforms…", { description: "Pulling ad spend, ad names and Stripe orders." })
      : 0;

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

      if (notify) {
        if (errors.length === 0) {
          toastRef.current.update(toastId, {
            type: "success",
            title: "Sync complete",
            description: "Ad spend, ad names and Stripe orders are up to date.",
            duration: 4500,
          });
        } else {
          const detail =
            errors.slice(0, 4).map(compactSyncError).join("\n") +
            (errors.length > 4 ? `\n+${errors.length - 4} more issue${errors.length - 4 === 1 ? "" : "s"}…` : "");
          toastRef.current.update(toastId, {
            type: "error",
            title: `Sync finished with ${errors.length} issue${errors.length === 1 ? "" : "s"}`,
            description: detail,
            duration: 13000,
          });
        }
      }
      return spendResult.status === "fulfilled" ? spendResult.value : null;
    } catch (err: any) {
      console.warn("Auto sync failed:", err);
      if (notify) {
        toastRef.current.update(toastId, {
          type: "error",
          title: "Sync failed",
          description: compactSyncError(err?.message || String(err) || "Could not reach the sync service."),
          duration: 12000,
        });
      }
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
      await loadReportRef.current();
    };

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

  const compareRange =
    effectiveCompareMode === "previous_period"
      ? previousPeriod(windowStart, windowEnd)
      : effectiveCompareMode === "custom_range"
      ? { start: compareStartDate, end: compareEndDate }
      : null;
  const compareCaption = compareRange ? monthDayLabel(compareRange.start) : undefined;

  const setRange = (range: { start: string; end: string }) => {
    setPrimaryStartDate(range.start);
    setPrimaryEndDate(range.end);
  };

  if (!authChecked) {
    return (
      <div className="flex min-h-screen items-center justify-center">
        <RefreshCw size={24} className="animate-spin text-brand-500" />
      </div>
    );
  }

  const showReportControls = section === "reports";
  const showDateControls = section === "dashboard" || section === "reports" || section === "leads";

  return (
    <div className="flex h-screen overflow-hidden bg-[var(--background)]">
      <Sidebar
        section={section}
        onSectionChange={setSection}
        collapsed={sidebarCollapsed}
        onToggleCollapse={() => setSidebarCollapsed((c) => !c)}
        userName={authUser || "Account"}
        authEnabled={authEnabled}
        onLogout={handleLogout}
      />

      <main className="flex-1 overflow-y-auto">
        {/* Top controls strip */}
        <div className="sticky top-0 z-40 border-b border-[var(--card-border)] bg-[var(--background)]/85 backdrop-blur-xl">
          <div className="flex flex-wrap items-center gap-3 px-6 py-3">
            <h1 className="h-title text-[26px]">
              {SECTION_TITLES[section]}
            </h1>

            <div className="ml-auto flex flex-wrap items-center gap-2">
              {showDateControls && (
                <DateRangePicker
                  value={{ start: primaryStartDate, end: primaryEndDate }}
                  onChange={setRange}
                  compareRange={compareRange}
                  compareEnabled={compareEnabled}
                  onCompareEnabledChange={setCompareEnabled}
                  autoCompare={autoCompare}
                  onAutoCompareChange={setAutoCompare}
                  showCompareControls={section !== "leads"}
                />
              )}

              {showReportControls && (
                <>
                  <ModelSelect value={model} onChange={setModel} />
                  <select
                    value={useClickDate ? "click" : "conversion"}
                    onChange={(e) => setUseClickDate(e.target.value === "click")}
                    className="h-[34px] rounded-lg border border-[var(--card-border)] bg-[var(--surface-2)] px-2.5 text-[13px] text-ink focus:border-brand-500 focus:outline-none"
                    aria-label="Attribution date basis"
                  >
                    <option value="conversion">Conversion Date</option>
                    <option value="click">Click Date</option>
                  </select>
                </>
              )}

              {section !== "leads" && section !== "settings" && (
                <button
                  onClick={() => setAutoRefresh((v) => !v)}
                  className={`flex h-[34px] items-center gap-1.5 rounded-lg px-3 text-[13px] font-medium transition-colors ${
                    autoRefresh
                      ? "bg-emerald-500/15 text-emerald-300 hover:bg-emerald-500/20"
                      : "bg-white/5 text-ink-dim hover:bg-white/10"
                  }`}
                  title={autoRefresh ? "Disable live refresh" : "Enable live refresh"}
                >
                  <RefreshCw size={13} /> {autoRefresh ? "Live On" : "Live Off"}
                </button>
              )}

              {section !== "leads" && section !== "settings" && (
                <button
                  onClick={async () => { await syncSpendData({ start_date: windowStart, end_date: windowEnd }, { notify: true }); await loadReport(); }}
                  disabled={syncingSpend}
                  className="flex h-[34px] items-center gap-1.5 rounded-lg bg-brand-600 px-3 text-[13px] font-semibold text-white transition-colors hover:bg-brand-700 disabled:opacity-50"
                >
                  <RefreshCw size={13} className={syncingSpend ? "animate-spin" : ""} />
                  {syncingSpend ? "Syncing..." : "Sync"}
                </button>
              )}

            </div>
          </div>
        </div>

        <div className="px-6 py-6">
          {error && (
            <div className="mb-5 rounded-xl border border-rose-500/30 bg-rose-500/5 p-4 text-sm text-rose-400">
              {error} — Check API URL, CORS, and backend logs.
            </div>
          )}

          {/* ───────── Dashboard ───────── */}
          {section === "dashboard" && (
            report ? (
              <DashboardView report={report} compareReport={compareReport} compareCaption={compareCaption} />
            ) : (
              <div className="flex items-center justify-center py-24">
                <RefreshCw size={24} className="animate-spin text-brand-500" />
              </div>
            )
          )}

          {/* ───────── Leads ───────── */}
          {section === "leads" && <LeadsView startDate={windowStart} endDate={windowEnd} />}

          {/* ───────── Settings ───────── */}
          {section === "settings" && <ConnectionsView />}

          {/* ───────── Reports ───────── */}
          {section === "reports" && (
            <div className="space-y-5">
              {/* Report sub-tabs */}
              <div className="flex items-center gap-1 overflow-x-auto border-b border-[var(--card-border)]">
                {REPORT_TABS.map((tab) => (
                  <button
                    key={tab.key}
                    onClick={() => setMainTab(tab.key)}
                    className={`-mb-px flex items-center gap-1.5 whitespace-nowrap border-b-2 px-3 py-2.5 text-[13px] transition-colors ${
                      mainTab === tab.key
                        ? "border-brand-500 font-medium text-ink-bright"
                        : "border-transparent text-ink-dim hover:text-ink"
                    }`}
                  >
                    {tab.icon} {tab.label}
                  </button>
                ))}
              </div>

              {mainTab === "funnel" && <FunnelPanel />}
              {mainTab === "ltv" && <LtvPanel />}
              {mainTab === "journey" && <JourneyPanel startDate={windowStart} endDate={windowEnd} />}
              {mainTab === "cohort" && <CohortPanel />}
              {mainTab === "capi" && <CapiPanel />}
              {mainTab === "spend" && (
                <SpendImportPanel startDate={windowStart} endDate={windowEnd} onImported={loadReport} />
              )}
              {mainTab === "names" && <AdNamesPanel />}

              {mainTab === "attribution" && (
                <>
                  {/* Sync status */}
                  <div className="flex items-center gap-3 text-[11px]">
                    <span className="text-ink-faint">
                      {syncingSpend ? (
                        <span className="flex items-center gap-1 text-brand-400"><RefreshCw size={10} className="animate-spin" /> Syncing all platforms...</span>
                      ) : lastAutoSyncAt ? (
                        <span className="text-ink-dim">Last sync: {new Date(lastAutoSyncAt).toLocaleTimeString()} · Auto every 10m</span>
                      ) : (
                        <span className="text-ink-faint">Syncing on load...</span>
                      )}
                    </span>
                    {syncErrors.length > 0 && (
                      <div className="max-h-24 min-w-0 flex-1 overflow-auto rounded-lg border border-yellow-500/25 bg-yellow-500/10 px-2.5 py-2 text-[10px] text-yellow-300">
                        <div className="font-semibold uppercase tracking-wide">Sync needs attention</div>
                        <div className="mt-1 space-y-1">
                          {syncErrors.map((syncError, index) => (
                            <div key={`${syncError}-${index}`} className="break-words">{compactSyncError(syncError)}</div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>

                  {report && (
                    <>
                      <div className="flex flex-wrap items-center gap-2 text-[11px]">
                        <span className="rounded-md bg-white/5 px-2 py-1 text-ink-dim">Model: {modelLabel(model)}</span>
                        <span className="rounded-md bg-white/5 px-2 py-1 text-ink-dim">Window: {activeWindowLabel}</span>
                        <span className="rounded-md bg-white/5 px-2 py-1 text-ink-dim">Basis: {useClickDate ? "Click Date" : "Conversion Date"}</span>
                      </div>

                      <SummaryCards
                        totals={report.summary_totals}
                        compareTotals={compareReport?.summary_totals}
                        compareLabel={compareLabel}
                        showCompareBanner={false}
                      />

                      <div className="grid grid-cols-1 items-start gap-6 lg:grid-cols-3">
                        <div className="space-y-6 lg:col-span-2">
                          <PlatformComparisonTable
                            rows={report.platform_comparison?.rows || []}
                            compareRows={compareReport?.platform_comparison?.rows || []}
                            compareLabel={compareLabel}
                          />
                          <PerformanceChart
                            data={report.charts?.time_series || []}
                            compareData={compareReport?.charts?.time_series || []}
                            compareLabel={compareLabel}
                          />
                          <div className="grid grid-cols-1 gap-6 xl:grid-cols-2">
                            <TrafficValueChart
                              data={report.charts?.time_series || []}
                              compareData={compareReport?.charts?.time_series || []}
                              compareLabel={compareLabel}
                            />
                            <CumulativePerformanceChart
                              data={report.charts?.time_series || []}
                              compareData={compareReport?.charts?.time_series || []}
                              compareLabel={compareLabel}
                            />
                          </div>
                          <PlatformMixChart
                            rows={report.platform_comparison?.rows || []}
                            compareRows={compareReport?.platform_comparison?.rows || []}
                            compareLabel={compareLabel}
                          />
                        </div>
                        <div className="space-y-4">
                          <FunnelSnapshotTable
                            rows={report.funnels?.rows || []}
                            compareRows={compareReport?.funnels?.rows || []}
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

                      {/* Advanced comparison controls */}
                      <div className="rounded-xl border border-[var(--card-border)] bg-[var(--surface)] p-3">
                        <div className="flex flex-wrap items-end gap-2">
                          <div className="min-w-[170px]">
                            <div className="mb-1 text-[10px] uppercase tracking-wide text-ink-dim">Compare</div>
                            <select
                              value={effectiveCompareMode}
                              onChange={(e) => {
                                const v = e.target.value as CompareMode;
                                if (v === "none") {
                                  setCompareEnabled(false);
                                } else {
                                  setCompareEnabled(true);
                                  setAutoCompare(v === "previous_period");
                                  setCompareMode(v);
                                }
                              }}
                              className="w-full rounded-lg border border-[var(--card-border)] bg-[var(--surface-2)] px-2.5 py-1.5 text-xs text-ink focus:border-brand-500 focus:outline-none"
                            >
                              <option value="none">No Comparison</option>
                              <option value="previous_period">Previous Period</option>
                              <option value="custom_range">Custom Range</option>
                              <option value="model">Another Model</option>
                            </select>
                          </div>

                          {effectiveCompareMode === "custom_range" && (
                            <div className="min-w-[290px]">
                              <div className="mb-1 text-[10px] uppercase tracking-wide text-ink-dim">Comparison Range</div>
                              <div className="grid grid-cols-2 gap-2">
                                <input type="date" value={compareStartDate} onChange={(e) => setCompareStartDate(e.target.value)}
                                  className="w-full rounded-lg border border-[var(--card-border)] bg-[var(--surface-2)] px-2.5 py-1.5 text-xs text-ink focus:border-brand-500 focus:outline-none" />
                                <input type="date" value={compareEndDate} onChange={(e) => setCompareEndDate(e.target.value)}
                                  className="w-full rounded-lg border border-[var(--card-border)] bg-[var(--surface-2)] px-2.5 py-1.5 text-xs text-ink focus:border-brand-500 focus:outline-none" />
                              </div>
                            </div>
                          )}

                          {effectiveCompareMode === "model" && (
                            <div className="min-w-[170px]">
                              <div className="mb-1 text-[10px] uppercase tracking-wide text-ink-dim">Comparison Model</div>
                              <select value={compareModel} onChange={(e) => setCompareModel(e.target.value)}
                                className="w-full rounded-lg border border-[var(--card-border)] bg-[var(--surface-2)] px-2.5 py-1.5 text-xs text-ink focus:border-brand-500 focus:outline-none">
                                {MODELS.map((m) => <option key={m.value} value={m.value}>{m.label}</option>)}
                              </select>
                            </div>
                          )}

                          <div className="ml-auto text-[11px]">
                            <span className={`rounded px-2 py-1 ${compareReport ? "bg-blue-500/15 text-blue-300" : "bg-white/5 text-ink-faint"}`}>
                              {compareReport ? `Comparing vs ${compareLabel}` : "Comparison Off"}
                            </span>
                          </div>
                        </div>
                      </div>

                      <AttributionTable
                        columns={report.table.columns}
                        rows={report.table.rows}
                        totals={report.table.totals_row}
                        compareRows={compareReport?.table?.rows || []}
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

                      <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
                        <div className="hpanel p-4">
                          <h3 className="mb-3 flex items-center gap-2 text-sm font-semibold text-emerald-400">
                            <BarChart3 size={14} /> Top Winners
                          </h3>
                          <div className="space-y-2">
                            {(report.charts?.top_winners || []).map((w: any, i: number) => (
                              <div key={i} className="flex items-center justify-between text-xs">
                                <span className="max-w-[200px] truncate text-ink">{w.name}</span>
                                <span className="font-medium text-emerald-400">
                                  ${Number(w.profit).toLocaleString(undefined, { minimumFractionDigits: 2 })}
                                </span>
                              </div>
                            ))}
                          </div>
                        </div>
                        <div className="hpanel p-4">
                          <h3 className="mb-3 flex items-center gap-2 text-sm font-semibold text-rose-400">
                            <BarChart3 size={14} /> Top Losers
                          </h3>
                          <div className="space-y-2">
                            {(report.charts?.top_losers || []).map((w: any, i: number) => (
                              <div key={i} className="flex items-center justify-between text-xs">
                                <span className="max-w-[200px] truncate text-ink">{w.name}</span>
                                <span className="font-medium text-rose-400">
                                  ${Number(w.profit).toLocaleString(undefined, { minimumFractionDigits: 2 })}
                                </span>
                              </div>
                            ))}
                          </div>
                        </div>
                      </div>

                      {report.action_plan && report.action_plan.length > 0 && (
                        <div className="hpanel p-4">
                          <h3 className="mb-3 flex items-center gap-2 text-sm font-semibold text-ink">
                            <Settings size={14} /> Recommended Actions
                          </h3>
                          <div className="grid grid-cols-1 gap-3 md:grid-cols-2 lg:grid-cols-3">
                            {report.action_plan.map((a: any, i: number) => (
                              <div key={i} className="rounded-lg border border-[var(--card-border)] bg-white/[0.01] p-3">
                                <div className="mb-1 text-xs font-semibold text-ink-bright">{a.title}</div>
                                <div className="mb-2 text-[11px] text-ink-dim">{a.why}</div>
                                <div className="flex gap-2 text-[10px]">
                                  <span className={`rounded px-1.5 py-0.5 ${a.expected_impact === "high" ? "bg-emerald-500/10 text-emerald-400" : "bg-yellow-500/10 text-yellow-400"}`}>
                                    Impact: {a.expected_impact}
                                  </span>
                                  <span className="rounded bg-blue-500/10 px-1.5 py-0.5 text-blue-400">Effort: {a.effort}</span>
                                </div>
                                <ul className="mt-2 space-y-0.5">
                                  {a.steps.map((s: string, j: number) => (
                                    <li key={j} className="flex gap-1 text-[11px] text-ink-dim">
                                      <span className="text-ink-faint">•</span> {s}
                                    </li>
                                  ))}
                                </ul>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}

                      {report.diagnostics && (
                        <div className="hpanel p-4 text-xs text-ink-dim">
                          <h3 className="mb-2 text-sm font-semibold text-ink">Diagnostics</h3>
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

                  {loading && !report && (
                    <div className="flex items-center justify-center py-24">
                      <RefreshCw size={24} className="animate-spin text-brand-500" />
                    </div>
                  )}
                </>
              )}
            </div>
          )}
        </div>
      </main>
    </div>
  );
}
