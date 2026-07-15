"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import dynamic from "next/dynamic";
import { fetchReport, createWebSocket, fetchAuthMe, logout as logoutApi, syncSpend, syncAdNames, syncStripe, syncGhl, type ManagedWebSocket } from "@/lib/api";
import { daysAgo } from "@/lib/utils";
import Sidebar, { Section } from "@/components/Sidebar";
import DateRangePicker from "@/components/DateRangePicker";
import DashboardView from "@/components/DashboardView";
import LeadsView from "@/components/LeadsView";
import ConnectionsView from "@/components/ConnectionsView";
import ReportsView from "@/components/ReportsView";
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
  Home,
  Plus,
  Target,
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
const CampaignTrackingPanel = dynamic(() => import("@/components/CampaignTrackingPanel"), { ssr: false, loading: PanelLoading });

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

function isAbortError(err: any): boolean {
  const message = String(err?.message || err || "");
  return err?.name === "AbortError" || message.includes("aborted");
}

// The browser used to auto-fire a full 4-platform sync (spend + ad names +
// Stripe + GHL leads) on load and every 10 minutes. On a modest backend that
// saturates the single worker and starves the report request, so the dashboard
// spins forever / "loads sometimes" and the ad-name/lead syncs time out. Syncing
// is now MANUAL by default (the "Sync" button); set NEXT_PUBLIC_AUTO_SYNC=1 to
// restore the automatic background sync only if your backend can spare the CPU.
const AUTO_SYNC =
  process.env.NEXT_PUBLIC_AUTO_SYNC === "1" ||
  process.env.NEXT_PUBLIC_AUTO_SYNC === "true";

async function withSyncDeadline<T>(label: string, timeoutMs: number, run: (signal: AbortSignal) => Promise<T>): Promise<T> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await run(controller.signal);
  } catch (err: any) {
    if (controller.signal.aborted || isAbortError(err)) {
      throw new Error(`${label} timed out after ${Math.round(timeoutMs / 1000)}s`);
    }
    throw err;
  } finally {
    clearTimeout(timeoutId);
  }
}

const REPORT_TABS = [
  { key: "attribution", label: "Attribution", icon: <BarChart3 size={14} /> },
  { key: "funnel", label: "Funnel", icon: <Filter size={14} /> },
  { key: "ltv", label: "LTV", icon: <TrendingUp size={14} /> },
  { key: "journey", label: "Journey", icon: <Route size={14} /> },
  { key: "cohort", label: "Cohorts", icon: <Grid3x3 size={14} /> },
  { key: "capi", label: "CAPI Sync", icon: <Send size={14} /> },
  { key: "spend", label: "Spend", icon: <DollarSign size={14} /> },
  { key: "names", label: "Ad Names", icon: <Tag size={14} /> },
  { key: "tracking", label: "Tracking", icon: <Target size={14} /> },
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
  const [compareUnavailable, setCompareUnavailable] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [authChecked, setAuthChecked] = useState(false);
  const [authEnabled, setAuthEnabled] = useState(false);
  const [authUser, setAuthUser] = useState("");
  // True while /api/auth/me is unreachable (e.g. Render cold start): we retry
  // rather than bouncing an authenticated user to /login.
  const [backendWaking, setBackendWaking] = useState(false);

  const [section, setSection] = useState<Section>("dashboard");
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);

  const [activeTab, setActiveTab] = useState("campaign");
  const [model, setModel] = useState("last_click");
  const [compareEnabled, setCompareEnabled] = useState(true);
  const [autoCompare, setAutoCompare] = useState(true);
  const [compareMode, setCompareMode] = useState<CompareMode>("previous_period");
  const [compareModel, setCompareModel] = useState("first_click");
  const [compareLabel, setCompareLabel] = useState("");
  const [primaryStartDate, setPrimaryStartDate] = useState(daysAgo(7));
  const [primaryEndDate, setPrimaryEndDate] = useState(daysAgo(1));
  const [compareStartDate, setCompareStartDate] = useState(daysAgo(14));
  const [compareEndDate, setCompareEndDate] = useState(daysAgo(8));
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
  const reportInFlightRef = useRef(false);
  const reportAbortRef = useRef<AbortController | null>(null);
  // Separate request/abort tracking for the deferred comparison fetch so it can
  // fail (or be superseded) without disturbing the primary report.
  const compareRequestSeqRef = useRef(0);
  const compareAbortRef = useRef<AbortController | null>(null);
  // Mirrors the currently-selected window so background sync covers what's on
  // screen without re-subscribing the sync interval on every range change.
  const syncRangeRef = useRef<{ start: string; end: string }>({ start: primaryStartDate, end: primaryEndDate });
  // Coalesces bursts of WS new_order events into a single trailing refetch.
  const wsRefetchTimerRef = useRef<NodeJS.Timeout | null>(null);
  // Monotonic id assigned to each live event on receipt (stable list key).
  const liveEventSeqRef = useRef(0);
  // Current main tab, mirrored into a ref so timers/WS handlers can gate on it.
  const mainTabRef = useRef("attribution");
  // Current app section, mirrored for timers/WS handlers.
  const sectionRef = useRef<Section>("dashboard");

  const effectiveCompareMode: CompareMode = !compareEnabled
    ? "none"
    : autoCompare
    ? "previous_period"
    : compareMode === "none"
    ? "previous_period"
    : compareMode;

  // Flips true once (and stays true) after the first primary report renders.
  // Background sync and the deferred comparison gate on this so neither races
  // the initial report load. `report` is only ever replaced with a fresh
  // (non-null) object afterward, so the boolean stays stable.
  const reportReady = report !== null;

  const loadReport = useCallback(async (opts?: { fresh?: boolean }) => {
    // Dashboard and the attribution report consume the full report; feature
    // panels such as Journey/Spend/Ad Names load their own data.
    const needsFullReport = section === "dashboard" || (section === "reports" && mainTab === "attribution");
    if (!needsFullReport) return;

    const requestSeq = reportRequestSeqRef.current + 1;
    reportRequestSeqRef.current = requestSeq;
    reportAbortRef.current?.abort();
    const abortController = new AbortController();
    reportAbortRef.current = abortController;
    reportInFlightRef.current = true;

    try {
      const [startDate, endDate] = normalizeDateRange(primaryStartDate, primaryEndDate);

      const primaryParams = {
        start_date: startDate,
        end_date: endDate,
        model,
        active_tab: activeTab,
        use_click_date: useClickDate,
        // Bypass the backend's short report cache when the user just pulled fresh
        // data (manual Sync / CSV import) so the new numbers show immediately.
        no_cache: opts?.fresh === true,
      };

      setLoading(true);
      setError(null);

      // Only the PRIMARY report is on the critical path. The comparison period
      // is fetched lazily by a separate effect once this has rendered, so the
      // dashboard never waits on (or is blanked by) a second report build.
      const data = await fetchReport(primaryParams, abortController.signal);

      if (requestSeq !== reportRequestSeqRef.current) return;
      setReport(data);
    } catch (err: any) {
      if (isAbortError(err)) return;
      if (requestSeq !== reportRequestSeqRef.current) return;
      const status = typeof err?.status === "number" ? err.status : 0;
      if (status === 401 || status === 403) {
        router.replace("/login");
        return;
      }
      setError(err.message || "Failed to load report");
    } finally {
      if (reportAbortRef.current === abortController) {
        reportAbortRef.current = null;
        reportInFlightRef.current = false;
      }
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
    router,
    section,
  ]);

  // Deferred comparison fetch. Runs off the critical path (after the primary
  // report has rendered) and NEVER blanks the primary: on failure it clears the
  // comparison and flags it unavailable instead of propagating the error.
  const loadCompare = useCallback(async () => {
    const needsFullReport = section === "dashboard" || (section === "reports" && mainTab === "attribution");

    // Resolve the comparison request for the current mode, if any.
    let compareParams: Parameters<typeof fetchReport>[0] | null = null;
    let nextCompareLabel = "";
    if (needsFullReport && effectiveCompareMode !== "none") {
      const [startDate, endDate] = normalizeDateRange(primaryStartDate, primaryEndDate);
      if (effectiveCompareMode === "previous_period") {
        const { start: previousStart, end: previousEnd } = previousPeriod(startDate, endDate);
        compareParams = { start_date: previousStart, end_date: previousEnd, model, active_tab: activeTab, use_click_date: useClickDate };
        nextCompareLabel = `${previousStart} to ${previousEnd}`;
      } else if (effectiveCompareMode === "custom_range") {
        const [customStart, customEnd] = normalizeDateRange(compareStartDate, compareEndDate);
        if (customStart && customEnd) {
          compareParams = { start_date: customStart, end_date: customEnd, model, active_tab: activeTab, use_click_date: useClickDate };
          nextCompareLabel = `${customStart} to ${customEnd}`;
        }
      } else if (effectiveCompareMode === "model" && compareModel !== model) {
        compareParams = { start_date: startDate, end_date: endDate, model: compareModel, active_tab: activeTab, use_click_date: useClickDate };
        nextCompareLabel = `${modelLabel(compareModel)} model`;
      }
    }

    const requestSeq = compareRequestSeqRef.current + 1;
    compareRequestSeqRef.current = requestSeq;
    compareAbortRef.current?.abort();

    // No comparison requested (disabled, unsupported mode, or model matches):
    // clear any stale comparison state and stop.
    if (!compareParams) {
      compareAbortRef.current = null;
      setCompareReport(null);
      setCompareLabel("");
      setCompareUnavailable(false);
      return;
    }

    const abortController = new AbortController();
    compareAbortRef.current = abortController;
    setCompareUnavailable(false);

    try {
      const compareData = await fetchReport(compareParams, abortController.signal);
      if (requestSeq !== compareRequestSeqRef.current) return;
      setCompareReport(compareData);
      setCompareLabel(nextCompareLabel);
    } catch (err: any) {
      if (isAbortError(err)) return;
      if (requestSeq !== compareRequestSeqRef.current) return;
      // A comparison failure must not discard the primary report.
      setCompareReport(null);
      setCompareLabel("");
      setCompareUnavailable(true);
    } finally {
      if (compareAbortRef.current === abortController) {
        compareAbortRef.current = null;
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
    section,
  ]);

  const syncSpendData = useCallback(async (
    range?: { start_date?: string; end_date?: string },
    opts?: { notify?: boolean },
  ) => {
    if (syncingSpendRef.current) return null;

    const notify = Boolean(opts?.notify);
    // Include today so the current day's spend/orders are covered; prefer the
    // caller-supplied (on-screen) window and only fall back to a 7-day trailer.
    const syncEnd = range?.end_date || daysAgo(0);
    const syncStart = range?.start_date || daysAgo(7);
    syncingSpendRef.current = true;
    setSyncingSpend(true);
    const toastId = notify
      ? toastRef.current.loading("Syncing all platforms…", { description: "Pulling ad spend, ad names and Stripe orders." })
      : 0;

    try {
      const [spendResult, namesResult, stripeResult, ghlResult] = await Promise.allSettled([
        withSyncDeadline("Spend sync", 60_000, (signal) => syncSpend({ platform: "all", start_date: syncStart, end_date: syncEnd }, signal)),
        withSyncDeadline("Ad name sync", 90_000, (signal) => syncAdNames("all", signal)),
        withSyncDeadline("Stripe sync", 180_000, (signal) => syncStripe({ start_date: syncStart, end_date: syncEnd }, signal)),
        withSyncDeadline("Lead sync", 90_000, (signal) => syncGhl({ start_date: syncStart, end_date: syncEnd }, signal)),
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
        // The spend sync reports per-platform failures inside `platforms`, not a
        // top-level `errors` array — surface them so a failing TikTok/Meta/Google
        // sync (e.g. an expired token) is visible instead of silently producing
        // no spend rows for that platform.
        const platforms = result.value?.platforms;
        if (platforms && typeof platforms === "object") {
          for (const [name, info] of Object.entries<any>(platforms)) {
            if (info && typeof info === "object" && info.error) {
              errors.push(`${scope} (${name}): ${info.error}`);
            }
          }
        }
      };
      addSyncErrors("Spend", spendResult);
      addSyncErrors("Names", namesResult);
      addSyncErrors("Stripe", stripeResult);
      addSyncErrors("Leads", ghlResult);
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
    let attempt = 0;
    let retryTimer: ReturnType<typeof setTimeout> | null = null;

    const checkAuth = async () => {
      try {
        const status = await fetchAuthMe();
        if (cancelled) return;
        if (!status?.authenticated) {
          router.replace("/login");
          return;
        }
        setBackendWaking(false);
        setAuthEnabled(Boolean(status?.auth_enabled));
        setAuthUser(String(status?.user?.username || ""));
        setAuthChecked(true);
      } catch (err: any) {
        if (cancelled) return;
        const status = typeof err?.status === "number" ? err.status : 0;
        // Only a genuine auth rejection should bounce the user to /login.
        if (status === 401 || status === 403) {
          router.replace("/login");
          return;
        }
        // Network error / timeout (e.g. Render cold start): keep the user here,
        // surface a "waking up" state, and retry with capped exponential backoff.
        setBackendWaking(true);
        attempt += 1;
        const delay = Math.min(2000 * 2 ** (attempt - 1), 15000);
        retryTimer = setTimeout(() => {
          void checkAuth();
        }, delay);
      }
    };
    void checkAuth();
    return () => {
      cancelled = true;
      if (retryTimer) clearTimeout(retryTimer);
    };
  }, [router]);

  useEffect(() => {
    loadReportRef.current = loadReport;
  }, [loadReport]);

  useEffect(() => {
    mainTabRef.current = mainTab;
  }, [mainTab]);

  useEffect(() => {
    sectionRef.current = section;
  }, [section]);

  // Dedicated single initial primary loader.
  useEffect(() => {
    if (!authChecked) return;
    loadReport();
  }, [loadReport, authChecked]);

  // Deferred comparison loader: only after the primary report has rendered, and
  // re-run whenever the primary refreshes or the compare settings change. This
  // keeps the comparison off the critical path (#6) while staying in sync.
  useEffect(() => {
    if (!authChecked || !reportReady) return;
    void loadCompare();
  }, [authChecked, reportReady, report, loadCompare]);

  // Background sync (spend/names/Stripe/GHL). Deferred until after the first
  // report render so it never runs concurrently with the initial load; only
  // the post-sync refetch remains (the pre-sync loadReport was removed). Reads
  // the selected window from a ref so the interval isn't torn down on range
  // changes.
  useEffect(() => {
    // Off by default — automatic heavy sync from the browser is what starves the
    // report request. Data still refreshes from the read-only report auto-refresh
    // below, and the user can pull fresh platform data on demand via "Sync".
    if (!AUTO_SYNC) return;
    if (!authChecked || !reportReady) return;

    const runBackgroundSync = () => {
      const { start, end } = syncRangeRef.current;
      void syncSpendData({ start_date: start, end_date: end }).finally(() => {
        void loadReportRef.current();
      });
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
  }, [authChecked, reportReady, syncSpendData]);

  // Auto-refresh every 30s — only on the attribution tab, paused while the tab is
  // hidden, and never while a report request is already in flight.
  useEffect(() => {
    if (!autoRefresh || !authChecked) return;
    if (section === "reports" && mainTab !== "attribution") return;

    const tick = () => {
      if (typeof document !== "undefined" && document.hidden) return;
      if (reportInFlightRef.current) return;
      void loadReport();
    };

    refreshTimerRef.current = setInterval(tick, 60_000);

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
  }, [autoRefresh, loadReport, authChecked, mainTab, section]);

  useEffect(() => {
    if (!authChecked) return;
    const managed = createWebSocket(
      (data: LiveEvent) => {
        const eventId = (liveEventSeqRef.current += 1);
        setLiveEvents((prev) => [...prev.slice(-49), { ...data, _id: eventId }]);
        // Coalesce bursts of new_order events into a single trailing refetch
        // ~1s after the last event (instead of one timer per event).
        if (data.type === "new_order") {
          if (wsRefetchTimerRef.current) clearTimeout(wsRefetchTimerRef.current);
          wsRefetchTimerRef.current = setTimeout(() => {
            wsRefetchTimerRef.current = null;
            if (sectionRef.current === "reports" && mainTabRef.current !== "attribution") return;
            void loadReportRef.current();
          }, 1000);
        }
      },
      // Status callback survives reconnects (unlike overriding socket.onopen,
      // which the internal backoff logic re-assigns on each new socket).
      (status) => setWsConnected(status === "open"),
    );
    if (managed) {
      wsRef.current = managed;
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

  // Keep the background-sync range ref pointed at the on-screen window so the
  // 10-minute sync always covers what the user is looking at (#8), without
  // re-subscribing the sync interval when the range changes.
  useEffect(() => {
    syncRangeRef.current = { start: windowStart, end: windowEnd };
  }, [windowStart, windowEnd]);

  // Stable value object for the DateRangePicker (primitive-keyed) — see #11/#12.
  const primaryRange = useMemo(() => ({ start: primaryStartDate, end: primaryEndDate }), [primaryStartDate, primaryEndDate]);

  const compareRange =
    effectiveCompareMode === "previous_period"
      ? previousPeriod(windowStart, windowEnd)
      : effectiveCompareMode === "custom_range"
      ? { start: compareStartDate, end: compareEndDate }
      : null;
  // Hyros captions headline cards with the current period start. The previous
  // implementation used the comparison period start, which made a Jul 05-11
  // report say "from Jun 28" even though the API request used the right range.
  const currentRangeCaption = monthDayLabel(windowStart);

  const setRange = useCallback((range: { start: string; end: string }) => {
    const [start, end] = normalizeDateRange(range.start, range.end);
    setError(null);
    setPrimaryStartDate(start);
    setPrimaryEndDate(end);
  }, []);

  const handleManualSync = useCallback(async () => {
    const syncPromise = syncSpendData({ start_date: windowStart, end_date: windowEnd }, { notify: true });
    await loadReport();
    await syncPromise;
    // Force-fresh after the sync wrote new data so the cache doesn't serve stale numbers.
    await loadReport({ fresh: true });
  }, [loadReport, syncSpendData, windowEnd, windowStart]);

  if (!authChecked) {
    return (
      <div className="flex min-h-screen flex-col items-center justify-center gap-3">
        <RefreshCw size={24} className="animate-spin text-brand-500" />
        {backendWaking && (
          <p className="text-sm text-ink-dim">Backend is waking up, retrying…</p>
        )}
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
        {section !== "reports" && (
        <>
        {/* Top controls strip */}
        <div className="sticky top-0 z-40 border-b border-[var(--card-border)] bg-[var(--background)]/85 backdrop-blur-xl">
          <div className="flex flex-wrap items-center gap-3 px-6 py-3">
            <h1 className="h-title text-[26px]">
              {SECTION_TITLES[section]}
            </h1>

            <div className="ml-auto flex flex-wrap items-center gap-2">
              {showDateControls && (
                <DateRangePicker
                  value={primaryRange}
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
                  onClick={handleManualSync}
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

          {section === "dashboard" && compareEnabled && compareUnavailable && (
            <div className="mb-5 rounded-xl border border-amber-500/30 bg-amber-500/5 px-4 py-2.5 text-sm text-amber-400">
              Comparison unavailable — showing the primary period only.
            </div>
          )}

          {/* ───────── Dashboard ───────── */}
          {section === "dashboard" && (
            report ? (
              <DashboardView report={report} compareReport={compareReport} currentRangeCaption={currentRangeCaption} />
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

        </div>
        </>
        )}

        {/* ───────── Reports (Hyros performance report) ───────── */}
        {section === "reports" && (
          <div className="flex min-h-full flex-col">
            <div className="flex items-center gap-1 overflow-x-auto border-b border-[var(--card-border)] bg-[#0a0a0e] px-3 py-2">
              <button onClick={() => setMainTab("attribution")} title="Performance report" className="mr-1 flex h-7 w-7 items-center justify-center rounded-md text-ink-dim hover:bg-white/5 hover:text-ink"><Home size={15} /></button>
              {REPORT_TABS.map((tab) => (
                <button key={tab.key} onClick={() => setMainTab(tab.key)} className={`flex items-center gap-1.5 whitespace-nowrap rounded-md px-3 py-1.5 text-[13px] transition-colors ${mainTab === tab.key ? "bg-white/[0.06] font-medium text-ink-bright" : "text-ink-dim hover:text-ink"}`}>{tab.icon} {tab.label}</button>
              ))}
              <button disabled title="New report — coming soon" className="ml-1 flex h-7 w-7 items-center justify-center rounded-md text-ink-faint opacity-60"><Plus size={15} /></button>
            </div>
            {error && (
              <div className="mx-6 mt-4 rounded-xl border border-rose-500/30 bg-rose-500/5 p-4 text-sm text-rose-400">{error} — Check API URL, CORS, and backend logs.</div>
            )}
            {mainTab === "attribution" && compareEnabled && compareUnavailable && (
              <div className="mx-6 mt-4 rounded-xl border border-amber-500/30 bg-amber-500/5 px-4 py-2.5 text-sm text-amber-400">
                Comparison unavailable — showing the primary period only.
              </div>
            )}
            {mainTab !== "attribution" && (
              <div className="px-6 py-6">
                {mainTab === "funnel" && <FunnelPanel startDate={windowStart} endDate={windowEnd} />}
                {mainTab === "ltv" && <LtvPanel />}
                {mainTab === "journey" && <JourneyPanel startDate={windowStart} endDate={windowEnd} />}
                {mainTab === "cohort" && <CohortPanel />}
                {mainTab === "capi" && <CapiPanel />}
                {mainTab === "spend" && <SpendImportPanel startDate={windowStart} endDate={windowEnd} onImported={() => loadReport({ fresh: true })} />}
                {mainTab === "names" && <AdNamesPanel />}
                {mainTab === "tracking" && <CampaignTrackingPanel onChange={() => loadReport({ fresh: true })} />}
              </div>
            )}
            {mainTab === "attribution" && (
              <ReportsView
                report={report}
                compareReport={compareReport}
                compareLabel={compareLabel}
                loading={loading}
                model={model}
                onModelChange={setModel}
                range={{ start: primaryStartDate, end: primaryEndDate }}
                onRangeChange={setRange}
                compareRange={compareRange}
                compareEnabled={compareEnabled}
                onCompareEnabledChange={setCompareEnabled}
                autoCompare={autoCompare}
                onAutoCompareChange={setAutoCompare}
                useClickDate={useClickDate}
                onUseClickDateChange={setUseClickDate}
                activeTab={activeTab}
                onTabChange={handleTabChange}
                platformFilter={platformFilter}
                onPlatformFilterChange={setPlatformFilter}
                startDate={windowStart}
                endDate={windowEnd}
                autoRefresh={autoRefresh}
                onToggleAutoRefresh={() => setAutoRefresh((v) => !v)}
                syncing={syncingSpend}
                onSync={handleManualSync}
                onReload={() => loadReport({ fresh: true })}
              />
            )}
          </div>
        )}
      </main>
    </div>
  );
}
