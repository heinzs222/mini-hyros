"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { fetchReport, createWebSocket, fetchAuthMe, logout as logoutApi } from "@/lib/api";
import { daysAgo } from "@/lib/utils";
import SummaryCards from "@/components/SummaryCards";
import PerformanceChart from "@/components/PerformanceChart";
import TrafficValueChart from "../components/TrafficValueChart";
import CumulativePerformanceChart from "../components/CumulativePerformanceChart";
import PlatformMixChart from "../components/PlatformMixChart";
import AttributionTable from "@/components/AttributionTable";
import TrackingHealth from "@/components/TrackingHealth";
import PlatformComparisonTable from "@/components/PlatformComparisonTable";
import FunnelSnapshotTable from "@/components/FunnelSnapshotTable";
import LiveFeed from "@/components/LiveFeed";
import LtvPanel from "@/components/LtvPanel";
import FunnelPanel from "@/components/FunnelPanel";
import JourneyPanel from "@/components/JourneyPanel";
import CohortPanel from "@/components/CohortPanel";
import CapiPanel from "@/components/CapiPanel";
import AdNamesPanel from "@/components/AdNamesPanel";
import {
  BarChart3,
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

interface LiveEvent {
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
  return date.toISOString().slice(0, 10);
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
  const [mainTab, setMainTab] = useState("attribution");
  const wsRef = useRef<WebSocket | null>(null);
  const refreshTimerRef = useRef<NodeJS.Timeout | null>(null);
  const loadReportRef = useRef<() => Promise<void>>(async () => {});

  const loadReport = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const [startDate, endDate] = normalizeDateRange(primaryStartDate, primaryEndDate);

      const primaryParams = {
        start_date: startDate,
        end_date: endDate,
        model,
        active_tab: activeTab,
        use_click_date: useClickDate,
      };

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

      setReport(data);
      setCompareReport(compareData);
      setCompareLabel(compareData ? nextCompareLabel : "");
    } catch (err: any) {
      if (String(err?.message || "").includes("401")) {
        router.replace("/login");
        return;
      }
      setError(err.message || "Failed to load report");
    } finally {
      setLoading(false);
    }
  }, [
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
    if (!authChecked) return;
    loadReport();
  }, [loadReport, authChecked]);

  // Auto-refresh every 30s
  useEffect(() => {
    if (autoRefresh && authChecked) {
      refreshTimerRef.current = setInterval(loadReport, 30_000);
    }
    return () => {
      if (refreshTimerRef.current) clearInterval(refreshTimerRef.current);
    };
  }, [autoRefresh, loadReport, authChecked]);

  // WebSocket for real-time events
  useEffect(() => {
    if (!authChecked) return;
    const ws = createWebSocket((data: LiveEvent) => {
      setLiveEvents((prev) => [...prev.slice(-49), data]);
      // Auto-refresh report on new orders
      if (data.type === "new_order") {
        setTimeout(() => {
          void loadReportRef.current();
        }, 1000);
      }
    });
    if (ws) {
      wsRef.current = ws;
      ws.onopen = () => setWsConnected(true);
      ws.onclose = () => setWsConnected(false);
    }
    return () => {
      if (wsRef.current) wsRef.current.close();
    };
  }, [authChecked]);

  const handleLogout = async () => {
    try {
      await logoutApi();
    } finally {
      router.replace("/login");
    }
  };

  const handleTabChange = (tab: string) => {
    setActiveTab(tab);
  };

  const [windowStart, windowEnd] = normalizeDateRange(primaryStartDate, primaryEndDate);
  const activeWindowLabel = `${windowStart} to ${windowEnd}`;

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
            <div className="min-w-[290px]">
              <div className="text-[10px] text-gray-500 uppercase tracking-wide mb-1">Primary Range</div>
              <div className="grid grid-cols-2 gap-2">
                <input
                  type="date"
                  value={primaryStartDate}
                  onChange={(e) => setPrimaryStartDate(e.target.value)}
                  className="w-full bg-[var(--card)] border border-[var(--card-border)] rounded-lg px-2.5 py-1.5 text-xs text-gray-300 focus:outline-none focus:border-brand-500"
                  aria-label="Primary start date"
                />
                <input
                  type="date"
                  value={primaryEndDate}
                  onChange={(e) => setPrimaryEndDate(e.target.value)}
                  className="w-full bg-[var(--card)] border border-[var(--card-border)] rounded-lg px-2.5 py-1.5 text-xs text-gray-300 focus:outline-none focus:border-brand-500"
                  aria-label="Primary end date"
                />
              </div>
            </div>

            <div className="min-w-[170px]">
              <div className="text-[10px] text-gray-500 uppercase tracking-wide mb-1">Primary Attribution</div>
              <select
                value={model}
                onChange={(e) => setModel(e.target.value)}
                className="w-full bg-[var(--card)] border border-[var(--card-border)] rounded-lg px-2.5 py-1.5 text-xs text-gray-300 focus:outline-none focus:border-brand-500"
              >
                {MODELS.map((m) => (
                  <option key={m.value} value={m.value}>{m.label}</option>
                ))}
              </select>
            </div>

            <div className="min-w-[150px]">
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

            <div className="min-w-[140px]">
              <div className="text-[10px] text-gray-500 uppercase tracking-wide mb-1">Date Basis</div>
              <select
                value={useClickDate ? "click" : "conversion"}
                onChange={(e) => setUseClickDate(e.target.value === "click")}
                className="w-full bg-[var(--card)] border border-[var(--card-border)] rounded-lg px-2.5 py-1.5 text-xs text-gray-300 focus:outline-none focus:border-brand-500"
              >
                <option value="conversion">Conversion Date</option>
                <option value="click">Click Date</option>
              </select>
            </div>

            <button
              onClick={loadReport}
              disabled={loading}
              className="flex items-center gap-1 px-3 py-1.5 rounded-lg bg-brand-600 hover:bg-brand-700 text-white text-xs font-medium transition-colors disabled:opacity-50 h-[34px]"
            >
              <RefreshCw size={12} className={loading ? "animate-spin" : ""} />
              Refresh
            </button>

            {authEnabled && (
              <button
                onClick={handleLogout}
                className="flex items-center gap-1 px-3 py-1.5 rounded-lg bg-white/5 hover:bg-white/10 text-gray-200 text-xs font-medium transition-colors h-[34px]"
                title={authUser ? `Logout ${authUser}` : "Logout"}
              >
                <LogOut size={12} />
                Logout
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
            {error} — Make sure the backend is running on port 8000.
          </div>
        )}

        {/* Feature Tabs */}
        {mainTab === "funnel" && <FunnelPanel />}
        {mainTab === "ltv" && <LtvPanel />}
        {mainTab === "journey" && <JourneyPanel />}
        {mainTab === "cohort" && <CohortPanel />}
        {mainTab === "capi" && <CapiPanel />}
        {mainTab === "names" && <AdNamesPanel />}

        {mainTab === "attribution" && report && (
          <>
            <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)]/70 p-3">
              <div className="flex flex-wrap items-center gap-2 text-[11px]">
                <span className="px-2 py-1 rounded bg-white/5 text-gray-300">Model: {modelLabel(model)}</span>
                <span className="px-2 py-1 rounded bg-white/5 text-gray-300">Window: {activeWindowLabel}</span>
                <span className="px-2 py-1 rounded bg-white/5 text-gray-300">Basis: {useClickDate ? "Click Date" : "Conversion Date"}</span>
                <span className={`px-2 py-1 rounded ${compareReport ? "bg-blue-500/15 text-blue-300" : "bg-white/5 text-gray-500"}`}>
                  {compareReport ? `Comparing vs ${compareLabel}` : "Comparison Off"}
                </span>
              </div>
            </div>

            {/* Summary KPI Cards */}
            <SummaryCards
              totals={report.summary_totals}
              compareTotals={compareReport?.summary_totals}
              compareLabel={compareLabel}
            />

            {/* Source + Funnel clarity */}
            <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
              <div className="xl:col-span-2">
                <PlatformComparisonTable
                  rows={report.platform_comparison?.rows || []}
                  compareRows={compareReport?.platform_comparison?.rows || []}
                  compareLabel={compareLabel}
                />
              </div>
              <div>
                <FunnelSnapshotTable
                  rows={report.funnels?.rows || []}
                  compareRows={compareReport?.funnels?.rows || []}
                  compareLabel={compareLabel}
                />
              </div>
            </div>

            {/* Charts + Sidebar */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              <div className="lg:col-span-2 space-y-6">
                <PerformanceChart
                  data={report.charts?.time_series || []}
                  compareData={compareReport?.charts?.time_series || []}
                  compareLabel={compareLabel}
                />
                <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
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
                <TrackingHealth
                  tracking={report.tracking}
                  freshness={report.diagnostics?.data_freshness}
                  wsConnected={wsConnected}
                />
                <LiveFeed events={liveEvents} connected={wsConnected} />
              </div>
            </div>

            {/* Attribution Table */}
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
