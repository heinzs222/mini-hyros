"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import DateRangePicker from "./DateRangePicker";
import ModelSelect, { modelLabel } from "./ModelSelect";
import AttributionTable from "./AttributionTable";
import PerformanceChart from "./PerformanceChart";
import PlatformMixChart from "./PlatformMixChart";
import { reportTodayIso } from "@/lib/utils";
import {
  Megaphone,
  Copy,
  MoreHorizontal,
  Save,
  SlidersHorizontal,
  Search,
  Columns3,
  LayoutGrid,
  ListTree,
  LineChart,
  ChevronDown,
  X,
  Globe,
  Building2,
  Layers3,
  Image as ImageIcon,
  RefreshCw,
  PanelLeftClose,
  BookOpen,
  Info,
  Check,
  Filter as FilterIcon,
  AlertTriangle,
} from "lucide-react";

interface Range {
  start: string;
  end: string;
}

interface Props {
  report: any;
  compareReport?: any;
  compareLabel?: string;
  loading?: boolean;
  model: string;
  onModelChange: (m: string) => void;
  range: Range;
  onRangeChange: (r: Range) => void;
  compareRange?: Range | null;
  compareEnabled: boolean;
  onCompareEnabledChange: (v: boolean) => void;
  autoCompare: boolean;
  onAutoCompareChange: (v: boolean) => void;
  useClickDate: boolean;
  onUseClickDateChange: (v: boolean) => void;
  activeTab: string;
  onTabChange: (t: string) => void;
  platformFilter: string;
  onPlatformFilterChange: (p: string) => void;
  startDate: string;
  endDate: string;
  autoRefresh: boolean;
  onToggleAutoRefresh: () => void;
  syncing: boolean;
  onSync: () => void;
  onReload: () => void;
}

/* ── helpers ── */
function pad(n: number) {
  return String(n).padStart(2, "0");
}
function fmtDot(iso: string): string {
  if (!iso) return "—";
  const d = new Date(`${iso}T00:00:00`);
  return `${pad(d.getMonth() + 1)}.${pad(d.getDate())}.${d.getFullYear()}`;
}
function fmtPill(iso: string): string {
  if (!iso) return "—";
  const d = new Date(`${iso}T00:00:00`);
  return `${pad(d.getMonth() + 1)}.${pad(d.getDate())}`;
}
function addDaysIso(iso: string, n: number): string {
  const d = new Date(`${iso}T00:00:00`);
  d.setDate(d.getDate() + n);
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}
function hashId(s: string): string {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
  return h.toString(16).padStart(6, "0").slice(0, 6);
}

const GROUP_TABS = [
  { key: "traffic_source", label: "Traffic source", icon: <Globe size={14} /> },
  { key: "ad_account", label: "Ad Account", icon: <Building2 size={14} /> },
  { key: "campaign", label: "Campaign", icon: <Megaphone size={14} /> },
  { key: "ad_set", label: "Ad Set", icon: <Layers3 size={14} /> },
  { key: "ad", label: "Ad", icon: <ImageIcon size={14} /> },
];

const SOURCE_OPTIONS = [
  { value: "all", label: "All sources" },
  { value: "meta", label: "Meta" },
  { value: "google", label: "Google" },
  { value: "tiktok", label: "TikTok" },
];

/* Column categorisation for the "Choose report columns" modal. */
const COLUMN_CATEGORIES: { key: string; label: string; match: (key: string) => boolean }[] = [
  { key: "all", label: "All", match: () => true },
  { key: "general", label: "General", match: (k) => ["clicks", "cost", "cpc", "cpm", "ctr", "impressions"].includes(k) },
  { key: "ecom", label: "E-Commerce", match: (k) => ["revenue", "total_revenue", "orders", "aov", "rpc", "roas", "profit", "net_profit", "margin_pct", "cpa", "cvr"].includes(k) },
  { key: "sync", label: "Reported", match: (k) => ["reported", "reported_delta"].includes(k) },
];

export default function ReportsView(props: Props) {
  const {
    report, compareReport, compareLabel, loading, model, onModelChange, range, onRangeChange,
    compareRange, compareEnabled, onCompareEnabledChange, autoCompare, onAutoCompareChange,
    useClickDate, onUseClickDateChange, activeTab, onTabChange, platformFilter,
    onPlatformFilterChange, startDate, endDate, autoRefresh, onToggleAutoRefresh, syncing, onSync, onReload,
  } = props;

  // Ground-truth tab of the currently-held report payload. When the user switches
  // tabs/ranges/model, `report` still holds the PREVIOUS tab's rows until the slow
  // refetch lands — rendering those under the new tab's label is the "campaign
  // rows / PMax under Traffic source" bug. Gate on this so a mismatched report
  // shows a neutral skeleton instead of misinterpreted rows.
  const dataTab: string | undefined = report?.table?.active_tab;
  const tabMismatch = Boolean(report && dataTab && dataTab !== activeTab);
  const showSkeleton = !report || tabMismatch;
  const isRefreshing = Boolean(loading);

  const [filtersOpen, setFiltersOpen] = useState(true);
  const [openByDefault, setOpenByDefault] = useState(true);
  const [viewMode, setViewMode] = useState<"tabs" | "nested" | "chart">("tabs");
  const [search, setSearch] = useState("");
  const [density, setDensity] = useState<"compact" | "comfortable">("compact");
  const [showDensity, setShowDensity] = useState(false);
  const [hiddenCols, setHiddenCols] = useState<Set<string>>(new Set());
  const [showColumns, setShowColumns] = useState(false);
  const [optimize, setOptimize] = useState(true);
  const densityRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const pref = window.localStorage.getItem("vigil_filters_open");
    if (pref != null) {
      setOpenByDefault(pref === "1");
      setFiltersOpen(pref === "1");
    }
  }, []);

  useEffect(() => {
    if (!showDensity) return;
    const onDown = (e: MouseEvent) => {
      if (densityRef.current && !densityRef.current.contains(e.target as Node)) setShowDensity(false);
    };
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [showDensity]);

  const columns = report?.table?.columns || [];
  const metricCols = useMemo(() => columns.filter((c: any) => c.type !== "dimension"), [columns]);
  const sourceCount = report?.platform_comparison?.rows?.length ?? 0;
  const reportId = useMemo(
    () => hashId(`${model}|${range.start}|${range.end}|${activeTab}|${useClickDate}`),
    [model, range, activeTab, useClickDate],
  );

  const today = reportTodayIso();
  const quickPresets = [
    { label: "Today", range: { start: today, end: today } },
    { label: "Yesterday", range: { start: addDaysIso(today, -1), end: addDaysIso(today, -1) } },
    { label: "7 days", range: { start: addDaysIso(today, -7), end: addDaysIso(today, -1) } },
    { label: "30 days", range: { start: addDaysIso(today, -30), end: addDaysIso(today, -1) } },
  ];
  const activePreset = quickPresets.find((p) => p.range.start === range.start && p.range.end === range.end)?.label;

  const setFiltersOpenPref = (open: boolean) => {
    setOpenByDefault(open);
    if (typeof window !== "undefined") window.localStorage.setItem("vigil_filters_open", open ? "1" : "0");
  };
  const clearFilters = () => {
    setSearch("");
    onPlatformFilterChange("all");
    setHiddenCols(new Set());
  };

  return (
    <div className="flex w-full flex-col">
      {/* ── Report header ── */}
      <div className="border-b border-[var(--card-border)] px-6 py-4">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div className="flex items-start gap-3">
            <span className="mt-1 flex h-9 w-9 items-center justify-center rounded-lg bg-amber-500/15 text-amber-400">
              <Megaphone size={18} />
            </span>
            <div>
              <h1 className="h-title text-[26px] leading-tight">
                Performance Report · {fmtDot(range.start)}—{fmtDot(range.end)}
              </h1>
              <div className="mt-1 flex items-center gap-2 text-[12px] text-ink-dim">
                <span className="text-amber-400">{modelLabel(model)}</span>
                <span className="text-ink-faint">/</span>
                <span>Id: {reportId}</span>
                <button
                  title="Copy report id"
                  onClick={() => navigator.clipboard?.writeText(reportId)}
                  className="text-ink-faint hover:text-ink"
                >
                  <Copy size={12} />
                </button>
              </div>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            {/* Quick presets */}
            <div className="flex items-center gap-1 text-[13px]">
              {quickPresets.map((p) => (
                <button
                  key={p.label}
                  onClick={() => onRangeChange(p.range)}
                  className={`rounded-md px-2.5 py-1.5 transition-colors ${
                    activePreset === p.label ? "bg-white/[0.06] text-ink-bright" : "text-ink-dim hover:text-ink"
                  }`}
                >
                  {p.label}
                </button>
              ))}
            </div>
            <DateRangePicker
              value={range}
              onChange={onRangeChange}
              compareRange={compareRange}
              compareEnabled={compareEnabled}
              onCompareEnabledChange={onCompareEnabledChange}
              autoCompare={autoCompare}
              onAutoCompareChange={onAutoCompareChange}
              showCompareControls={false}
            />
            <ModelSelect value={model} onChange={onModelChange} />
            <button
              onClick={onSync}
              disabled={syncing}
              title="Sync all platforms"
              className="flex h-[34px] items-center gap-1.5 rounded-lg bg-brand-600 px-3 text-[13px] font-semibold text-white transition-colors hover:bg-brand-700 disabled:opacity-50"
            >
              <RefreshCw size={13} className={syncing ? "animate-spin" : ""} /> Sync
            </button>
            <button title="More" className="flex h-[34px] w-9 items-center justify-center rounded-lg border border-[var(--card-border)] bg-[var(--surface)] text-ink-dim hover:text-ink">
              <MoreHorizontal size={16} />
            </button>
            <button title="Save report" className="flex h-[34px] w-9 items-center justify-center rounded-lg border border-[var(--card-border)] bg-[var(--surface)] text-ink-dim hover:text-ink">
              <Save size={15} />
            </button>
            <button
              onClick={() => setFiltersOpen((o) => !o)}
              className={`flex h-[34px] items-center gap-1.5 rounded-lg border px-3 text-[13px] font-medium transition-colors ${
                filtersOpen ? "border-brand-500/50 bg-brand-500/10 text-brand-300" : "border-[var(--card-border)] bg-[var(--surface)] text-ink-dim hover:text-ink"
              }`}
            >
              <FilterIcon size={14} /> Filters
            </button>
          </div>
        </div>
      </div>

      {/* ── Body: filter rail + main ── */}
      <div className="flex">
        {filtersOpen && (
          <aside className="w-[300px] shrink-0 border-r border-[var(--card-border)] p-4">
            <div className="mb-4 flex items-center justify-between">
              <button onClick={() => setFiltersOpen(false)} title="Collapse filters" className="text-ink-dim hover:text-ink">
                <PanelLeftClose size={16} />
              </button>
              <label className="flex items-center gap-2 text-[12px] text-ink-dim">
                Open filter by default
                <Toggle on={openByDefault} onClick={() => setFiltersOpenPref(!openByDefault)} />
              </label>
            </div>

            <div className="mb-3 flex items-center justify-between">
              <h3 className="text-[15px] font-semibold text-ink-bright">Filters</h3>
              <button onClick={clearFilters} className="text-[12px] text-ink-dim hover:text-ink">Clear</button>
            </div>

            {/* Attribute filters */}
            <div className="mb-4 rounded-xl border border-[var(--card-border)] p-3">
              <div className="flex items-center gap-1.5 text-[13px] font-medium text-ink">
                Attribute filters <ChevronDown size={13} className="text-ink-faint" />
              </div>
              <p className="mt-1 text-[11px] leading-snug text-ink-dim">Filter results by certain sources, products, tags, etc</p>
              <button className="mt-2 w-full rounded-lg border border-dashed border-[var(--card-border)] py-2 text-[12px] text-ink-dim hover:text-ink">
                + Specify Attributes
              </button>
            </div>

            <Row label={<span className="flex items-center gap-1">Optimize report <Info size={12} className="text-ink-faint" /></span>}>
              <Toggle on={optimize} onClick={() => setOptimize((v) => !v)} />
            </Row>
            <Row label={<span className="flex items-center gap-1">Use date of click attribution <BookOpen size={12} className="text-ink-faint" /></span>}>
              <Toggle on={useClickDate} onClick={() => onUseClickDateChange(!useClickDate)} />
            </Row>

            <div className="mt-4">
              <div className="mb-1.5 text-[12px] text-ink-dim">Base grouping</div>
              <div className="flex items-center gap-1 rounded-lg border border-[var(--card-border)] bg-[var(--surface-2)] p-1 text-[12px]">
                {["Source", "Day", "Week", "Month"].map((g) => {
                  const isSource = g === "Source";
                  return (
                    <button
                      key={g}
                      disabled={!isSource}
                      title={isSource ? undefined : "Time grouping coming soon"}
                      className={`flex-1 rounded-md py-1.5 transition-colors ${
                        isSource ? "bg-white/[0.07] text-ink-bright" : "text-ink-faint cursor-not-allowed"
                      }`}
                    >
                      {g}
                    </button>
                  );
                })}
              </div>
            </div>

            <div className="mt-4">
              <div className="mb-1.5 text-[12px] text-ink-dim">Source configuration</div>
              <select
                value={platformFilter}
                onChange={(e) => onPlatformFilterChange(e.target.value)}
                className="w-full rounded-lg border border-[var(--card-border)] bg-[var(--surface-2)] px-2.5 py-2 text-[13px] text-ink focus:outline-none"
              >
                {SOURCE_OPTIONS.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
              </select>
            </div>

            <div className="mt-3">
              <div className="mb-1.5 text-[12px] text-ink-dim">New customer configuration</div>
              <select disabled className="w-full cursor-not-allowed rounded-lg border border-[var(--card-border)] bg-[var(--surface-2)] px-2.5 py-2 text-[13px] text-ink-dim focus:outline-none">
                <option>All Customers</option>
              </select>
            </div>

            <div className="mt-4 flex items-center gap-2">
              <button onClick={onReload} className="flex flex-1 items-center justify-center gap-1.5 rounded-lg border border-[var(--card-border)] bg-[var(--surface)] py-2 text-[13px] text-ink hover:bg-white/5">
                Generate new report
              </button>
              <button onClick={onReload} className="flex items-center gap-1 rounded-lg bg-white px-4 py-2 text-[13px] font-semibold text-black hover:bg-white/90">
                Apply
              </button>
            </div>
            <div className="mt-3 text-center text-[12px] text-ink-dim">{sourceCount} sources</div>
          </aside>
        )}

        {/* Main */}
        <div className="min-w-0 flex-1 p-6">
          {/* View toggle + controls */}
          <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
            <div className="flex items-center gap-1 text-[13px]">
              <ViewTab active={viewMode === "tabs"} onClick={() => setViewMode("tabs")} icon={<LayoutGrid size={14} />} label="Tabs" />
              <ViewTab active={viewMode === "nested"} onClick={() => setViewMode("nested")} icon={<ListTree size={14} />} label="Nested" />
              <ViewTab active={viewMode === "chart"} onClick={() => setViewMode("chart")} icon={<LineChart size={14} />} label="Chart" />
            </div>
            <div className="flex items-center gap-2">
              <div className="flex h-8 items-center gap-2 rounded-lg border border-[var(--card-border)] bg-[var(--surface-2)] px-2.5">
                <Search size={13} className="text-ink-faint" />
                <input
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  placeholder="Search…"
                  className="w-32 bg-transparent text-[12px] text-ink placeholder:text-ink-faint focus:outline-none"
                />
              </div>
              <div className="relative" ref={densityRef}>
                <button
                  onClick={() => setShowDensity((s) => !s)}
                  className="flex h-8 items-center gap-1.5 rounded-lg border border-[var(--card-border)] bg-[var(--surface-2)] px-2.5 text-[12px] text-ink-dim hover:text-ink"
                >
                  Density: <span className="capitalize text-ink">{density}</span> <ChevronDown size={12} />
                </button>
                {showDensity && (
                  <div className="animate-hpop absolute right-0 z-30 mt-2 w-[160px] rounded-lg border border-[var(--card-border)] bg-[#0c0c11] p-1 shadow-2xl">
                    {(["compact", "comfortable"] as const).map((d) => (
                      <button
                        key={d}
                        onClick={() => { setDensity(d); setShowDensity(false); }}
                        className="flex w-full items-center justify-between rounded-md px-2 py-1.5 text-left text-[12px] capitalize text-ink hover:bg-white/5"
                      >
                        {d}
                        {density === d && <Check size={13} className="text-brand-400" />}
                      </button>
                    ))}
                  </div>
                )}
              </div>
              <button className="flex h-8 cursor-not-allowed items-center gap-1.5 rounded-lg border border-[var(--card-border)] bg-[var(--surface-2)] px-2.5 text-[12px] text-ink-dim opacity-70" title="Column presets coming soon">
                Preset: <span className="text-ink">Custom</span> <ChevronDown size={12} />
              </button>
              <button
                onClick={() => setShowColumns(true)}
                className="flex h-8 items-center gap-1.5 rounded-lg border border-[var(--card-border)] bg-[var(--surface-2)] px-2.5 text-[12px] text-ink-dim hover:text-ink hover:border-white/20"
              >
                <Columns3 size={13} /> Columns
              </button>
            </div>
          </div>

          {/* Grouping breadcrumb tabs */}
          {viewMode !== "chart" && (
            <div className="mb-3 flex items-center gap-1 overflow-x-auto rounded-xl border border-[var(--card-border)] bg-[var(--surface)] p-1">
              {GROUP_TABS.map((t) => {
                const active = activeTab === t.key;
                return (
                  <button
                    key={t.key}
                    onClick={() => { onTabChange(t.key); onPlatformFilterChange("all"); }}
                    className={`flex flex-1 items-center justify-center gap-1.5 whitespace-nowrap rounded-lg px-3 py-2 text-[13px] font-medium transition-colors ${
                      active ? "bg-white/[0.06] text-ink-bright" : "text-ink-dim hover:text-ink"
                    }`}
                  >
                    <span className={active ? "text-brand-400" : "text-ink-faint"}>{t.icon}</span>
                    {t.label}
                  </button>
                );
              })}
            </div>
          )}

          {/* Content */}
          {showSkeleton ? (
            <TableSkeleton />
          ) : (
            <div className={`relative transition-opacity duration-150 ${isRefreshing ? "pointer-events-none opacity-50" : ""}`}>
              {isRefreshing && (
                <div className="pointer-events-none absolute inset-x-0 top-1 z-30 flex justify-center">
                  <div className="flex items-center gap-2 rounded-full border border-[var(--card-border)] bg-[#0c0c11] px-3 py-1.5 text-[12px] text-ink-dim shadow-lg">
                    <RefreshCw size={13} className="animate-spin text-brand-400" /> Updating report…
                  </div>
                </div>
              )}
              {viewMode === "chart" ? (
            <div className="space-y-6">
              <PerformanceChart
                data={report.charts?.time_series || []}
                compareData={compareReport?.charts?.time_series || []}
                compareLabel={compareLabel}
              />
              <PlatformMixChart
                rows={report.platform_comparison?.rows || []}
                compareRows={compareReport?.platform_comparison?.rows || []}
                compareLabel={compareLabel}
              />
            </div>
          ) : (
            <>
              {(() => {
                // Explicit empty state — the coverage banner below suppresses
                // itself when total<=0, which is exactly when the user most needs
                // to know WHY the grid is blank (no data vs a data error vs a
                // future window that got clamped).
                const s = report.summary_totals || {};
                const total = Number(s.all_orders_count ?? s.tracked_orders ?? 0);
                if (total > 0) return null;
                const dataErrors = report?.diagnostics?.data_errors || [];
                const clamped = Boolean(report?.report_meta?.future_window_clamped);
                return (
                  <div className="mb-3 flex items-start gap-2 rounded-xl border border-[var(--card-border)] bg-white/[0.03] px-4 py-3 text-[13px] leading-relaxed text-ink-dim">
                    <Info size={16} className="mt-0.5 flex-shrink-0 text-ink-faint" />
                    <div>
                      <span className="font-semibold text-ink">No orders found for this range.</span>{" "}
                      {clamped && "This window was in the future and was clamped to today. "}
                      {dataErrors.length > 0
                        ? "A data error occurred while building this report — some numbers may be missing. Check the backend logs."
                        : "If you expect data here, press Sync to pull the latest spend and orders, widen the date range, or confirm the tracking script is firing on your funnel and checkout pages."}
                    </div>
                  </div>
                );
              })()}
              {(() => {
                const s = report.summary_totals || {};
                const total = Number(s.all_orders_count ?? s.tracked_orders ?? 0);
                const attributed = Number(s.attributed_orders ?? 0);
                const unattrOrders = Math.max(Number(s.unattributed_orders ?? (total - attributed)), 0);
                const unattrRev = Number(s.unattributed_revenue ?? 0);
                const tableCoverage = report?.table?.coverage || {};
                const dimensionAttributed = Number(
                  tableCoverage.dimension_attributed_orders ?? report?.table?.totals_row?.orders ?? 0,
                );
                const dimensionSourceOrders = Number(
                  tableCoverage.source_attributed_orders ?? attributed,
                );
                const dimensionUnmapped = Number(
                  tableCoverage.unmapped_orders ??
                    Math.max(dimensionSourceOrders - dimensionAttributed, 0),
                );
                const dimensionLabel = String(activeTab || "dimension").replaceAll("_", " ");
                if (total <= 0 || unattrOrders <= 0) return null;
                const pct = total > 0 ? Math.round((attributed / total) * 100) : 0;
                const money = (v: number) =>
                  `$${v.toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
                return (
                  <div className="mb-3 flex items-start gap-2 rounded-xl border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-[13px] leading-relaxed text-amber-200">
                    <AlertTriangle size={16} className="mt-0.5 flex-shrink-0" />
                    <div>
                      <span className="font-semibold">
                        Source coverage — {pct}% of orders matched to a source touchpoint.
                      </span>{" "}
                      {unattrOrders} of {total} order{total === 1 ? "" : "s"}
                      {unattrRev > 0 ? ` (${money(unattrRev)})` : ""} in this range have no
                      qualifying source touchpoint inside the attribution window. This is not
                      automatically a sync failure: direct, recurring, offline, and identity-unmatched
                      sales can remain unattributed.
                      {activeTab !== "traffic_source" && dimensionSourceOrders > 0 && (
                        <>
                          {" "}<span className="font-semibold">
                            {dimensionAttributed.toLocaleString(undefined, { maximumFractionDigits: 2 })} of{" "}
                            {dimensionSourceOrders.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                          </span>{" "}
                          source-attributed orders map to a platform {dimensionLabel};{" "}
                          <span className="font-semibold">
                            {dimensionUnmapped.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                          </span>{" "}
                          remain source-known but platform-unmapped at this level.
                        </>
                      )}
                    </div>
                  </div>
                );
              })()}
            <AttributionTable
              columns={report.table.columns}
              rows={report.table.rows}
              totals={report.table.totals_row}
              compareRows={compareReport?.table?.rows || []}
              compareLabel={compareLabel}
              activeTab={activeTab}
              dataTab={dataTab}
              onTabChange={onTabChange}
              startDate={startDate}
              endDate={endDate}
              model={model}
              lookbackDays={30}
              useClickDate={useClickDate}
              platformFilter={platformFilter}
              onPlatformFilterChange={onPlatformFilterChange}
              embedded
              densityValue={density}
              searchValue={search}
              hiddenColumnKeys={Array.from(hiddenCols)}
            />
            </>
              )}
            </div>
          )}
        </div>
      </div>

      {/* ── Columns modal ── */}
      {showColumns && (
        <ColumnsModal
          metricCols={metricCols}
          hiddenCols={hiddenCols}
          onApply={(next) => { setHiddenCols(next); setShowColumns(false); }}
          onClose={() => setShowColumns(false)}
        />
      )}
    </div>
  );
}

/* ── small components ── */

function Toggle({ on, onClick }: { on: boolean; onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={on}
      className={`relative inline-flex h-[20px] w-[36px] items-center rounded-full transition-colors ${on ? "bg-emerald-500" : "bg-white/15"}`}
    >
      <span className={`inline-block h-[14px] w-[14px] transform rounded-full bg-white transition-transform ${on ? "translate-x-[19px]" : "translate-x-[3px]"}`} />
    </button>
  );
}

function Row({ label, children }: { label: React.ReactNode; children: React.ReactNode }) {
  return (
    <div className="mt-3 flex items-center justify-between rounded-lg border border-[var(--card-border)] px-3 py-2.5 text-[13px] text-ink">
      <span>{label}</span>
      {children}
    </div>
  );
}

function TableSkeleton() {
  return (
    <div className="hpanel overflow-hidden" aria-busy="true" aria-label="Loading report">
      <div className="flex items-center gap-3 border-b border-[var(--card-border)] px-4 py-3">
        <div className="skeleton h-3 w-28" />
        <div className="ml-auto flex gap-6">
          {Array.from({ length: 6 }).map((_, i) => (
            <div key={i} className="skeleton h-3 w-14" />
          ))}
        </div>
      </div>
      {Array.from({ length: 9 }).map((_, r) => (
        <div key={r} className="flex items-center gap-3 border-b border-[var(--card-border)]/60 px-4 py-3">
          <div className="skeleton h-4 w-44" style={{ opacity: 1 - r * 0.06 }} />
          <div className="ml-auto flex gap-6">
            {Array.from({ length: 6 }).map((_, i) => (
              <div key={i} className="skeleton h-3 w-14" style={{ opacity: 1 - r * 0.06 }} />
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

function ViewTab({ active, onClick, icon, label }: { active: boolean; onClick: () => void; icon: React.ReactNode; label: string }) {
  return (
    <button
      onClick={onClick}
      className={`flex items-center gap-1.5 rounded-lg px-3 py-1.5 font-medium transition-colors ${active ? "bg-white/[0.06] text-ink-bright" : "text-ink-dim hover:text-ink"}`}
    >
      <span className={active ? "text-brand-400" : "text-ink-faint"}>{icon}</span>
      {label}
    </button>
  );
}

function ColumnsModal({
  metricCols,
  hiddenCols,
  onApply,
  onClose,
}: {
  metricCols: any[];
  hiddenCols: Set<string>;
  onApply: (next: Set<string>) => void;
  onClose: () => void;
}) {
  const [cat, setCat] = useState("all");
  const [q, setQ] = useState("");
  // Local draft of hidden columns — toggles only mutate this copy so Cancel /
  // backdrop discards changes and only Apply commits them to the parent.
  const [draft, setDraft] = useState<Set<string>>(() => new Set(hiddenCols));
  const toggleDraft = (key: string) => {
    setDraft((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };
  const catDef = COLUMN_CATEGORIES.find((c) => c.key === cat) || COLUMN_CATEGORIES[0];
  const filtered = metricCols.filter((c) => catDef.match(c.key) && c.label.toLowerCase().includes(q.toLowerCase()));
  const selected = metricCols.filter((c) => !draft.has(c.key));

  return (
    <div className="fixed inset-0 z-[80] flex items-center justify-center bg-black/70 p-4" onClick={onClose}>
      <div className="animate-hpop flex h-[560px] w-[920px] max-w-[95vw] flex-col rounded-2xl border border-[var(--card-border)] bg-[#0c0c11] shadow-2xl" onClick={(e) => e.stopPropagation()}>
        <div className="flex items-center justify-between border-b border-[var(--card-border)] px-5 py-4">
          <h3 className="flex items-center gap-2 text-[16px] font-semibold text-ink-bright">
            <BookOpen size={16} className="text-emerald-400" /> Choose report columns
          </h3>
          <button onClick={onClose} className="text-ink-dim hover:text-ink"><X size={18} /></button>
        </div>
        <div className="flex min-h-0 flex-1">
          {/* Categories */}
          <div className="w-[200px] shrink-0 space-y-0.5 border-r border-[var(--card-border)] p-3">
            {COLUMN_CATEGORIES.map((c) => (
              <button
                key={c.key}
                onClick={() => setCat(c.key)}
                className={`flex w-full items-center justify-between rounded-lg px-3 py-2 text-left text-[13px] transition-colors ${
                  cat === c.key ? "bg-white/[0.06] text-ink-bright" : "text-ink-dim hover:bg-white/5 hover:text-ink"
                }`}
              >
                {c.label} <ChevronDown size={13} className="-rotate-90 text-ink-faint" />
              </button>
            ))}
          </div>
          {/* Available */}
          <div className="min-w-0 flex-1 border-r border-[var(--card-border)] p-3">
            <div className="mb-2 flex h-8 items-center gap-2 rounded-lg border border-[var(--card-border)] bg-[var(--surface-2)] px-2.5">
              <Search size={13} className="text-ink-faint" />
              <input value={q} onChange={(e) => setQ(e.target.value)} placeholder="Search columns…" className="w-full bg-transparent text-[12px] text-ink placeholder:text-ink-faint focus:outline-none" />
            </div>
            <div className="max-h-[420px] space-y-0.5 overflow-auto">
              {filtered.map((c) => {
                const visible = !draft.has(c.key);
                return (
                  <button key={c.key} onClick={() => toggleDraft(c.key)} className="flex w-full items-center justify-between rounded-md px-2 py-2 text-left text-[13px] text-ink hover:bg-white/5">
                    {c.label}
                    <span className={`flex h-4 w-4 items-center justify-center rounded border ${visible ? "border-brand-500 bg-brand-500" : "border-[var(--card-border)]"}`}>
                      {visible && <Check size={11} className="text-white" />}
                    </span>
                  </button>
                );
              })}
              {filtered.length === 0 && <div className="px-2 py-6 text-center text-[12px] text-ink-dim">No columns.</div>}
            </div>
          </div>
          {/* Selected */}
          <div className="w-[300px] shrink-0 p-3">
            <div className="mb-2 text-[12px] font-medium text-ink-dim">{selected.length} columns selected</div>
            <div className="max-h-[420px] space-y-1 overflow-auto">
              {selected.map((c) => (
                <div key={c.key} className="flex items-center justify-between rounded-lg border border-[var(--card-border)] bg-white/[0.02] px-2.5 py-2 text-[13px] text-ink">
                  <span className="flex items-center gap-2"><SlidersHorizontal size={12} className="text-ink-faint" /> {c.label}</span>
                  <button onClick={() => toggleDraft(c.key)} className="text-ink-faint hover:text-rose-400" title="Remove column"><X size={14} /></button>
                </div>
              ))}
            </div>
          </div>
        </div>
        <div className="flex items-center justify-between border-t border-[var(--card-border)] px-5 py-3">
          <button onClick={onClose} className="rounded-lg border border-[var(--card-border)] px-4 py-1.5 text-[13px] text-ink-dim hover:text-ink">Cancel</button>
          <button onClick={() => onApply(draft)} className="rounded-lg bg-white px-5 py-1.5 text-[13px] font-semibold text-black hover:bg-white/90">Apply</button>
        </div>
      </div>
    </div>
  );
}
