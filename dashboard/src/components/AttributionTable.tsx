"use client";

import React from "react";
import { formatMoney, formatNumber, formatPercentValue, formatRatio, profitColor } from "@/lib/utils";
import { fetchChildren, apiFetch } from "@/lib/api";
import {
  ChevronDown,
  ChevronRight,
  ChevronUp,
  Loader2,
  Play,
  Image as ImageIcon,
  X,
  ExternalLink,
  Search,
  SlidersHorizontal,
  Columns3,
  Filter as FilterIcon,
  Globe,
  Building2,
  Megaphone,
  Layers3,
  Check,
} from "lucide-react";
import { memo, useState, useCallback, useEffect, useMemo, useRef } from "react";
import PlatformBadge, { isKnownPlatform } from "./PlatformBadge";

const BACKEND = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

type MetricValues = {
  impressions?: number;
  clicks: number;
  ctr?: number | null;
  orders: number;
  cost: number;
  cpc?: number | null;
  cpm?: number | null;
  cpa?: number | null;
  cvr?: number | null;
  total_revenue: number;
  revenue: number;
  aov?: number | null;
  rpc?: number | null;
  roas?: number | null;
  margin_pct?: number | null;
  profit: number;
  net_profit: number;
  reported: number | null;
  reported_delta: number | null;
  [key: string]: number | null | undefined;
};

interface TableRow {
  id: string;
  name: string;
  raw_id?: string;
  level: string;
  thumbnail_url?: string;
  creative_type?: string;
  video_id?: string;
  metrics: MetricValues;
  children_available: boolean;
  children_count: number | null;
}

interface Column {
  key: string;
  label: string;
  type: string;
}

interface Props {
  columns: Column[];
  rows: TableRow[];
  totals: {
    [key: string]: number | null | undefined;
  };
  activeTab: string;
  /** The tab the CURRENT report payload was built for. If it disagrees with
   * activeTab the rows belong to a previous tab (mid-refetch) and must not be
   * interpreted through the new tab's lens. */
  dataTab?: string;
  onTabChange: (tab: string) => void;
  startDate?: string;
  endDate?: string;
  model?: string;
  lookbackDays?: number;
  useClickDate?: boolean;
  compareRows?: TableRow[];
  compareLabel?: string;
  platformFilter?: string;
  onPlatformFilterChange?: (p: string) => void;
  /** When embedded inside ReportsView, hide the internal tab bar + toolbar (chrome is provided outside). */
  embedded?: boolean;
  densityValue?: "compact" | "comfortable";
  searchValue?: string;
  hiddenColumnKeys?: string[];
}

const PLATFORM_META: Record<string, { label: string; dot: string }> = {
  meta:    { label: "Meta",    dot: "bg-blue-500" },
  tiktok:  { label: "TikTok", dot: "bg-pink-500" },
  google:  { label: "Google", dot: "bg-yellow-400" },
};

function platformFromId(id: string): string {
  const parts = id.split("|");
  return parts.length > 1 ? parts[0].toLowerCase() : "";
}

function platformFromRow(row: Pick<TableRow, "id" | "name">): string {
  const fromId = platformFromId(row.id);
  if (fromId) return fromId;
  const name = `${row.id} ${row.name}`.toLowerCase();
  if (name.includes("google")) return "google";
  if (name.includes("facebook") || name.includes("meta")) return "meta";
  if (name.includes("tiktok")) return "tiktok";
  return "";
}

function toFiniteNumber(value: number | null | undefined): number {
  const n = Number(value ?? 0);
  return Number.isFinite(n) ? n : 0;
}

function recalcTotals(rows: TableRow[]): MetricValues {
  let impressions = 0;
  let clicks = 0;
  let orders = 0;
  let cost = 0;
  let totalRevenue = 0;
  let revenue = 0;
  let profit = 0;
  let netProfit = 0;
  let reported = 0;
  let hasReported = false;

  for (const row of rows) {
    impressions += toFiniteNumber(row.metrics.impressions);
    clicks += toFiniteNumber(row.metrics.clicks);
    orders += toFiniteNumber(row.metrics.orders);
    cost += toFiniteNumber(row.metrics.cost);
    totalRevenue += toFiniteNumber(row.metrics.total_revenue);
    revenue += toFiniteNumber(row.metrics.revenue);
    profit += toFiniteNumber(row.metrics.profit);
    netProfit += toFiniteNumber(row.metrics.net_profit);
    if (row.metrics.reported != null) {
      reported += toFiniteNumber(row.metrics.reported);
      hasReported = true;
    }
  }

  const cpc = clicks > 0 ? cost / clicks : null;
  const cpm = impressions > 0 ? (cost * 1000) / impressions : null;
  const ctr = impressions > 0 ? (clicks / impressions) * 100 : null;
  const cpa = orders > 0 ? cost / orders : null;
  const cvr = clicks > 0 ? (orders / clicks) * 100 : null;
  const aov = orders > 0 ? revenue / orders : null;
  const rpc = clicks > 0 ? revenue / clicks : null;
  const roas = cost > 0 ? revenue / cost : null;
  const marginPct = revenue > 0 ? (profit / revenue) * 100 : null;

  return {
    impressions,
    clicks,
    ctr,
    orders: Math.round(orders * 100) / 100,
    cost,
    cpc,
    cpm,
    cpa,
    cvr,
    total_revenue: totalRevenue,
    revenue,
    aov,
    rpc,
    roas,
    margin_pct: marginPct,
    profit,
    net_profit: netProfit,
    reported: hasReported ? reported : null,
    reported_delta: hasReported ? revenue - reported : null,
  };
}

const TABS = [
  { key: "traffic_source", label: "Traffic source", icon: <Globe size={14} /> },
  { key: "ad_account", label: "Ad Account", icon: <Building2 size={14} /> },
  { key: "campaign", label: "Campaign", icon: <Megaphone size={14} /> },
  { key: "ad_set", label: "Ad Set", icon: <Layers3 size={14} /> },
  { key: "ad", label: "Ad", icon: <ImageIcon size={14} /> },
];

const LEVEL_LABELS: Record<string, string> = {
  campaign: "Campaign",
  ad_set: "Ad set",
  ad: "Ad",
};

// Column-key sets driving the per-cell colour/weight rules (mirrors `colorFor` /
// `weightFor` in the design reference: reports-table-1a.dc.html).
const PRIMARY_MONEY_KEYS = new Set(["cost", "total_revenue", "revenue"]);
const PROFIT_ROLE_KEYS = new Set(["roas", "profit", "margin_pct", "net_profit"]);
// ROAS / Net Profit get the at-a-glance "pill" treatment.
const PILLED_KEYS = new Set(["roas", "net_profit"]);

function formatCellValue(col: Column, val: any): string {
  if (col.type === "money") return formatMoney(val);
  if (col.type === "percent") return formatPercentValue(val, 2);
  if (col.type === "ratio") return formatRatio(val, 2);
  if (col.type === "number") return formatNumber(val);
  return val ?? "—";
}

/** Colour class for a cell, following the design's colorFor rules exactly. */
function colorClassFor(col: Column, val: any): string {
  const num = val == null ? null : Number(val);

  // Signed money: profit / net_profit — reuse the shared profitColor() helper
  // so it stays in lockstep with the rest of the app's profit palette.
  if (col.key === "profit" || col.key === "net_profit") return profitColor(val);

  // Signed ratio: ROAS — universal 1.0x breakeven (0.09x and 0.00x read red).
  if (col.key === "roas") {
    if (num == null) return "text-ink-faint";
    return num >= 1 ? "text-positive" : "text-negative";
  }

  // Signed percent: Margin — negative red, positive neutral, 0/blank dim.
  if (col.key === "margin_pct") {
    if (num == null) return "text-ink-faint";
    if (num < 0) return "text-negative";
    if (num > 0) return "text-[#b9bcc8]";
    return "text-ink-faint";
  }

  // Everything else: null/empty and numeric 0 are dimmed.
  if (num == null || num === 0) return "text-ink-faint";

  if (PRIMARY_MONEY_KEYS.has(col.key)) return "text-[#eceef4]"; // Cost, Gross Rev., Net Rev.
  if (col.type === "money") return "text-[#b9bcc8]"; // CPC, CPM, CPA, AOV
  if (col.type === "number") return "text-[#eceef4]"; // Impressions, Clicks, Attr. Orders
  if (col.type === "percent") return "text-[#b9bcc8]"; // CTR, CVR

  // Unknown/other numeric column: sensible fallback.
  return "text-ink";
}

function weightClassFor(col: Column): string {
  return PRIMARY_MONEY_KEYS.has(col.key) || PROFIT_ROLE_KEYS.has(col.key) ? "font-semibold" : "font-medium";
}

/** Pill tint classes for the ROAS / Net Profit cells (data rows only). */
function pillClassFor(col: Column, val: any, pillsEnabled: boolean): string {
  if (!pillsEnabled || !PILLED_KEYS.has(col.key) || val == null) return "";
  const positive = col.key === "roas" ? Number(val) >= 1 : Number(val) >= 0;
  return positive
    ? "inline-block rounded-full px-[11px] py-[5px] bg-[var(--positive-tint)] border border-[rgba(62,224,161,.26)]"
    : "inline-block rounded-full px-[11px] py-[5px] bg-[var(--negative-tint)] border border-[rgba(255,107,122,.24)]";
}

function CellValue({ col, metrics, size = "13", pill = true, bold = false }: { col: Column; metrics: any; size?: "13" | "13.5"; pill?: boolean; bold?: boolean }) {
  const val = metrics[col.key];
  const sizeCls = size === "13.5" ? "text-[13.5px]" : "text-[13px]";
  // Totals row values are uniformly font-weight 700 (per the reference's totCells),
  // unlike data rows where weight varies by column role.
  const weightCls = bold ? "font-bold" : weightClassFor(col);
  const cls = `${colorClassFor(col, val)} ${weightCls} ${sizeCls} tabular ${pillClassFor(col, val, pill)}`;
  return <span className={cls}>{formatCellValue(col, val)}</span>;
}

function DeltaValue({ col, currentMetrics, compareMetrics }: { col: Column; currentMetrics: any; compareMetrics: any }) {
  const current = currentMetrics?.[col.key];
  const previous = compareMetrics?.[col.key];
  if (current == null || previous == null) return null;

  const delta = Number(current) - Number(previous);
  const sign = delta > 0 ? "+" : delta < 0 ? "−" : "";
  const color = delta > 0 ? "text-positive/90" : delta < 0 ? "text-negative/90" : "text-ink-faint";
  const cls = `mt-0.5 text-[10px] font-medium tabular ${color}`;

  if (col.type === "money") {
    return <div className={cls}>{`${sign}${formatMoney(Math.abs(delta))}`}</div>;
  }
  if (col.type === "number") {
    return <div className={cls}>{`${sign}${formatNumber(Math.abs(delta))}`}</div>;
  }
  if (col.type === "ratio") {
    return <div className={cls}>{`${sign}${Math.abs(delta).toFixed(2)}x`}</div>;
  }
  if (col.type === "percent") {
    return <div className={cls}>{`${sign}${Math.abs(delta).toFixed(2)} pp`}</div>;
  }
  return null;
}

type FilterOp = ">=" | "<=";

function AttributionTable({ columns, rows, totals, activeTab, dataTab, onTabChange, startDate, endDate, model, lookbackDays, useClickDate, compareRows = [], compareLabel = "", platformFilter = "all", onPlatformFilterChange, embedded = false, densityValue, searchValue, hiddenColumnKeys }: Props) {
  // Defense-in-depth against rendering the previous tab's rows under the new tab
  // (ReportsView already gates on this, but guard here too so the traffic-source
  // badge collapse is never applied to a mismatched payload).
  const rowsTabMatchesView = dataTab === undefined || dataTab === activeTab;
  const [sortKey, setSortKey] = useState("profit");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [search, setSearch] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const [childRows, setChildRows] = useState<Record<string, TableRow[]>>({});
  const [loadingChildren, setLoadingChildren] = useState<Record<string, boolean>>({});
  const [lightbox, setLightbox] = useState<{ url: string; type: string; name: string; ad_id: string; video_id: string } | null>(null);
  const [videoUrl, setVideoUrl] = useState<string | null>(null);
  const [videoLoading, setVideoLoading] = useState(false);
  const videoRef = useRef<HTMLVideoElement>(null);
  // Cache fetched lightbox video URLs by video_id so reopening doesn't refetch.
  const videoUrlCacheRef = useRef<Record<string, string>>({});
  // In-flight child-row fetches, so a report tab/range/model change can cancel
  // them and a slow expand can't land data from the wrong query afterwards.
  const childAbortRef = useRef<Record<string, AbortController>>({});

  // When embedded in ReportsView the search box is a controlled prop; otherwise
  // the table owns its own search state.
  const effSearch = searchValue !== undefined ? searchValue : search;

  // Debounce the effective search used for filtering/sorting (input stays responsive).
  useEffect(() => {
    const id = setTimeout(() => setDebouncedSearch(effSearch), 200);
    return () => clearTimeout(id);
  }, [effSearch]);

  // Hyros-style table controls
  const [density, setDensity] = useState<"compact" | "comfortable">("comfortable");
  const [hiddenCols, setHiddenCols] = useState<Set<string>>(new Set());
  const [showColsMenu, setShowColsMenu] = useState(false);
  const [showFilterMenu, setShowFilterMenu] = useState(false);
  const [filterKey, setFilterKey] = useState("");
  const [filterOp, setFilterOp] = useState<FilterOp>(">=");
  const [filterVal, setFilterVal] = useState("");
  const colsMenuRef = useRef<HTMLDivElement>(null);
  const filterMenuRef = useRef<HTMLDivElement>(null);

  // Effective control values: prefer external (controlled) props when embedded in ReportsView.
  const effDensity = densityValue ?? density;
  const effHidden = useMemo(
    () => (hiddenColumnKeys ? new Set(hiddenColumnKeys) : hiddenCols),
    [hiddenColumnKeys, hiddenCols],
  );

  const metricCols = useMemo(() => columns.filter((c) => c.type !== "dimension"), [columns]);
  const shownCols = useMemo(() => metricCols.filter((c) => !effHidden.has(c.key)), [metricCols, effHidden]);
  const dimensionCol = useMemo(() => columns.find((c) => c.type === "dimension"), [columns]);
  const compareById = useMemo(() => new Map(compareRows.map((r) => [r.id, r])), [compareRows]);
  const querySignature = `${activeTab}|${startDate || ""}|${endDate || ""}|${model || ""}|${lookbackDays || ""}|${useClickDate ? "click" : "conversion"}`;
  // Comfortable is the design default: roomy 15px-vertical cells; compact keeps
  // the same horizontal padding but tightens vertical rhythm to 10px.
  const metricPad = effDensity === "compact" ? "px-[14px] py-[10px]" : "px-[14px] py-[15px]";
  const sourcePad = effDensity === "compact" ? "px-[18px] py-[10px]" : "px-[18px] py-[15px]";
  const filterActive = Boolean(filterKey && filterVal !== "");

  const filtered = useMemo(() => {
    const needle = debouncedSearch.toLowerCase().trim();
    return rows.filter((r) => {
      const matchSearch = !needle
        || r.name.toLowerCase().includes(needle)
        || r.id.toLowerCase().includes(needle)
        || (r.raw_id || "").toLowerCase().includes(needle);
      const matchPlatform = platformFilter === "all" || platformFromRow(r) === platformFilter;
      let matchMetric = true;
      if (filterActive) {
        const v = Number((r.metrics as any)[filterKey] ?? 0);
        const threshold = Number(filterVal);
        matchMetric = filterOp === ">=" ? v >= threshold : v <= threshold;
      }
      return matchSearch && matchPlatform && matchMetric;
    });
  }, [rows, debouncedSearch, platformFilter, filterActive, filterKey, filterOp, filterVal]);

  const platformsInData = useMemo(
    () => Array.from(new Set(rows.map((r) => platformFromRow(r)).filter(Boolean))),
    [rows],
  );
  const platformsKey = platformsInData.join("|");
  const isFiltering = Boolean(debouncedSearch.trim()) || platformFilter !== "all" || filterActive;
  const visibleTotals = useMemo(
    () => (isFiltering ? recalcTotals(filtered) : (totals as MetricValues)),
    [isFiltering, filtered, totals],
  );
  const totalsLabel = isFiltering ? "Total (visible)" : "Total";

  useEffect(() => {
    // A new tab/range/model invalidates every expanded child set. Cancel any
    // in-flight child fetch so its (now stale) result can't land after the switch.
    for (const controller of Object.values(childAbortRef.current)) {
      try { controller.abort(); } catch {}
    }
    childAbortRef.current = {};
    setExpanded({});
    setChildRows({});
    setLoadingChildren({});
  }, [querySignature]);

  useEffect(() => {
    if (platformFilter !== "all" && !platformsKey.split("|").includes(platformFilter)) {
      onPlatformFilterChange?.("all");
    }
  }, [platformFilter, platformsKey, onPlatformFilterChange]);

  useEffect(() => {
    if (!showColsMenu && !showFilterMenu) return;
    const onDown = (e: MouseEvent) => {
      if (colsMenuRef.current && !colsMenuRef.current.contains(e.target as Node)) setShowColsMenu(false);
      if (filterMenuRef.current && !filterMenuRef.current.contains(e.target as Node)) setShowFilterMenu(false);
    };
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [showColsMenu, showFilterMenu]);

  const sorted = useMemo(
    () =>
      [...filtered].sort((a, b) => {
        const av = (a.metrics as any)[sortKey] ?? 0;
        const bv = (b.metrics as any)[sortKey] ?? 0;
        return sortDir === "desc" ? bv - av : av - bv;
      }),
    [filtered, sortKey, sortDir],
  );

  const handleSort = (key: string) => {
    if (sortKey === key) {
      setSortDir(sortDir === "desc" ? "asc" : "desc");
    } else {
      setSortKey(key);
      setSortDir("desc");
    }
  };

  const toggleExpand = useCallback(async (row: TableRow) => {
    if (!row.children_available) return;

    const key = row.id;
    if (expanded[key]) {
      setExpanded((prev) => ({ ...prev, [key]: false }));
      return;
    }

    if (childRows[key]) {
      setExpanded((prev) => ({ ...prev, [key]: true }));
      return;
    }

    setLoadingChildren((prev) => ({ ...prev, [key]: true }));
    childAbortRef.current[key]?.abort();
    const controller = new AbortController();
    childAbortRef.current[key] = controller;
    try {
      const data = await fetchChildren({
        parent_tab: row.level,
        parent_id: row.id,
        start_date: startDate,
        end_date: endDate,
        model,
        lookback_days: lookbackDays,
        use_click_date: useClickDate,
      }, controller.signal);
      setChildRows((prev) => ({ ...prev, [key]: data.rows || [] }));
      setExpanded((prev) => ({ ...prev, [key]: true }));
    } catch (err: any) {
      if (err?.name === "AbortError" || controller.signal.aborted) return;
      console.error("Failed to fetch children:", err);
    } finally {
      if (childAbortRef.current[key] === controller) delete childAbortRef.current[key];
      setLoadingChildren((prev) => ({ ...prev, [key]: false }));
    }
  }, [expanded, childRows, startDate, endDate, model, lookbackDays, useClickDate]);

  const toggleExpandChild = useCallback(async (parentKey: string, row: TableRow) => {
    if (!row.children_available) return;

    const key = `${parentKey}>${row.id}`;
    if (expanded[key]) {
      setExpanded((prev) => ({ ...prev, [key]: false }));
      return;
    }

    if (childRows[key]) {
      setExpanded((prev) => ({ ...prev, [key]: true }));
      return;
    }

    setLoadingChildren((prev) => ({ ...prev, [key]: true }));
    childAbortRef.current[key]?.abort();
    const controller = new AbortController();
    childAbortRef.current[key] = controller;
    try {
      const data = await fetchChildren({
        parent_tab: row.level,
        parent_id: row.id,
        start_date: startDate,
        end_date: endDate,
        model,
        lookback_days: lookbackDays,
        use_click_date: useClickDate,
      }, controller.signal);
      setChildRows((prev) => ({ ...prev, [key]: data.rows || [] }));
      setExpanded((prev) => ({ ...prev, [key]: true }));
    } catch (err: any) {
      if (err?.name === "AbortError" || controller.signal.aborted) return;
      console.error("Failed to fetch grandchildren:", err);
    } finally {
      if (childAbortRef.current[key] === controller) delete childAbortRef.current[key];
      setLoadingChildren((prev) => ({ ...prev, [key]: false }));
    }
  }, [expanded, childRows, startDate, endDate, model, lookbackDays, useClickDate]);

  const openLightbox = useCallback((e: React.MouseEvent, row: TableRow) => {
    e.stopPropagation();
    if (row.thumbnail_url) {
      const parts = row.id.split("|");
      const ad_id = parts[parts.length - 1] || "";
      setVideoUrl(null);
      setLightbox({ url: row.thumbnail_url, type: row.creative_type || "", name: row.name, ad_id, video_id: row.video_id || "" });
    }
  }, []);

  useEffect(() => {
    if (!lightbox?.video_id || lightbox.type !== "video") return;
    const videoId = lightbox.video_id;

    // Reuse the cached URL if we already fetched it for this video_id.
    const cached = videoUrlCacheRef.current[videoId];
    if (cached) {
      setVideoUrl(cached);
      setVideoLoading(false);
      return;
    }

    let cancelled = false;
    setVideoLoading(true);
    const token = typeof window !== "undefined" ? (window.localStorage.getItem("hyros_auth_token") || "") : "";
    fetch(`${BACKEND}/api/ad-names/video-url?video_id=${videoId}`, {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    })
      .then((r) => r.json())
      .then((d) => {
        if (cancelled) return;
        if (d.video_url) {
          videoUrlCacheRef.current[videoId] = d.video_url;
          setVideoUrl(d.video_url);
        }
      })
      .catch(() => {})
      .finally(() => { if (!cancelled) setVideoLoading(false); });

    return () => { cancelled = true; };
  }, [lightbox?.video_id, lightbox?.type]);

  // Zebra striping is keyed off render order, not row identity, so it must be a
  // plain counter local to this render pass (reset every render, shared by the
  // recursive renderRow closure so expanded children keep counting on from
  // their parent's position).
  let zebraIdx = 0;

  // Render a single data row
  const renderRow = (row: TableRow, depth: number, parentKey?: string): React.ReactNode => {
    const rowKey = parentKey ? `${parentKey}>${row.id}` : row.id;
    const isExpanded = Boolean(expanded[rowKey]);
    const isLoading = Boolean(loadingChildren[rowKey]);
    const children = childRows[rowKey] || [];
    const levelLabel = LEVEL_LABELS[row.level] || "";
    const compareRow = depth === 0 ? compareById.get(row.id) : undefined;
    const nameTitle = row.raw_id && row.raw_id !== row.name ? `${row.name} (ID: ${row.raw_id})` : row.name;
    const pillCls = "shrink-0 rounded px-1.5 py-[1px] text-[10px] font-medium leading-none text-ink-faint bg-white/[0.05]";

    // Zebra striping counts every rendered row in visual top-to-bottom order —
    // including inline-expanded children — via a render-scoped counter, not the
    // totals row (which always keeps its own #15151c band).
    const zebraPos = zebraIdx++;
    const rowBgCls = zebraPos % 2 ? "bg-[#0f0f16]" : "bg-[#0e0e13]";

    // Health spine: sign of this row's Net Profit.
    const netProfit = row.metrics.net_profit;
    const spineCls =
      netProfit != null && Number(netProfit) > 0
        ? "bg-positive"
        : netProfit != null && Number(netProfit) < 0
          ? "bg-negative"
          : "bg-transparent";

    return (
      <React.Fragment key={rowKey}>
        <tr
          className={`group ${rowBgCls} border-b border-white/5 transition-colors hover:bg-[var(--surface-2)] ${
            row.children_available ? "cursor-pointer" : ""
          }`}
          onClick={() => {
            if (depth === 0) toggleExpand(row);
            else if (parentKey) toggleExpandChild(parentKey, row);
          }}
        >
          <td className={`relative ${sourcePad} sticky left-0 z-10 ${rowBgCls} group-hover:bg-[var(--surface-2)]`}>
            <span className={`pointer-events-none absolute left-0 top-[9px] bottom-[9px] w-[3px] rounded-tr-[3px] rounded-br-[3px] ${spineCls}`} />
            <div className="flex items-center gap-2" style={{ paddingLeft: depth * 18 }}>
              {row.children_available ? (
                isLoading ? (
                  <Loader2 size={14} className="text-brand-400 animate-spin shrink-0" />
                ) : isExpanded ? (
                  <ChevronDown size={14} className="text-[#54596a] shrink-0" />
                ) : (
                  <ChevronRight size={14} className="text-[#54596a] shrink-0" />
                )
              ) : (
                <span className="w-3.5 shrink-0" />
              )}
              {/* Thumbnail for ad-level rows */}
              {row.level === "ad" && row.thumbnail_url && (
                <button
                  onClick={(e) => openLightbox(e, row)}
                  className="shrink-0 w-8 h-8 rounded-md overflow-hidden border border-[var(--card-border)] relative group/thumb hover:border-brand-500 transition-colors"
                  title="Preview creative"
                >
                  {/* eslint-disable-next-line @next/next/no-img-element */}
                  <img
                    src={row.thumbnail_url}
                    alt=""
                    className="w-full h-full object-cover"
                    onError={async (ev) => {
                      const img = ev.currentTarget;
                      if (img.dataset.refreshed) return;
                      img.dataset.refreshed = "1";
                      const parts = row.id.split("|");
                      const ad_id = parts[parts.length - 1];
                      try {
                        const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";
                        const r = await apiFetch(`${API_BASE}/api/ad-names/thumbnail?ad_id=${ad_id}`);
                        const d = await r.json();
                        if (d.thumbnail_url) img.src = d.thumbnail_url;
                      } catch {}
                    }}
                  />
                  <div className="absolute inset-0 bg-black/40 opacity-0 group-hover/thumb:opacity-100 flex items-center justify-center transition-opacity">
                    {row.creative_type === "video" ? <Play size={10} className="text-white" /> : <ImageIcon size={10} className="text-white" />}
                  </div>
                </button>
              )}
              {row.level === "ad" && !row.thumbnail_url && (
                <span className="shrink-0 w-8 h-8 rounded-md border border-dashed border-[var(--card-border)] flex items-center justify-center">
                  <ImageIcon size={11} className="text-ink-faint" />
                </span>
              )}
              {depth === 0 && activeTab === "traffic_source" && rowsTabMatchesView ? (
                // Traffic-source tab: the row IS the platform — show logo + the
                // canonical platform name ("Google Ads" / "Meta Ads" / "TikTok Ads").
                (() => {
                  const plat = platformFromRow(row);
                  const known = isKnownPlatform(plat);
                  return (
                    <PlatformBadge
                      platform={plat}
                      label={known ? undefined : row.name}
                      rawName={nameTitle}
                      size={25}
                      labelClassName={`truncate text-[14.5px] font-semibold ${known ? "text-[#e8eaf1]" : "text-[#a7aab6]"}`}
                    />
                  );
                })()
              ) : (
                // Entity rows (campaign / ad set / ad, at any depth): show the
                // platform logo as an indicator + the entity's own name.
                <>
                  <PlatformBadge platform={platformFromRow(row)} showLabel={false} size={depth === 0 ? 16 : 15} rawName={row.name} />
                  <span className="truncate max-w-[240px] text-[13px] text-ink" title={nameTitle}>
                    {row.name}
                  </span>
                </>
              )}
              {row.raw_id && row.raw_id !== row.name && (
                <span className={`${pillCls} font-mono`} title={row.raw_id}>ID</span>
              )}
              {depth > 0 && levelLabel && (
                <span className={pillCls}>{levelLabel}</span>
              )}
              {row.children_count != null && row.children_count > 0 && (
                <span className="shrink-0 text-[12px] font-medium text-[#636675] tabular">({row.children_count})</span>
              )}
            </div>
          </td>
          {shownCols.map((col) => (
            <td key={col.key} className={`text-right ${metricPad} whitespace-nowrap`}>
              <CellValue col={col} metrics={row.metrics} />
              {compareRow && (
                <DeltaValue col={col} currentMetrics={row.metrics} compareMetrics={compareRow.metrics} />
              )}
            </td>
          ))}
        </tr>
        {/* Render children inline as sibling tr elements */}
        {isExpanded && children.length > 0 && children.map((child) => renderRow(child, depth + 1, row.id))}
        {isExpanded && children.length === 0 && !isLoading && (
          <tr key={`${rowKey}-empty`} className="border-b border-white/5">
            <td colSpan={shownCols.length + 1} className="px-4 py-2 text-ink-faint text-[11px]" style={{ paddingLeft: (depth + 1) * 18 + 32 }}>
              No child items found
            </td>
          </tr>
        )}
      </React.Fragment>
    );
  };

  const TotalsRow = () => (
    <tr>
      <td className={`${sourcePad} sticky left-0 z-10 bg-[#15151c] border-t border-b border-white/[0.09]`}>
        <span className="text-[12.5px] font-extrabold uppercase tracking-[.06em] text-[#f0f1f6]">{totalsLabel}</span>
      </td>
      {shownCols.map((col) => (
        <td key={col.key} className={`text-right ${metricPad} whitespace-nowrap bg-[#15151c] border-t border-b border-white/[0.09]`}>
          {/* Totals row shows plain coloured values — no ROAS / Net Profit pill. */}
          <CellValue col={col} metrics={visibleTotals} size="13.5" pill={false} bold />
        </td>
      ))}
    </tr>
  );

  return (
    <>
    {/* Lightbox */}
    {lightbox && (
      <div
        className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 backdrop-blur-sm"
        onClick={() => setLightbox(null)}
      >
        <div className="relative max-w-2xl max-h-[90vh] flex flex-col gap-2" onClick={(e) => e.stopPropagation()}>
          <div className="flex items-center justify-between px-1">
            <span className="text-sm text-ink truncate max-w-[500px]">{lightbox.name}</span>
            <button onClick={() => setLightbox(null)} className="text-ink-dim hover:text-white ml-4">
              <X size={18} />
            </button>
          </div>
          {lightbox.type === "video" && videoUrl ? (
            <video
              ref={videoRef}
              src={videoUrl}
              controls
              autoPlay
              className="rounded-lg max-h-[75vh] max-w-full border border-[var(--card-border)] bg-black"
              style={{ minWidth: 480 }}
            />
          ) : lightbox.type === "video" && videoLoading ? (
            <div className="flex items-center justify-center rounded-lg border border-[var(--card-border)] bg-black/50" style={{ minWidth: 480, minHeight: 270 }}>
              <Loader2 size={28} className="text-brand-500 animate-spin" />
            </div>
          ) : lightbox.type === "video" && !videoUrl ? (
            <div className="flex flex-col items-center gap-3 rounded-lg border border-[var(--card-border)] bg-black/50 p-6" style={{ minWidth: 480 }}>
              {/* eslint-disable-next-line @next/next/no-img-element */}
              <img src={lightbox.url} alt={lightbox.name} className="rounded max-h-48 object-contain" />
              <span className="text-xs text-ink-dim">Video preview unavailable — open in Ads Manager to watch</span>
            </div>
          ) : (
            /* eslint-disable-next-line @next/next/no-img-element */
            <img
              src={lightbox.url}
              alt={lightbox.name}
              className="rounded-lg max-h-[80vh] max-w-full object-contain border border-[var(--card-border)]"
              style={{ minWidth: 320 }}
            />
          )}
          <div className="flex items-center justify-between px-1">
            {lightbox.type && (
              <span className="text-xs text-ink-dim capitalize">{lightbox.type} creative</span>
            )}
            {lightbox.ad_id && (
              <a
                href={`https://adsmanager.facebook.com/adsmanager/manage/ads?selected_ad_ids=${lightbox.ad_id}`}
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center gap-1 text-xs text-brand-400 hover:text-brand-300 transition-colors ml-auto"
                onClick={(e) => e.stopPropagation()}
              >
                Open in Ads Manager
                <ExternalLink size={11} />
              </a>
            )}
          </div>
        </div>
      </div>
    )}
    <div className="overflow-hidden rounded-[18px] border border-[var(--card-border)] bg-[#0e0e13] shadow-[0_30px_70px_-42px_rgba(0,0,0,.9)]">
      {compareRows.length > 0 && (
        <div className="px-4 py-2 border-b border-[var(--card-border)] text-[11px] text-blue-300 bg-blue-500/5">
          Row deltas shown vs {compareLabel || "comparison"}
        </div>
      )}

      {!embedded && (
      <>
      {/* Grouping tabs (segmented control) */}
      <div className="flex items-center gap-2 px-4 pt-4 pb-3 overflow-x-auto">
        <div className="flex items-center gap-1 rounded-[12px] border border-[#20202b] bg-[#121219] p-1">
          {TABS.map((t) => (
            <button
              key={t.key}
              onClick={() => {
                onTabChange(t.key);
                onPlatformFilterChange?.("all");
                setExpanded({});
                setChildRows({});
              }}
              className={`flex items-center gap-1.5 rounded-[9px] px-3.5 py-1.5 text-[13px] font-medium whitespace-nowrap transition-colors ${
                activeTab === t.key
                  ? "bg-white/[0.07] text-[#e8eaf1]"
                  : "text-[#797d8a] hover:text-ink"
              }`}
            >
              <span className={activeTab === t.key ? "text-brand-400" : "text-ink-faint"}>{t.icon}</span>
              {t.label}
            </button>
          ))}
        </div>
      </div>

      {/* Toolbar: platform pills + search + density + filter + columns */}
      <div className="flex flex-wrap items-center gap-2 px-4 pb-3 border-b border-[var(--card-border)]">
        {onPlatformFilterChange && platformsInData.length > 0 && (
          <div className="flex items-center gap-1">
            <button
              onClick={() => onPlatformFilterChange("all")}
              className={`px-2.5 py-1 rounded-lg text-[11px] font-medium transition-colors whitespace-nowrap ${
                platformFilter === "all" ? "bg-white/10 text-ink-bright" : "text-ink-dim hover:text-ink"
              }`}
            >
              All sources
            </button>
            {platformsInData.map((p) => {
              const meta = PLATFORM_META[p] || { label: p, dot: "bg-gray-400" };
              return (
                <button
                  key={p}
                  onClick={() => onPlatformFilterChange(platformFilter === p ? "all" : p)}
                  className={`flex items-center gap-1.5 pl-1.5 pr-2.5 py-1 rounded-lg text-[11px] font-medium transition-colors whitespace-nowrap ${
                    platformFilter === p ? "bg-white/10 text-ink-bright" : "text-ink-dim hover:text-ink"
                  }`}
                >
                  <PlatformBadge platform={p} showLabel={false} size={14} rawName={meta.label} />
                  {meta.label}
                </button>
              );
            })}
          </div>
        )}

        <div className="ml-auto flex items-center gap-2">
          {/* Search */}
          <div className="flex h-[34px] items-center gap-2 rounded-[9px] border border-[#20202b] bg-[#121219] px-2.5">
            <Search size={13} className="text-ink-faint" />
            <input
              type="text"
              placeholder="Search name or ID…"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="bg-transparent text-[12px] text-ink placeholder:text-ink-faint focus:outline-none w-40"
            />
          </div>

          {/* Density */}
          <button
            onClick={() => setDensity((d) => (d === "compact" ? "comfortable" : "compact"))}
            className="flex h-[34px] items-center gap-1.5 rounded-[9px] border border-[#20202b] bg-[#121219] px-2.5 text-[12px] text-ink-dim hover:text-ink"
            title="Toggle row density"
          >
            <SlidersHorizontal size={13} />
            <span className="capitalize">{density}</span>
          </button>

          {/* Metric filter */}
          <div className="relative" ref={filterMenuRef}>
            <button
              title="Filter rows by a metric threshold"
              onClick={() => { setShowFilterMenu((s) => !s); setShowColsMenu(false); }}
              className={`flex h-[34px] items-center gap-1.5 rounded-[9px] border px-2.5 text-[12px] transition-colors ${
                filterActive
                  ? "border-brand-500/50 bg-brand-500/10 text-brand-300"
                  : "border-[#20202b] bg-[#121219] text-ink-dim hover:text-ink"
              }`}
            >
              <FilterIcon size={13} /> Filter{filterActive ? " (1)" : ""}
            </button>
            {showFilterMenu && (
              <div className="animate-hpop absolute right-0 z-30 mt-2 w-[280px] rounded-xl border border-[var(--card-border)] bg-[#0c0c11] p-3 shadow-2xl">
                <div className="mb-2 text-[11px] font-semibold uppercase tracking-wide text-ink-dim">Filter by metric</div>
                <div className="space-y-2">
                  <select
                    value={filterKey}
                    onChange={(e) => setFilterKey(e.target.value)}
                    className="w-full rounded-lg border border-[var(--card-border)] bg-[var(--surface-2)] px-2.5 py-1.5 text-[12px] text-ink focus:outline-none"
                  >
                    <option value="">Select a metric…</option>
                    {metricCols.map((c) => (
                      <option key={c.key} value={c.key}>{c.label}</option>
                    ))}
                  </select>
                  <div className="flex items-center gap-2">
                    <select
                      value={filterOp}
                      onChange={(e) => setFilterOp(e.target.value as FilterOp)}
                      className="rounded-lg border border-[var(--card-border)] bg-[var(--surface-2)] px-2.5 py-1.5 text-[12px] text-ink focus:outline-none"
                    >
                      <option value=">=">≥ at least</option>
                      <option value="<=">≤ at most</option>
                    </select>
                    <input
                      type="number"
                      value={filterVal}
                      onChange={(e) => setFilterVal(e.target.value)}
                      placeholder="Value"
                      className="flex-1 rounded-lg border border-[var(--card-border)] bg-[var(--surface-2)] px-2.5 py-1.5 text-[12px] text-ink focus:outline-none"
                    />
                  </div>
                </div>
                <div className="mt-3 flex items-center justify-between">
                  <button
                    onClick={() => { setFilterKey(""); setFilterVal(""); }}
                    className="text-[12px] text-ink-dim hover:text-ink"
                  >
                    Clear
                  </button>
                  <button
                    onClick={() => setShowFilterMenu(false)}
                    className="rounded-lg bg-brand-600 px-3 py-1 text-[12px] font-medium text-white hover:bg-brand-700"
                  >
                    Apply
                  </button>
                </div>
              </div>
            )}
          </div>

          {/* Columns chooser */}
          <div className="relative" ref={colsMenuRef}>
            <button
              title="Show or hide metric columns"
              onClick={() => { setShowColsMenu((s) => !s); setShowFilterMenu(false); }}
              className="flex h-[34px] items-center gap-1.5 rounded-[9px] border border-[#20202b] bg-[#121219] px-2.5 text-[12px] text-ink-dim transition-colors hover:text-ink hover:border-white/20"
            >
              <Columns3 size={13} /> Columns
            </button>
            {showColsMenu && (
              <div className="animate-hpop absolute right-0 z-30 mt-2 max-h-[320px] w-[220px] overflow-auto rounded-xl border border-[var(--card-border)] bg-[#0c0c11] p-2 shadow-2xl">
                <div className="px-2 py-1 text-[11px] font-semibold uppercase tracking-wide text-ink-dim">Visible metrics</div>
                {metricCols.map((c) => {
                  const visible = !hiddenCols.has(c.key);
                  return (
                    <button
                      key={c.key}
                      onClick={() =>
                        setHiddenCols((prev) => {
                          const next = new Set(prev);
                          if (next.has(c.key)) next.delete(c.key);
                          else if (shownCols.length > 1) next.add(c.key);
                          return next;
                        })
                      }
                      className="flex w-full items-center justify-between gap-2 rounded-md px-2 py-1.5 text-left text-[12px] text-ink hover:bg-white/5"
                    >
                      {c.label}
                      <span className={`flex h-4 w-4 items-center justify-center rounded border ${visible ? "border-brand-500 bg-brand-500" : "border-[var(--card-border)]"}`}>
                        {visible && <Check size={11} className="text-white" />}
                      </span>
                    </button>
                  );
                })}
              </div>
            )}
          </div>
        </div>
      </div>
      </>
      )}

      {/* Table */}
      <div className="overflow-auto max-h-[70vh]">
        <table className="w-full min-w-[1380px] border-separate border-spacing-0 text-[13px]">
          <thead>
            <tr>
              <th className="sticky left-0 top-0 z-30 min-w-[238px] border-b border-white/[0.07] bg-[#101017] px-[18px] py-[14px] text-left text-[10.5px] font-bold uppercase tracking-[.08em] text-[#71757f]">
                {dimensionCol?.label || "Name"}
              </th>
              {shownCols.map((col) => {
                const active = sortKey === col.key;
                return (
                  <th
                    key={col.key}
                    className={`group sticky top-0 z-20 cursor-pointer select-none whitespace-nowrap border-b border-white/[0.07] bg-[#101017] px-[14px] py-[14px] text-right text-[10.5px] font-bold uppercase tracking-[.07em] transition-colors ${
                      active ? "text-[#c9ccd6]" : "text-[#71757f] hover:text-[#9ca0ac]"
                    }`}
                    onClick={() => handleSort(col.key)}
                  >
                    <div className="flex items-center justify-end gap-1">
                      {col.label}
                      {active ? (
                        sortDir === "desc" ? <ChevronDown size={12} className="text-[#8b5cf6]" /> : <ChevronUp size={12} className="text-[#8b5cf6]" />
                      ) : (
                        <ChevronDown size={12} className="text-ink-faint opacity-0 group-hover:opacity-60 transition-opacity" />
                      )}
                    </div>
                  </th>
                );
              })}
            </tr>
          </thead>
          <tbody>
            {/* Total row on top (Hyros-style) */}
            <TotalsRow />
            {sorted.map((row) => renderRow(row, 0))}
            {sorted.length === 0 && (
              <tr>
                <td colSpan={shownCols.length + 1} className="px-4 py-10 text-center text-ink-dim text-[13px]">
                  No rows match the current filters.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
    </>
  );
}

export default memo(AttributionTable);
