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
import { useState, useCallback, useEffect, useRef } from "react";

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

const LEVEL_COLORS: Record<string, string> = {
  traffic_source: "border-l-brand-500",
  ad_account: "border-l-purple-500",
  campaign: "border-l-blue-500",
  ad_set: "border-l-cyan-500",
  ad: "border-l-emerald-500",
};

const LEVEL_LABELS: Record<string, string> = {
  campaign: "Campaign",
  ad_set: "Ad Set",
  ad: "Ad",
};

function CellValue({ col, metrics }: { col: Column; metrics: any }) {
  const val = metrics[col.key];
  if (col.type === "money") {
    return (
      <span className={col.key === "profit" || col.key === "net_profit" ? profitColor(val) : ""}>
        {formatMoney(val)}
      </span>
    );
  }
  if (col.type === "percent") {
    return <span>{formatPercentValue(val, 2)}</span>;
  }
  if (col.type === "ratio") {
    return <span>{formatRatio(val, 2)}</span>;
  }
  if (col.type === "number") return <span>{formatNumber(val)}</span>;
  return <span>{val ?? "—"}</span>;
}

function DeltaValue({ col, currentMetrics, compareMetrics }: { col: Column; currentMetrics: any; compareMetrics: any }) {
  const current = currentMetrics?.[col.key];
  const previous = compareMetrics?.[col.key];
  if (current == null || previous == null) return null;

  const delta = Number(current) - Number(previous);
  const sign = delta > 0 ? "+" : delta < 0 ? "-" : "";
  const color = delta > 0 ? "text-emerald-400" : delta < 0 ? "text-rose-400" : "text-ink-faint";

  if (col.type === "money") {
    return <div className={`text-[10px] ${color}`}>{`${sign}${formatMoney(Math.abs(delta))}`}</div>;
  }
  if (col.type === "number") {
    return <div className={`text-[10px] ${color}`}>{`${sign}${formatNumber(Math.abs(delta))}`}</div>;
  }
  if (col.type === "ratio") {
    return <div className={`text-[10px] ${color}`}>{`${sign}${Math.abs(delta).toFixed(2)}x`}</div>;
  }
  if (col.type === "percent") {
    return <div className={`text-[10px] ${color}`}>{`${sign}${Math.abs(delta).toFixed(2)} pp`}</div>;
  }
  return null;
}

type FilterOp = ">=" | "<=";

export default function AttributionTable({ columns, rows, totals, activeTab, onTabChange, startDate, endDate, model, lookbackDays, useClickDate, compareRows = [], compareLabel = "", platformFilter = "all", onPlatformFilterChange }: Props) {
  const [sortKey, setSortKey] = useState("profit");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [search, setSearch] = useState("");
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const [childRows, setChildRows] = useState<Record<string, TableRow[]>>({});
  const [loadingChildren, setLoadingChildren] = useState<Record<string, boolean>>({});
  const [lightbox, setLightbox] = useState<{ url: string; type: string; name: string; ad_id: string; video_id: string } | null>(null);
  const [videoUrl, setVideoUrl] = useState<string | null>(null);
  const [videoLoading, setVideoLoading] = useState(false);
  const videoRef = useRef<HTMLVideoElement>(null);

  // Hyros-style table controls
  const [density, setDensity] = useState<"compact" | "comfortable">("compact");
  const [hiddenCols, setHiddenCols] = useState<Set<string>>(new Set());
  const [showColsMenu, setShowColsMenu] = useState(false);
  const [showFilterMenu, setShowFilterMenu] = useState(false);
  const [filterKey, setFilterKey] = useState("");
  const [filterOp, setFilterOp] = useState<FilterOp>(">=");
  const [filterVal, setFilterVal] = useState("");
  const colsMenuRef = useRef<HTMLDivElement>(null);
  const filterMenuRef = useRef<HTMLDivElement>(null);

  const metricCols = columns.filter((c) => c.type !== "dimension");
  const shownCols = metricCols.filter((c) => !hiddenCols.has(c.key));
  const dimensionCol = columns.find((c) => c.type === "dimension");
  const compareById = new Map(compareRows.map((r) => [r.id, r]));
  const querySignature = `${activeTab}|${startDate || ""}|${endDate || ""}|${model || ""}|${lookbackDays || ""}|${useClickDate ? "click" : "conversion"}`;
  const cellPad = density === "compact" ? "py-1.5" : "py-3";
  const filterActive = Boolean(filterKey && filterVal !== "");

  const filtered = rows.filter((r) => {
    const needle = search.toLowerCase().trim();
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

  const platformsInData = Array.from(new Set(rows.map((r) => platformFromRow(r)).filter(Boolean)));
  const platformsKey = platformsInData.join("|");
  const visibleTotals = search.trim() || platformFilter !== "all" || filterActive ? recalcTotals(filtered) : (totals as MetricValues);
  const totalsLabel = search.trim() || platformFilter !== "all" || filterActive ? "Total (visible)" : "Total";

  useEffect(() => {
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

  const sorted = [...filtered].sort((a, b) => {
    const av = (a.metrics as any)[sortKey] ?? 0;
    const bv = (b.metrics as any)[sortKey] ?? 0;
    return sortDir === "desc" ? bv - av : av - bv;
  });

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
    try {
      const data = await fetchChildren({
        parent_tab: row.level,
        parent_id: row.id,
        start_date: startDate,
        end_date: endDate,
        model,
        lookback_days: lookbackDays,
        use_click_date: useClickDate,
      });
      setChildRows((prev) => ({ ...prev, [key]: data.rows || [] }));
      setExpanded((prev) => ({ ...prev, [key]: true }));
    } catch (err) {
      console.error("Failed to fetch children:", err);
    } finally {
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
    try {
      const data = await fetchChildren({
        parent_tab: row.level,
        parent_id: row.id,
        start_date: startDate,
        end_date: endDate,
        model,
        lookback_days: lookbackDays,
        use_click_date: useClickDate,
      });
      setChildRows((prev) => ({ ...prev, [key]: data.rows || [] }));
      setExpanded((prev) => ({ ...prev, [key]: true }));
    } catch (err) {
      console.error("Failed to fetch grandchildren:", err);
    } finally {
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

  const BACKEND = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

  useEffect(() => {
    if (!lightbox?.video_id || lightbox.type !== "video") return;
    setVideoLoading(true);
    const token = typeof window !== "undefined" ? (window.localStorage.getItem("hyros_auth_token") || "") : "";
    fetch(`${BACKEND}/api/ad-names/video-url?video_id=${lightbox.video_id}`, {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    })
      .then((r) => r.json())
      .then((d) => { if (d.video_url) setVideoUrl(d.video_url); })
      .catch(() => {})
      .finally(() => setVideoLoading(false));
  }, [lightbox?.video_id, lightbox?.type, BACKEND]);

  // Render a single data row
  const renderRow = (row: TableRow, depth: number, parentKey?: string): React.ReactNode => {
    const rowKey = parentKey ? `${parentKey}>${row.id}` : row.id;
    const isExpanded = Boolean(expanded[rowKey]);
    const isLoading = Boolean(loadingChildren[rowKey]);
    const children = childRows[rowKey] || [];
    const levelColor = LEVEL_COLORS[row.level] || "";
    const levelLabel = LEVEL_LABELS[row.level] || "";
    const compareRow = depth === 0 ? compareById.get(row.id) : undefined;

    return (
      <React.Fragment key={rowKey}>
        <tr
          className={`border-b border-[var(--card-border)]/70 hover:bg-white/[0.025] transition-colors ${
            row.children_available ? "cursor-pointer" : ""
          } ${depth > 0 ? "bg-white/[0.015]" : ""}`}
          onClick={() => {
            if (depth === 0) toggleExpand(row);
            else if (parentKey) toggleExpandChild(parentKey, row);
          }}
        >
          <td className={`px-4 ${cellPad} sticky left-0 bg-[var(--surface)] z-10 ${depth > 0 ? `border-l-2 ${levelColor}` : ""}`}>
            <div className="flex items-center gap-1.5" style={{ paddingLeft: depth * 20 }}>
              {row.children_available ? (
                isLoading ? (
                  <Loader2 size={13} className="text-brand-500 animate-spin flex-shrink-0" />
                ) : isExpanded ? (
                  <ChevronDown size={13} className="text-brand-400 flex-shrink-0" />
                ) : (
                  <ChevronRight size={13} className="text-ink-faint flex-shrink-0" />
                )
              ) : (
                <span className="w-3" />
              )}
              {/* Thumbnail for ad-level rows */}
              {row.level === "ad" && row.thumbnail_url && (
                <button
                  onClick={(e) => openLightbox(e, row)}
                  className="flex-shrink-0 w-8 h-8 rounded overflow-hidden border border-[var(--card-border)] relative group hover:border-brand-500 transition-colors"
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
                  <div className="absolute inset-0 bg-black/40 opacity-0 group-hover:opacity-100 flex items-center justify-center transition-opacity">
                    {row.creative_type === "video" ? <Play size={10} className="text-white" /> : <ImageIcon size={10} className="text-white" />}
                  </div>
                </button>
              )}
              {row.level === "ad" && !row.thumbnail_url && (
                <span className="flex-shrink-0 w-8 h-8 rounded border border-dashed border-[var(--card-border)] flex items-center justify-center">
                  <ImageIcon size={10} className="text-ink-faint" />
                </span>
              )}
              {depth === 0 && (() => {
                const plat = platformFromRow(row);
                const pm = plat ? PLATFORM_META[plat] : null;
                return pm ? <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${pm.dot}`} title={pm.label} /> : null;
              })()}
              <span className="text-ink truncate max-w-[220px]" title={row.raw_id ? `${row.name} (ID: ${row.raw_id})` : row.name}>
                {row.name}
              </span>
              {row.raw_id && row.raw_id !== row.name && (
                <span className="text-[9px] text-ink-faint ml-1 px-1 py-0.5 rounded bg-white/5 font-mono" title={row.raw_id}>ID</span>
              )}
              {depth > 0 && levelLabel && (
                <span className="text-[9px] text-ink-faint ml-1 px-1 py-0.5 rounded bg-white/5">{levelLabel}</span>
              )}
              {row.children_count != null && row.children_count > 0 && (
                <span className="text-[10px] text-ink-faint ml-1">({row.children_count})</span>
              )}
            </div>
          </td>
          {shownCols.map((col) => (
            <td key={col.key} className={`text-right px-3 ${cellPad} text-ink whitespace-nowrap tabular`}>
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
          <tr key={`${rowKey}-empty`} className="border-b border-[var(--card-border)]">
            <td colSpan={shownCols.length + 1} className="px-4 py-2 text-ink-faint text-[11px]" style={{ paddingLeft: (depth + 1) * 20 + 16 }}>
              No child items found
            </td>
          </tr>
        )}
      </React.Fragment>
    );
  };

  const TotalsRow = () => (
    <tr className="border-b border-[var(--card-border)] bg-white/[0.03] font-semibold">
      <td className={`px-4 ${cellPad} text-ink-bright sticky left-0 bg-[var(--surface)] z-10`}>{totalsLabel}</td>
      {shownCols.map((col) => (
        <td key={col.key} className={`text-right px-3 ${cellPad} whitespace-nowrap tabular text-ink-bright`}>
          <CellValue col={col} metrics={visibleTotals} />
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
    <div className="hpanel overflow-hidden">
      {compareRows.length > 0 && (
        <div className="px-4 py-2 border-b border-[var(--card-border)] text-[11px] text-blue-300 bg-blue-500/5">
          Row deltas shown vs {compareLabel || "comparison"}
        </div>
      )}

      {/* Grouping tabs (segmented control) */}
      <div className="flex items-center gap-2 px-4 pt-4 pb-3 overflow-x-auto">
        <div className="flex items-center gap-1 rounded-xl border border-[var(--card-border)] bg-[var(--surface-2)] p-1">
          {TABS.map((t) => (
            <button
              key={t.key}
              onClick={() => {
                onTabChange(t.key);
                onPlatformFilterChange?.("all");
                setExpanded({});
                setChildRows({});
              }}
              className={`flex items-center gap-1.5 px-3.5 py-1.5 rounded-lg text-[13px] font-medium transition-colors whitespace-nowrap ${
                activeTab === t.key
                  ? "bg-white/[0.07] text-ink-bright"
                  : "text-ink-dim hover:text-ink"
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
                  className={`flex items-center gap-1.5 px-2.5 py-1 rounded-lg text-[11px] font-medium transition-colors whitespace-nowrap ${
                    platformFilter === p ? "bg-white/10 text-ink-bright" : "text-ink-dim hover:text-ink"
                  }`}
                >
                  <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${meta.dot}`} />
                  {meta.label}
                </button>
              );
            })}
          </div>
        )}

        <div className="ml-auto flex items-center gap-2">
          {/* Search */}
          <div className="flex items-center gap-2 rounded-lg border border-[var(--card-border)] bg-[var(--surface-2)] px-2.5 h-8">
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
            className="flex items-center gap-1.5 h-8 rounded-lg border border-[var(--card-border)] bg-[var(--surface-2)] px-2.5 text-[12px] text-ink-dim hover:text-ink"
            title="Toggle row density"
          >
            <SlidersHorizontal size={13} />
            <span className="capitalize">{density}</span>
          </button>

          {/* Metric filter */}
          <div className="relative" ref={filterMenuRef}>
            <button
              onClick={() => { setShowFilterMenu((s) => !s); setShowColsMenu(false); }}
              className={`flex items-center gap-1.5 h-8 rounded-lg border px-2.5 text-[12px] ${
                filterActive
                  ? "border-brand-500/50 bg-brand-500/10 text-brand-300"
                  : "border-[var(--card-border)] bg-[var(--surface-2)] text-ink-dim hover:text-ink"
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
              onClick={() => { setShowColsMenu((s) => !s); setShowFilterMenu(false); }}
              className="flex items-center gap-1.5 h-8 rounded-lg border border-[var(--card-border)] bg-[var(--surface-2)] px-2.5 text-[12px] text-ink-dim hover:text-ink"
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

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-[13px]">
          <thead>
            <tr className="border-b border-[var(--card-border)]">
              <th className="text-left px-4 py-2.5 text-ink-dim font-medium sticky left-0 bg-[var(--surface)] z-10 min-w-[220px]">
                {dimensionCol?.label || "Name"}
              </th>
              {shownCols.map((col) => {
                const active = sortKey === col.key;
                return (
                  <th
                    key={col.key}
                    className={`text-right px-3 py-2.5 font-medium cursor-pointer whitespace-nowrap select-none ${active ? "text-ink-bright" : "text-ink-dim hover:text-ink"}`}
                    onClick={() => handleSort(col.key)}
                  >
                    <div className="flex items-center justify-end gap-1">
                      {col.label}
                      {active ? (
                        sortDir === "desc" ? <ChevronDown size={12} className="text-brand-400" /> : <ChevronUp size={12} className="text-brand-400" />
                      ) : (
                        <ChevronDown size={12} className="text-ink-faint/40" />
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
