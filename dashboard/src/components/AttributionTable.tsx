"use client";

import { formatMoney, formatNumber, formatPercentValue, formatRatio, profitColor } from "@/lib/utils";
import { fetchChildren } from "@/lib/api";
import { ArrowUpDown, ChevronDown, ChevronRight, Loader2 } from "lucide-react";
import { useState, useCallback } from "react";

interface TableRow {
  id: string;
  name: string;
  raw_id?: string;
  level: string;
  metrics: {
    clicks: number;
    cost: number;
    total_revenue: number;
    revenue: number;
    profit: number;
    net_profit: number;
    reported: number | null;
    reported_delta: number | null;
  };
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
    clicks: number;
    cost: number;
    total_revenue: number;
    revenue: number;
    profit: number;
    net_profit: number;
    reported: number | null;
    reported_delta: number | null;
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
}

const TABS = [
  { key: "traffic_source", label: "Traffic Source" },
  { key: "ad_account", label: "Ad Account" },
  { key: "campaign", label: "Campaign" },
  { key: "ad_set", label: "Ad Set" },
  { key: "ad", label: "Ad" },
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
  const color = delta > 0 ? "text-emerald-400" : delta < 0 ? "text-red-400" : "text-gray-500";

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

export default function AttributionTable({ columns, rows, totals, activeTab, onTabChange, startDate, endDate, model, lookbackDays, useClickDate, compareRows = [], compareLabel = "" }: Props) {
  const [sortKey, setSortKey] = useState("profit");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [search, setSearch] = useState("");
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});
  const [childRows, setChildRows] = useState<Record<string, TableRow[]>>({});
  const [loadingChildren, setLoadingChildren] = useState<Record<string, boolean>>({});

  const metricCols = columns.filter((c) => c.type !== "dimension");
  const dimensionCol = columns.find((c) => c.type === "dimension");
  const compareById = new Map(compareRows.map((r) => [r.id, r]));

  const filtered = rows.filter((r) =>
    r.name.toLowerCase().includes(search.toLowerCase()) ||
    r.id.toLowerCase().includes(search.toLowerCase())
  );

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

    // Already loaded
    if (childRows[key]) {
      setExpanded((prev) => ({ ...prev, [key]: true }));
      return;
    }

    // Fetch children
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

  // Render a single data row
  const renderRow = (row: TableRow, depth: number, parentKey?: string) => {
    const rowKey = parentKey ? `${parentKey}>${row.id}` : row.id;
    const isExpanded = expanded[rowKey] || expanded[row.id];
    const isLoading = loadingChildren[rowKey] || loadingChildren[row.id];
    const children = childRows[rowKey] || childRows[row.id] || [];
    const levelColor = LEVEL_COLORS[row.level] || "";
    const levelLabel = LEVEL_LABELS[row.level] || "";
    const compareRow = depth === 0 ? compareById.get(row.id) : undefined;

    return (
      <tbody key={rowKey}>
        <tr
          className={`border-b border-[var(--card-border)] hover:bg-white/[0.02] transition-colors ${
            row.children_available ? "cursor-pointer" : ""
          } ${depth > 0 ? "bg-white/[0.01]" : ""}`}
          onClick={() => {
            if (depth === 0) toggleExpand(row);
            else if (parentKey) toggleExpandChild(parentKey, row);
          }}
        >
          <td className={`px-4 py-2.5 sticky left-0 bg-[var(--card)] z-10 ${depth > 0 ? `border-l-2 ${levelColor}` : ""}`}>
            <div className="flex items-center gap-1.5" style={{ paddingLeft: depth * 20 }}>
              {row.children_available ? (
                isLoading ? (
                  <Loader2 size={12} className="text-brand-500 animate-spin flex-shrink-0" />
                ) : isExpanded ? (
                  <ChevronDown size={12} className="text-brand-400 flex-shrink-0" />
                ) : (
                  <ChevronRight size={12} className="text-gray-500 flex-shrink-0" />
                )
              ) : (
                <span className="w-3" />
              )}
              <span className="text-gray-200 truncate max-w-[220px]" title={row.raw_id ? `${row.name} (ID: ${row.raw_id})` : row.name}>
                {row.name}
              </span>
              {row.raw_id && row.raw_id !== row.name && (
                <span className="text-[9px] text-gray-600 ml-1 px-1 py-0.5 rounded bg-white/5 font-mono" title={row.raw_id}>ID</span>
              )}
              {depth > 0 && levelLabel && (
                <span className="text-[9px] text-gray-600 ml-1 px-1 py-0.5 rounded bg-white/5">{levelLabel}</span>
              )}
              {row.children_count != null && row.children_count > 0 && (
                <span className="text-[10px] text-gray-600 ml-1">({row.children_count})</span>
              )}
            </div>
          </td>
          {metricCols.map((col) => (
            <td key={col.key} className="text-right px-3 py-2.5 text-gray-300 whitespace-nowrap">
              <CellValue col={col} metrics={row.metrics} />
              {compareRow && (
                <DeltaValue col={col} currentMetrics={row.metrics} compareMetrics={compareRow.metrics} />
              )}
            </td>
          ))}
        </tr>
        {/* Render children if expanded */}
        {isExpanded && children.length > 0 && (
          <>
            {children.map((child) => renderRow(child, depth + 1, row.id))}
          </>
        )}
        {isExpanded && children.length === 0 && !isLoading && (
          <tr className="border-b border-[var(--card-border)]">
            <td colSpan={metricCols.length + 1} className="px-4 py-2 text-gray-600 text-[11px]" style={{ paddingLeft: (depth + 1) * 20 + 16 }}>
              No child items found
            </td>
          </tr>
        )}
      </tbody>
    );
  };

  return (
    <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] overflow-hidden">
      {compareRows.length > 0 && (
        <div className="px-4 py-2 border-b border-[var(--card-border)] text-[11px] text-blue-300 bg-blue-500/5">
          Row deltas shown vs {compareLabel || "comparison"}
        </div>
      )}

      {/* Tab bar */}
      <div className="flex items-center gap-1 px-4 pt-3 pb-2 border-b border-[var(--card-border)] overflow-x-auto">
        {TABS.map((t) => (
          <button
            key={t.key}
            onClick={() => {
              onTabChange(t.key);
              setExpanded({});
              setChildRows({});
            }}
            className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors whitespace-nowrap ${
              activeTab === t.key
                ? "bg-brand-600 text-white"
                : "text-gray-400 hover:text-white hover:bg-white/5"
            }`}
          >
            {t.label}
          </button>
        ))}
        <div className="ml-auto">
          <input
            type="text"
            placeholder="Search by name or ID..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="bg-transparent border border-[var(--card-border)] rounded-lg px-3 py-1 text-xs text-gray-300 placeholder-gray-600 focus:outline-none focus:border-brand-500 w-44"
          />
        </div>
      </div>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-[var(--card-border)]">
              <th className="text-left px-4 py-2 text-gray-500 font-medium sticky left-0 bg-[var(--card)] z-10 min-w-[200px]">
                {dimensionCol?.label || "Name"}
              </th>
              {metricCols.map((col) => (
                <th
                  key={col.key}
                  className="text-right px-3 py-2 text-gray-500 font-medium cursor-pointer hover:text-gray-300 whitespace-nowrap"
                  onClick={() => handleSort(col.key)}
                >
                  <div className="flex items-center justify-end gap-1">
                    {col.label}
                    {sortKey === col.key && (
                      <ArrowUpDown size={10} className="text-brand-500" />
                    )}
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          {sorted.map((row) => renderRow(row, 0))}
          <tbody>
            {/* Totals row */}
            <tr className="border-t-2 border-brand-600/30 bg-white/[0.02] font-semibold">
              <td className="px-4 py-2.5 text-gray-300 sticky left-0 bg-[var(--card)] z-10">
                TOTAL
              </td>
              {metricCols.map((col) => (
                <td key={col.key} className="text-right px-3 py-2.5 whitespace-nowrap">
                  <CellValue col={col} metrics={totals} />
                </td>
              ))}
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  );
}
