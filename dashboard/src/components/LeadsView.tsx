"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import {
  RefreshCw,
  Download,
  Plus,
  Search,
  SlidersHorizontal,
  Filter,
  ChevronDown,
  ChevronsLeft,
  ChevronLeft,
  ChevronRight,
  ChevronsRight,
  Check,
  Info,
} from "lucide-react";
import { fetchLeadJourneys, fetchRefundSummary } from "@/lib/api";
import { formatMoney } from "@/lib/utils";
import { useToast } from "@/components/Toast";
import CustomerProfileModal from "@/components/CustomerProfileModal";

function rowKey(r: { customer_key: string; conversion_ts: string; order_id: string }): string {
  return `${r.customer_key}|${r.conversion_ts}|${r.order_id}`;
}

interface Props {
  startDate: string;
  endDate: string;
}

type LeadRow = {
  customer_key: string;
  customer_key_short: string;
  conversion_type: string;
  conversion_ts: string;
  order_id: string;
  value: number;
  gross: number;
  time_to_convert: string;
  touchpoint_count: number;
  path: string[];
};

type TabKey = "sales" | "subscriptions" | "leads" | "calls";

const TABS: { key: TabKey; label: string }[] = [
  { key: "sales", label: "Sales" },
  { key: "subscriptions", label: "Subscriptions" },
  { key: "leads", label: "Leads" },
  { key: "calls", label: "Calls" },
];

const SALE_TYPES = ["purchase", "payment"];
const LEAD_TYPES = ["lead", "formsubmission", "form_submission", "signup"];
const CALL_TYPES = ["booking", "appointment", "appointmentbooked", "appointment_booked"];

function typeBucket(t: string): "sales" | "leads" | "calls" | null {
  const lt = (t || "").toLowerCase();
  if (SALE_TYPES.includes(lt)) return "sales";
  if (LEAD_TYPES.includes(lt)) return "leads";
  if (CALL_TYPES.includes(lt)) return "calls";
  return null;
}

/** Rows in the sales bucket render as "Customer / Won"; everything else as "Lead / —". */
function isSaleBucket(r: LeadRow): boolean {
  return typeBucket(r.conversion_type) === "sales";
}

function fmtDate(iso: string): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso.slice(0, 19).replace("T", " ");
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(
    d.getMinutes(),
  )}:${pad(d.getSeconds())}`;
}

function priceOf(r: LeadRow): number {
  return r.gross || r.value || 0;
}

function originOf(r: LeadRow): string {
  return r.path?.[0] || "Direct";
}
function lastSourceOf(r: LeadRow): string {
  return r.path && r.path.length >= 2 ? r.path[r.path.length - 2] : originOf(r);
}

// Deterministic avatar gradient per lead — same 5-gradient palette every time
// for a given customer_key so a lead's avatar colour never flickers on refetch.
const AVATAR_GRADIENTS = [
  "linear-gradient(135deg,#8b5cf6,#6366f1)",
  "linear-gradient(135deg,#3ee0a1,#22a06b)",
  "linear-gradient(135deg,#f59e0b,#d97706)",
  "linear-gradient(135deg,#22d3ee,#0ea5b7)",
  "linear-gradient(135deg,#fb7185,#e11d48)",
];

function hashStr(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
  return h;
}

function avatarGradient(customerKey: string): string {
  return AVATAR_GRADIENTS[hashStr(customerKey || "") % AVATAR_GRADIENTS.length];
}

function initialsOf(label: string): string {
  const alnum = (label || "").replace(/[^a-z0-9]/gi, "");
  return alnum.slice(0, 2).toUpperCase() || "?";
}

type SortKey = "lead" | "joined" | "first" | "last" | "income" | "status" | "stage";

const COLS: { label: string; key: SortKey; align?: "right" }[] = [
  { label: "Lead", key: "lead" },
  { label: "Joined On", key: "joined" },
  { label: "First Source", key: "first" },
  { label: "Last Source", key: "last" },
  { label: "Income", key: "income", align: "right" },
  { label: "Status", key: "status" },
  { label: "Stage", key: "stage" },
];

function sortValue(r: LeadRow, key: SortKey): string | number {
  switch (key) {
    case "lead": return r.customer_key_short.toLowerCase();
    case "joined": return r.conversion_ts;
    case "first": return originOf(r).toLowerCase();
    case "last": return lastSourceOf(r).toLowerCase();
    case "income": return priceOf(r);
    case "status": return isSaleBucket(r) ? 1 : 0;
    case "stage": return isSaleBucket(r) ? 1 : 0;
    default: return 0;
  }
}

export default function LeadsView({ startDate, endDate }: Props) {
  const toast = useToast();
  const [rows, setRows] = useState<LeadRow[]>([]);
  const [refundCount, setRefundCount] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [tab, setTab] = useState<TabKey>("sales");
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState<"all" | "attributed" | "unattributed">("all");
  const [showFilterMenu, setShowFilterMenu] = useState(false);
  const [density, setDensity] = useState<"comfortable" | "compact">("comfortable");
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [sortKey, setSortKey] = useState<SortKey>("joined");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const [profile, setProfile] = useState<{ customerKey: string; label: string } | null>(null);
  const filterMenuRef = useRef<HTMLDivElement>(null);
  const filterActive = statusFilter !== "all";
  // Monotonic request id: a newer load() supersedes any in-flight older one so a
  // slow older response can never overwrite newer rows (request race guard).
  const reqIdRef = useRef(0);

  const load = async (signal?: AbortSignal) => {
    const myId = ++reqIdRef.current;
    const isStale = () => signal?.aborted || myId !== reqIdRef.current;
    setLoading(true);
    setError(null);
    try {
      const [data, refunds] = await Promise.allSettled([
        fetchLeadJourneys({ start_date: startDate, end_date: endDate, limit: 200, include_purchases: true }, signal),
        fetchRefundSummary(),
      ]);
      if (isStale()) return;
      if (data.status === "fulfilled") setRows(data.value?.rows || []);
      else throw data.reason;
      if (refunds.status === "fulfilled") setRefundCount(Number(refunds.value?.totals?.refund?.count || 0));
    } catch (e: any) {
      if (isStale() || e?.name === "AbortError") return;
      const msg = e?.message || "Failed to load leads";
      setError(msg);
      toast.error("Couldn’t load leads", { description: msg });
    } finally {
      if (!isStale()) setLoading(false);
    }
  };

  useEffect(() => {
    const controller = new AbortController();
    void load(controller.signal);
    return () => controller.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [startDate, endDate]);

  useEffect(() => setPage(1), [tab, search, statusFilter, sortKey, sortDir, pageSize, startDate, endDate]);
  useEffect(() => setSelected(new Set()), [tab, search, statusFilter, startDate, endDate]);

  useEffect(() => {
    if (!showFilterMenu) return;
    const onDown = (e: MouseEvent) => {
      if (filterMenuRef.current && !filterMenuRef.current.contains(e.target as Node)) setShowFilterMenu(false);
    };
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [showFilterMenu]);

  const counts = useMemo(() => {
    const c: Record<TabKey, number> = { sales: 0, subscriptions: 0, leads: 0, calls: 0 };
    for (const r of rows) {
      const b = typeBucket(r.conversion_type);
      if (b === "sales") {
        c.sales += 1;
        if (r.conversion_type.toLowerCase() === "payment") c.subscriptions += 1;
      } else if (b) {
        c[b] += 1;
      }
    }
    return c;
  }, [rows]);

  const display = useMemo(() => {
    const q = search.trim().toLowerCase();
    let list = rows.filter((r) => {
      const b = typeBucket(r.conversion_type);
      if (tab === "subscriptions") return b === "sales" && r.conversion_type.toLowerCase() === "payment";
      return b === tab;
    });
    if (q) {
      list = list.filter((r) =>
        [r.customer_key_short, originOf(r), lastSourceOf(r)].join(" ").toLowerCase().includes(q),
      );
    }
    if (statusFilter !== "all") {
      list = list.filter((r) => (r.touchpoint_count > 0 ? "attributed" : "unattributed") === statusFilter);
    }
    const dir = sortDir === "asc" ? 1 : -1;
    list = [...list].sort((a, b) => {
      const av = sortValue(a, sortKey);
      const bv = sortValue(b, sortKey);
      if (typeof av === "number" && typeof bv === "number") return (av - bv) * dir;
      return String(av).localeCompare(String(bv)) * dir;
    });
    return list;
  }, [rows, tab, search, statusFilter, sortKey, sortDir]);

  const totalPages = Math.max(1, Math.ceil(display.length / pageSize));
  const pageRows = display.slice((page - 1) * pageSize, page * pageSize);

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    else {
      setSortKey(key);
      setSortDir("desc");
    }
  };

  const pageNumbers = useMemo(() => {
    const nums: number[] = [];
    const span = 2;
    for (let i = 1; i <= totalPages; i++) {
      if (i === 1 || i === totalPages || (i >= page - span && i <= page + span)) nums.push(i);
    }
    return nums;
  }, [totalPages, page]);

  const toggleRow = (r: LeadRow) => {
    const k = rowKey(r);
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(k)) next.delete(k);
      else next.add(k);
      return next;
    });
  };
  const pageKeys = pageRows.map(rowKey);
  const allPageSelected = pageKeys.length > 0 && pageKeys.every((k) => selected.has(k));
  const toggleSelectAllPage = () => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (allPageSelected) pageKeys.forEach((k) => next.delete(k));
      else pageKeys.forEach((k) => next.add(k));
      return next;
    });
  };

  const tabLabel = TABS.find((t) => t.key === tab)?.label.toLowerCase() ?? tab;

  const exportCsv = () => {
    const source = selected.size > 0 ? display.filter((r) => selected.has(rowKey(r))) : display;
    if (source.length === 0) {
      toast.info("Nothing to export", { description: "No rows match the current view." });
      return;
    }
    const lines = source.map((r) => {
      const sale = isSaleBucket(r);
      const price = priceOf(r);
      return [
        r.customer_key_short,
        fmtDate(r.conversion_ts),
        originOf(r),
        lastSourceOf(r),
        formatMoney(price > 0 ? price : 0),
        sale ? "Customer" : "Lead",
        sale ? "Won" : "—",
      ];
    });
    const headers = ["Lead", "Joined On", "First Source", "Last Source", "Income", "Status", "Stage"];
    const csv = [headers, ...lines]
      .map((cols) => cols.map((c) => `"${String(c).replace(/"/g, '""')}"`).join(","))
      .join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `leads-${tab}-${startDate}_${endDate}.csv`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
    toast.success("Export ready", {
      description: `Downloaded ${source.length} ${selected.size > 0 ? "selected " : ""}row${source.length === 1 ? "" : "s"} as CSV.`,
    });
  };

  const Header = () => (
    <tr>
      <th className="w-11 border-b border-white/[0.07] bg-[#101017] py-3.5 pl-[18px] pr-0">
        <input
          type="checkbox"
          className="h-[15px] w-[15px] rounded accent-[#8b5cf6]"
          aria-label="Select all on page"
          checked={allPageSelected}
          onChange={toggleSelectAllPage}
        />
      </th>
      {COLS.map((c) => {
        const active = sortKey === c.key;
        return (
          <th
            key={c.key}
            className={`whitespace-nowrap border-b border-white/[0.07] bg-[#101017] px-4 py-3.5 text-[10.5px] font-bold uppercase tracking-[.08em] text-[#71757f] ${
              c.key === "lead" ? "min-w-[220px]" : ""
            } ${c.align === "right" ? "text-right" : "text-left"}`}
          >
            <button
              type="button"
              onClick={() => toggleSort(c.key)}
              className={`inline-flex items-center gap-1 ${c.align === "right" ? "ml-auto flex" : ""} ${
                active ? "text-[#a9adba]" : "hover:text-[#9a9ea8]"
              }`}
            >
              {c.label}
              <ChevronDown
                size={11}
                className={`transition-transform ${active && sortDir === "asc" ? "rotate-180" : ""} ${
                  active ? "text-brand-400" : "text-[#71757f]/50"
                }`}
              />
            </button>
          </th>
        );
      })}
    </tr>
  );

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-[30px] font-extrabold tracking-[-0.02em] text-[#eceef4]">CRM</h1>
          <p className="mt-1.5 text-[13.5px] text-[#838794]">All tracked sales, leads and booked calls</p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => void load()}
            className="flex h-9 w-9 items-center justify-center rounded-[10px] border border-[#20202b] bg-[#121219] text-[#9aa0ad] hover:text-[#eceef4]"
            title="Refresh"
          >
            <RefreshCw size={14} className={loading ? "animate-spin" : ""} />
          </button>
          <button
            onClick={exportCsv}
            disabled={display.length === 0}
            className="flex h-9 items-center gap-1.5 rounded-[10px] border border-[#20202b] bg-[#121219] px-3.5 text-[12.5px] font-semibold text-[#9aa0ad] hover:text-[#eceef4] disabled:opacity-40"
            title="Export the current view to CSV"
          >
            <Download size={13} /> Export {tabLabel}
          </button>
          <button
            onClick={() => toast.info("Add lead", { description: "Manual lead creation is coming soon." })}
            className="flex h-9 items-center gap-1.5 rounded-[10px] bg-[#8b5cf6] px-3.5 text-[12.5px] font-bold text-white hover:bg-[#7c3aed]"
          >
            <Plus size={13} /> Add lead
          </button>
        </div>
      </div>

      {/* Sub-tabs */}
      <div className="flex items-center gap-6 overflow-x-auto border-b border-[#1c1c25]">
        {TABS.map((t) => {
          const active = tab === t.key;
          return (
            <button
              key={t.key}
              onClick={() => setTab(t.key)}
              className={`-mb-px whitespace-nowrap border-b-2 px-0.5 py-3 text-[13.5px] transition-colors ${
                active ? "border-[#8b5cf6] font-bold text-[#eceef4]" : "border-transparent font-medium text-[#797d8a] hover:text-[#b9bcc8]"
              }`}
            >
              {t.label}
            </button>
          );
        })}
      </div>

      {/* Count chip + toolbar */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <div className="inline-flex h-[38px] items-center gap-2.5 rounded-[11px] border border-[#1d1d27] bg-[#0e0e13] py-0 pl-[15px] pr-2">
            <span className="text-[13px] font-semibold capitalize text-[#b9bcc8]">{tabLabel} total</span>
            <span className="rounded-[7px] border border-white/[0.08] bg-white/[0.06] px-2.5 py-1 font-mono text-[12.5px] font-bold tabular text-[#eceef4]">
              {(counts[tab] ?? 0).toLocaleString()}
            </span>
          </div>
          {tab === "sales" && (
            <div
              className="inline-flex h-[38px] items-center gap-2 rounded-[11px] border border-[#1d1d27] bg-[#0e0e13]/60 px-3 text-[12px] text-[#6a6e7b]"
              title="Refunds tracked across all sales"
            >
              Refunds (all time)
              <span className="rounded-[7px] border border-white/[0.06] bg-white/[0.03] px-2 py-0.5 font-mono text-[11.5px] font-semibold tabular text-[#9aa0ad]">
                {refundCount.toLocaleString()}
              </span>
            </div>
          )}
        </div>

        <div className="flex items-center gap-2">
          <div className="flex h-9 items-center gap-2 rounded-[10px] border border-[#20202b] bg-[#121219] px-3">
            <Search size={13} className="text-[#797d8a]" />
            <input
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search lead, source…"
              className="w-48 bg-transparent text-[12.5px] text-[#dfe1ea] placeholder:text-[#797d8a] focus:outline-none"
            />
          </div>
          <div className="relative" ref={filterMenuRef}>
            <button
              onClick={() => setShowFilterMenu((s) => !s)}
              title="Filter by attribution status"
              className={`flex h-9 items-center gap-1.5 rounded-[10px] border bg-[#121219] px-3 text-[12.5px] font-medium transition-colors ${
                filterActive ? "border-brand-500/50 text-brand-300" : "border-[#20202b] text-[#9aa0ad] hover:text-[#eceef4]"
              }`}
            >
              <Filter size={13} />
              Filter
              {filterActive && <span className="capitalize">· {statusFilter}</span>}
            </button>
            {showFilterMenu && (
              <div className="animate-hpop absolute right-0 z-30 mt-2 w-[210px] rounded-xl border border-[#24242f] bg-[#14141b] p-2 shadow-2xl">
                <div className="px-2 py-1 text-[11px] font-semibold uppercase tracking-wide text-[#7e828f]">Attribution status</div>
                {([["all", "All"], ["attributed", "Attributed"], ["unattributed", "Unattributed"]] as const).map(([val, label]) => (
                  <button
                    key={val}
                    onClick={() => { setStatusFilter(val); setShowFilterMenu(false); }}
                    className="flex w-full items-center justify-between rounded-md px-2 py-1.5 text-left text-[13px] text-[#dfe1ea] hover:bg-white/5"
                  >
                    {label}
                    {statusFilter === val && <Check size={14} className="text-brand-400" />}
                  </button>
                ))}
              </div>
            )}
          </div>
          <button
            onClick={() => setDensity((d) => (d === "compact" ? "comfortable" : "compact"))}
            title="Toggle row density"
            className="flex h-9 items-center gap-1.5 rounded-[10px] border border-[#20202b] bg-[#121219] px-3 text-[12.5px] font-medium text-[#9aa0ad] transition-colors hover:text-[#eceef4]"
          >
            <SlidersHorizontal size={13} /> <span className="capitalize">{density}</span>
          </button>
        </div>
      </div>

      {/* Truncation notice — backend hard-caps at 200 newest rows */}
      {rows.length === 200 && (
        <div className="flex items-center gap-2 rounded-lg border border-amber-500/20 bg-amber-500/5 px-3 py-2 text-[12px] text-amber-300">
          <Info size={13} className="flex-shrink-0" />
          Showing newest 200 — narrow the date range to see more.
        </div>
      )}

      {/* Table */}
      <div className="overflow-hidden rounded-2xl border border-[#1d1d27] bg-[#0e0e13]">
        <div className="overflow-x-auto">
          <table className={`w-full min-w-[880px] text-left ${density === "compact" ? "leads-compact" : ""}`}>
            <thead className="sticky top-0 z-10 bg-[#101017]">
              <Header />
            </thead>
            <tbody>
              {loading ? (
                // Skeleton rows keep the table layout stable instead of
                // collapsing to a bare spinner, so a refetch reads as "updating".
                Array.from({ length: Math.min(pageSize, 10) }).map((_, i) => (
                  <tr key={`sk-${i}`} className="border-b border-white/[0.045]" style={{ opacity: 1 - i * 0.06 }}>
                    <td className="py-3.5 pl-[18px] pr-0"><div className="skeleton h-4 w-4" /></td>
                    {COLS.map((_c, ci) => (
                      <td key={ci} className="px-4 py-3.5">
                        <div className="skeleton h-3.5" style={{ width: `${45 + ((i * 7 + ci * 13) % 45)}%` }} />
                      </td>
                    ))}
                  </tr>
                ))
              ) : error ? (
                <tr>
                  <td colSpan={COLS.length + 1} className="px-4 py-16 text-center text-rose-400">{error}</td>
                </tr>
              ) : pageRows.length === 0 ? (
                <tr>
                  <td colSpan={COLS.length + 1} className="px-4 py-16 text-center text-[#7e828f]">
                    No {tabLabel} in this range.
                  </td>
                </tr>
              ) : (
                pageRows.map((r, i) => {
                  const price = priceOf(r);
                  const sale = isSaleBucket(r);
                  const origin = originOf(r);
                  const last = lastSourceOf(r);
                  const rowBgClass = i % 2 === 0 ? "bg-[#0e0e13]" : "bg-[#0f0f16]";
                  const grad = avatarGradient(r.customer_key);
                  const initials = initialsOf(r.customer_key_short);
                  return (
                    <tr
                      key={`${r.customer_key}-${r.conversion_ts}-${i}`}
                      className={`text-[13px] transition-colors hover:bg-white/[0.03] ${
                        selected.has(rowKey(r)) ? "bg-[#8b5cf6]/[0.08]" : rowBgClass
                      }`}
                    >
                      <td className="border-b border-white/[0.045] py-3.5 pl-[18px] pr-0">
                        <input
                          type="checkbox"
                          className="h-[15px] w-[15px] rounded accent-[#8b5cf6]"
                          checked={selected.has(rowKey(r))}
                          onChange={() => toggleRow(r)}
                          aria-label="Select row"
                        />
                      </td>
                      <td className="border-b border-white/[0.045] p-0">
                        <button
                          type="button"
                          onClick={() => r.customer_key && setProfile({ customerKey: r.customer_key, label: r.customer_key_short })}
                          disabled={!r.customer_key}
                          title="Open customer details"
                          className="flex w-full items-center gap-2.5 px-4 py-3.5 text-left disabled:cursor-default"
                        >
                          <span
                            className="inline-flex h-7 w-7 flex-shrink-0 items-center justify-center rounded-lg text-[11px] font-bold text-white"
                            style={{ backgroundImage: grad }}
                          >
                            {initials}
                          </span>
                          <span className="whitespace-nowrap text-[13.5px] font-semibold text-[#dfe1ea]">{r.customer_key_short}</span>
                        </button>
                      </td>
                      <td className="whitespace-nowrap border-b border-white/[0.045] px-4 py-3.5 font-mono text-[12.5px] font-medium tabular text-[#8b8f9c]">
                        {fmtDate(r.conversion_ts)}
                      </td>
                      <td className="border-b border-white/[0.045] px-4 py-3.5">
                        <span
                          className="inline-block max-w-[200px] truncate rounded-[7px] border border-[#24242e] bg-[#15151c] px-2.5 py-1 align-middle text-[12px] font-medium text-[#a2a6b2]"
                          title={origin}
                        >
                          {origin}
                        </span>
                      </td>
                      <td className="border-b border-white/[0.045] px-4 py-3.5">
                        <span
                          className="inline-block max-w-[200px] truncate rounded-[7px] border border-[#24242e] bg-[#15151c] px-2.5 py-1 align-middle text-[12px] font-medium text-[#a2a6b2]"
                          title={last}
                        >
                          {last}
                        </span>
                      </td>
                      <td
                        className={`whitespace-nowrap border-b border-white/[0.045] px-4 py-3.5 text-right tabular text-[13px] font-semibold ${
                          price > 0 ? "text-[#3ee0a1]" : "text-[#565a67]"
                        }`}
                      >
                        {formatMoney(price > 0 ? price : 0)}
                      </td>
                      <td className="whitespace-nowrap border-b border-white/[0.045] px-4 py-3.5">
                        {sale ? (
                          <span className="inline-flex items-center gap-1.5 rounded-full border border-[rgba(62,224,161,.24)] bg-[rgba(62,224,161,.1)] px-2.5 py-1 text-[11.5px] font-semibold text-[#3ee0a1]">
                            <span className="h-[5px] w-[5px] rounded-full bg-[#3ee0a1]" /> Customer
                          </span>
                        ) : (
                          <span className="inline-flex items-center gap-1.5 rounded-full border border-[rgba(139,92,246,.24)] bg-[rgba(139,92,246,.1)] px-2.5 py-1 text-[11.5px] font-semibold text-[#b9a3ef]">
                            <span className="h-[5px] w-[5px] rounded-full bg-[#a78bfa]" /> Lead
                          </span>
                        )}
                      </td>
                      <td
                        className={`whitespace-nowrap border-b border-white/[0.045] px-4 py-3.5 text-[12.5px] font-semibold ${
                          sale ? "text-[#3ee0a1]" : "text-[#565a67]"
                        }`}
                      >
                        {sale ? "Won" : "—"}
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        <div className="flex flex-wrap items-center justify-between gap-3 border-t border-white/[0.06] px-[18px] py-3 text-[12.5px] text-[#797d8a]">
          <span>
            Showing {pageRows.length} of {display.length.toLocaleString()} {tabLabel}
          </span>
          <div className="flex flex-wrap items-center gap-3">
            {selected.size > 0 && (
              <span className="rounded-md bg-brand-500/10 px-2 py-1 text-[12px] font-medium text-brand-300">
                {selected.size} selected
              </span>
            )}
            <div className="flex items-center gap-1.5">
              <PagerBtn disabled={page === 1} onClick={() => setPage(1)}><ChevronsLeft size={14} /></PagerBtn>
              <PagerBtn disabled={page === 1} onClick={() => setPage((p) => Math.max(1, p - 1))}><ChevronLeft size={14} /></PagerBtn>
              {pageNumbers.map((n, idx) => {
                const prev = pageNumbers[idx - 1];
                const gap = prev && n - prev > 1;
                return (
                  <span key={n} className="flex items-center gap-1.5">
                    {gap && <span className="text-[#565a67]">…</span>}
                    <button
                      onClick={() => setPage(n)}
                      className={`flex h-[26px] min-w-[26px] items-center justify-center rounded-[7px] px-1.5 ${
                        n === page ? "bg-white/[0.06] font-bold text-[#eceef4]" : "text-[#797d8a] hover:text-[#b9bcc8]"
                      }`}
                    >
                      {n}
                    </button>
                  </span>
                );
              })}
              <PagerBtn disabled={page === totalPages} onClick={() => setPage((p) => Math.min(totalPages, p + 1))}><ChevronRight size={14} /></PagerBtn>
              <PagerBtn disabled={page === totalPages} onClick={() => setPage(totalPages)}><ChevronsRight size={14} /></PagerBtn>
            </div>
            <span className="flex items-center gap-2">
              <select
                value={pageSize}
                onChange={(e) => setPageSize(Number(e.target.value))}
                className="rounded-md border border-[#20202b] bg-[#121219] px-1.5 py-1 text-[#dfe1ea] focus:outline-none"
              >
                {[20, 50, 100].map((n) => <option key={n} value={n}>{n}</option>)}
              </select>
              per page
            </span>
          </div>
        </div>
      </div>

      {profile && (
        <CustomerProfileModal
          customerKey={profile.customerKey}
          label={profile.label}
          onClose={() => setProfile(null)}
        />
      )}
    </div>
  );
}

function PagerBtn({ children, disabled, onClick }: { children: React.ReactNode; disabled?: boolean; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className="flex h-[26px] w-[26px] items-center justify-center rounded-[7px] text-[#797d8a] hover:text-[#b9bcc8] disabled:opacity-30"
    >
      {children}
    </button>
  );
}
