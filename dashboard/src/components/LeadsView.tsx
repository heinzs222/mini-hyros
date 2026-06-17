"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import {
  RefreshCw,
  Upload,
  Download,
  Search,
  SlidersHorizontal,
  Filter,
  ChevronDown,
  ChevronsLeft,
  ChevronLeft,
  ChevronRight,
  ChevronsRight,
  Check,
  AlertCircle,
  CircleDollarSign,
  Repeat2,
  Users,
  Phone,
  PhoneCall,
  Mail,
  Hash,
} from "lucide-react";
import { fetchLeadJourneys, fetchRefundSummary } from "@/lib/api";
import { formatMoney } from "@/lib/utils";
import { useToast } from "@/components/Toast";

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
  _lineItems?: number;
};

type TabKey = "sales" | "subscriptions" | "leads" | "calls" | "phone";

const TABS: { key: TabKey; label: string; icon: React.ReactNode }[] = [
  { key: "sales", label: "Sales", icon: <CircleDollarSign size={14} /> },
  { key: "subscriptions", label: "Subscriptions", icon: <Repeat2 size={14} /> },
  { key: "leads", label: "Leads", icon: <Users size={14} /> },
  { key: "calls", label: "Calls", icon: <Phone size={14} /> },
  { key: "phone", label: "Phone closing", icon: <PhoneCall size={14} /> },
];

const SALE_TYPES = ["purchase", "payment"];
const LEAD_TYPES = ["lead", "formsubmission", "form_submission", "signup"];
const CALL_TYPES = ["booking", "appointment", "appointmentbooked", "appointment_booked"];

function typeBucket(t: string): TabKey | null {
  const lt = (t || "").toLowerCase();
  if (SALE_TYPES.includes(lt)) return "sales";
  if (LEAD_TYPES.includes(lt)) return "leads";
  if (CALL_TYPES.includes(lt)) return "calls";
  return null;
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

function titleCase(s: string): string {
  return (s || "").replace(/[_-]+/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function priceOf(r: LeadRow): number {
  return r.gross || r.value || 0;
}

/** Product-style name like Hyros ("Stripe 745"). Falls back to the conversion type. */
function nameOf(r: LeadRow): string {
  if (typeBucket(r.conversion_type) === "sales") {
    const p = priceOf(r);
    return p > 0 ? `Stripe ${Math.round(p)}` : "Stripe order";
  }
  return titleCase(r.conversion_type) || "Conversion";
}

function originOf(r: LeadRow): string {
  return r.path?.[0] || "Direct";
}
function lastSourceOf(r: LeadRow): string {
  return r.path && r.path.length >= 2 ? r.path[r.path.length - 2] : originOf(r);
}

type SortKey = "date" | "name" | "origin" | "last" | "lead" | "status" | "price" | "info" | "order";

const COLS: { label: string; key: SortKey; align?: "right" }[] = [
  { label: "Date", key: "date" },
  { label: "Name", key: "name" },
  { label: "Origin Source", key: "origin" },
  { label: "Last Source", key: "last" },
  { label: "Lead", key: "lead" },
  { label: "Status", key: "status" },
  { label: "Price", key: "price", align: "right" },
  { label: "Info", key: "info" },
  { label: "Order", key: "order" },
];

function sortValue(r: LeadRow, key: SortKey): string | number {
  switch (key) {
    case "date": return r.conversion_ts;
    case "name": return nameOf(r).toLowerCase();
    case "origin": return originOf(r).toLowerCase();
    case "last": return lastSourceOf(r).toLowerCase();
    case "lead": return r.customer_key_short.toLowerCase();
    case "status": return r.touchpoint_count > 0 ? 1 : 0;
    case "price": return priceOf(r);
    case "info": return r.touchpoint_count;
    case "order": return (r.order_id || "").toLowerCase();
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
  const [subFilter, setSubFilter] = useState<"all" | "recurring">("all");
  const [groupOrders, setGroupOrders] = useState(false);
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState<"all" | "attributed" | "unattributed">("all");
  const [showFilterMenu, setShowFilterMenu] = useState(false);
  const [density, setDensity] = useState<"comfortable" | "compact">("comfortable");
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [sortKey, setSortKey] = useState<SortKey>("date");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);
  const filterMenuRef = useRef<HTMLDivElement>(null);
  const filterActive = statusFilter !== "all";

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      const [data, refunds] = await Promise.allSettled([
        fetchLeadJourneys({ start_date: startDate, end_date: endDate, limit: 200, include_purchases: true }),
        fetchRefundSummary(),
      ]);
      if (data.status === "fulfilled") setRows(data.value?.rows || []);
      else throw data.reason;
      if (refunds.status === "fulfilled") setRefundCount(Number(refunds.value?.totals?.refund?.count || 0));
    } catch (e: any) {
      const msg = e?.message || "Failed to load leads";
      setError(msg);
      toast.error("Couldn’t load leads", { description: msg });
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [startDate, endDate]);

  useEffect(() => setPage(1), [tab, subFilter, groupOrders, search, statusFilter, sortKey, sortDir, pageSize]);
  useEffect(() => setSubFilter("all"), [tab]);
  useEffect(() => setSelected(new Set()), [tab, subFilter, groupOrders, search, statusFilter, startDate, endDate]);

  useEffect(() => {
    if (!showFilterMenu) return;
    const onDown = (e: MouseEvent) => {
      if (filterMenuRef.current && !filterMenuRef.current.contains(e.target as Node)) setShowFilterMenu(false);
    };
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [showFilterMenu]);

  const counts = useMemo(() => {
    const c: Record<TabKey, number> = { sales: 0, subscriptions: 0, leads: 0, calls: 0, phone: 0 };
    let recurring = 0;
    for (const r of rows) {
      const b = typeBucket(r.conversion_type);
      if (b) c[b] += 1;
      if (b === "sales" && r.conversion_type.toLowerCase() === "payment") recurring += 1;
    }
    return { ...c, recurring };
  }, [rows]);

  const display = useMemo(() => {
    const q = search.trim().toLowerCase();
    let list = rows.filter((r) => typeBucket(r.conversion_type) === tab);
    if (tab === "sales" && subFilter === "recurring") {
      list = list.filter((r) => r.conversion_type.toLowerCase() === "payment");
    }
    if (q) {
      list = list.filter((r) =>
        [r.customer_key_short, nameOf(r), (r.path || []).join(" "), r.order_id]
          .join(" ")
          .toLowerCase()
          .includes(q),
      );
    }
    if (statusFilter !== "all") {
      list = list.filter((r) => (r.touchpoint_count > 0 ? "attributed" : "unattributed") === statusFilter);
    }
    // Group line items into a single order row
    if (groupOrders) {
      const map = new Map<string, LeadRow>();
      for (const r of list) {
        const key = r.order_id || `${r.customer_key}|${r.conversion_ts}`;
        const existing = map.get(key);
        if (!existing) {
          map.set(key, { ...r, _lineItems: 1 });
        } else {
          existing.gross = (existing.gross || 0) + (r.gross || 0);
          existing.value = (existing.value || 0) + (r.value || 0);
          existing._lineItems = (existing._lineItems || 1) + 1;
        }
      }
      list = Array.from(map.values());
    }
    const dir = sortDir === "asc" ? 1 : -1;
    list = [...list].sort((a, b) => {
      const av = sortValue(a, sortKey);
      const bv = sortValue(b, sortKey);
      if (typeof av === "number" && typeof bv === "number") return (av - bv) * dir;
      return String(av).localeCompare(String(bv)) * dir;
    });
    return list;
  }, [rows, tab, subFilter, groupOrders, search, statusFilter, sortKey, sortDir]);

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

  const exportCsv = () => {
    const source = selected.size > 0 ? display.filter((r) => selected.has(rowKey(r))) : display;
    if (source.length === 0) {
      toast.info("Nothing to export", { description: "No rows match the current view." });
      return;
    }
    const lines = source.map((r) => [
      fmtDate(r.conversion_ts),
      nameOf(r),
      originOf(r),
      lastSourceOf(r),
      r.customer_key_short,
      r.touchpoint_count > 0 ? "Attributed" : "Unattributed",
      priceOf(r) || "",
      `${r.touchpoint_count} touches${r.time_to_convert ? ` · ${r.time_to_convert}` : ""}`,
      r.order_id,
    ]);
    const headers = ["Date", "Name", "Origin Source", "Last Source", "Lead", "Status", "Price", "Info", "Order"];
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

  const Header = ({ bottom = false }: { bottom?: boolean }) => (
    <tr className={`text-[12px] text-ink-dim ${bottom ? "border-t border-[var(--card-border)]" : "border-b border-[var(--card-border)]"}`}>
      <th className="w-10 px-4 py-3">
        {!bottom && (
          <input
            type="checkbox"
            className="accent-brand-500"
            aria-label="Select all on page"
            checked={allPageSelected}
            onChange={toggleSelectAllPage}
          />
        )}
      </th>
      {COLS.map((c) => {
        const active = sortKey === c.key;
        return (
          <th key={c.key} className={`whitespace-nowrap px-4 py-3 font-medium ${c.align === "right" ? "text-right" : ""}`}>
            <button
              type="button"
              onClick={() => toggleSort(c.key)}
              className={`flex items-center gap-1 ${c.align === "right" ? "ml-auto" : ""} ${active ? "text-ink" : "hover:text-ink"}`}
            >
              {c.label}
              <ChevronDown
                size={12}
                className={`transition-transform ${active && sortDir === "asc" ? "rotate-180" : ""} ${active ? "text-brand-400" : "text-ink-faint/50"}`}
              />
            </button>
          </th>
        );
      })}
    </tr>
  );

  const tabLabel = TABS.find((t) => t.key === tab)?.label.toLowerCase();

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="h-title text-[30px]">Leads</h1>
          <p className="mt-1 text-[13px] text-ink-dim">All tracked sales, leads and booked calls</p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => void load()}
            className="flex h-9 items-center gap-1.5 rounded-lg border border-[var(--card-border)] bg-[var(--surface)] px-3 text-[13px] text-ink-dim hover:text-ink"
            title="Refresh"
          >
            <RefreshCw size={14} className={loading ? "animate-spin" : ""} />
          </button>
          <button
            onClick={exportCsv}
            disabled={display.length === 0}
            className="flex h-9 items-center gap-1.5 rounded-lg border border-[var(--card-border)] bg-[var(--surface)] px-3 text-[13px] font-medium text-ink hover:bg-white/5 disabled:opacity-40"
            title="Export the current view to CSV"
          >
            <Download size={14} /> Export sales
          </button>
          <button
            disabled
            title="CSV import coming soon"
            className="flex h-9 cursor-not-allowed items-center gap-1.5 rounded-lg border border-[var(--card-border)] bg-[var(--surface)] px-3 text-[13px] font-medium text-ink-faint opacity-60"
          >
            <Upload size={14} /> Import sales
          </button>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex items-center gap-6 overflow-x-auto border-b border-[var(--card-border)]">
        {TABS.map((t) => {
          const active = tab === t.key;
          return (
            <button
              key={t.key}
              onClick={() => setTab(t.key)}
              className={`-mb-px flex items-center gap-1.5 whitespace-nowrap border-b-2 px-1 py-3 text-[13px] transition-colors ${
                active ? "border-ink-bright font-medium text-ink-bright" : "border-transparent text-ink-dim hover:text-ink"
              }`}
            >
              {t.icon} {t.label}
            </button>
          );
        })}
      </div>

      {/* Summary segmented bar + controls */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        {tab === "sales" ? (
          <div className="flex items-center gap-1 rounded-xl border border-[var(--card-border)] bg-[var(--surface)] p-1">
            <SegPill label="Sales" value={counts.sales} active={subFilter === "all"} onClick={() => setSubFilter("all")} />
            <SegPill label="Refunds" value={refundCount} muted />
            <SegPill label="Recurring sales" value={counts.recurring} active={subFilter === "recurring"} onClick={() => setSubFilter("recurring")} />
          </div>
        ) : (
          <div className="rounded-xl border border-[var(--card-border)] bg-[var(--surface)] px-4 py-2 text-[13px]">
            {TABS.find((t) => t.key === tab)?.label}
            <span className="ml-2 font-semibold text-ink-bright">{counts[tab] ?? 0}</span>
          </div>
        )}

        <div className="flex items-center gap-2">
          <button
            onClick={() => setGroupOrders((g) => !g)}
            className="flex items-center gap-2 rounded-lg border border-[var(--card-border)] bg-[var(--surface)] px-3 h-9 text-[13px] text-ink-dim hover:text-ink"
            title="Collapse line items belonging to the same order into one row"
          >
            Group into orders
            <span className={`relative inline-flex h-[20px] w-[36px] items-center rounded-full transition-colors ${groupOrders ? "bg-emerald-500" : "bg-white/15"}`}>
              <span className={`inline-block h-[14px] w-[14px] transform rounded-full bg-white transition-transform ${groupOrders ? "translate-x-[19px]" : "translate-x-[3px]"}`} />
            </span>
          </button>
          <div className="flex h-9 items-center gap-2 rounded-lg border border-[var(--card-border)] bg-[var(--surface)] px-3">
            <Search size={14} className="text-ink-faint" />
            <input
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search lead, source, order…"
              className="w-48 bg-transparent text-[13px] text-ink placeholder:text-ink-faint focus:outline-none"
            />
          </div>
          <div className="relative" ref={filterMenuRef}>
            <button
              onClick={() => setShowFilterMenu((s) => !s)}
              title="Filter by attribution status"
              className={`flex h-9 items-center gap-1.5 rounded-lg border bg-[var(--surface)] px-2.5 text-[13px] transition-colors ${
                filterActive ? "border-brand-500/50 text-brand-300" : "border-[var(--card-border)] text-ink-dim hover:text-ink"
              }`}
            >
              <Filter size={14} />
              {filterActive && <span className="capitalize">{statusFilter}</span>}
            </button>
            {showFilterMenu && (
              <div className="animate-hpop absolute right-0 z-30 mt-2 w-[210px] rounded-xl border border-[var(--card-border)] bg-[#0c0c11] p-2 shadow-2xl">
                <div className="px-2 py-1 text-[11px] font-semibold uppercase tracking-wide text-ink-dim">Attribution status</div>
                {([["all", "All"], ["attributed", "Attributed"], ["unattributed", "Unattributed"]] as const).map(([val, label]) => (
                  <button
                    key={val}
                    onClick={() => { setStatusFilter(val); setShowFilterMenu(false); }}
                    className="flex w-full items-center justify-between rounded-md px-2 py-1.5 text-left text-[13px] text-ink hover:bg-white/5"
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
            className="flex h-9 items-center gap-1.5 rounded-lg border border-[var(--card-border)] bg-[var(--surface)] px-2.5 text-[13px] text-ink-dim transition-colors hover:text-ink"
          >
            <SlidersHorizontal size={14} /> <span className="capitalize">{density}</span>
          </button>
        </div>
      </div>

      {/* Table */}
      <div className="hpanel overflow-hidden">
        <div className="overflow-x-auto">
          <table className={`w-full min-w-[1080px] text-left ${density === "compact" ? "leads-compact" : ""}`}>
            <thead className="sticky top-0 z-10 bg-[var(--surface)]">
              <Header />
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={COLS.length + 1} className="px-4 py-16 text-center text-ink-dim">
                    <RefreshCw size={20} className="mx-auto animate-spin text-brand-500" />
                  </td>
                </tr>
              ) : error ? (
                <tr>
                  <td colSpan={COLS.length + 1} className="px-4 py-16 text-center text-rose-400">{error}</td>
                </tr>
              ) : pageRows.length === 0 ? (
                <tr>
                  <td colSpan={COLS.length + 1} className="px-4 py-16 text-center text-ink-dim">
                    No {tabLabel} in this range.
                  </td>
                </tr>
              ) : (
                pageRows.map((r, i) => {
                  const price = priceOf(r);
                  const attributed = r.touchpoint_count > 0;
                  const origin = originOf(r);
                  const last = lastSourceOf(r);
                  return (
                    <tr
                      key={`${r.customer_key}-${r.conversion_ts}-${i}`}
                      className={`border-b border-[var(--card-border)]/60 text-[13px] text-ink transition-colors hover:bg-white/[0.02] ${
                        selected.has(rowKey(r)) ? "bg-brand-500/[0.06]" : ""
                      }`}
                    >
                      <td className="px-4 py-3">
                        <input
                          type="checkbox"
                          className="accent-brand-500"
                          checked={selected.has(rowKey(r))}
                          onChange={() => toggleRow(r)}
                          aria-label="Select row"
                        />
                      </td>
                      <td className="whitespace-nowrap px-4 py-3 tabular text-ink-dim">{fmtDate(r.conversion_ts)}</td>
                      <td className="whitespace-nowrap px-4 py-3 font-medium">
                        <span className="inline-flex items-center gap-1.5">
                          <CircleDollarSign size={13} className="text-ink-faint" />
                          {nameOf(r)}
                          {r._lineItems && r._lineItems > 1 && (
                            <span className="rounded bg-white/5 px-1 text-[10px] text-ink-dim">×{r._lineItems}</span>
                          )}
                        </span>
                      </td>
                      <td className="max-w-[190px] truncate px-4 py-3 text-ink-dim" title={origin}>{origin}</td>
                      <td className="max-w-[190px] truncate px-4 py-3 text-ink-dim" title={last}>{last}</td>
                      <td className="whitespace-nowrap px-4 py-3">
                        <span className="inline-flex items-center gap-1.5 text-ink-dim">
                          <Mail size={12} className="text-ink-faint" />
                          <span className="tabular">{r.customer_key_short}</span>
                        </span>
                      </td>
                      <td className="whitespace-nowrap px-4 py-3">
                        {attributed ? (
                          <span className="inline-flex items-center gap-1 text-emerald-400">
                            <Check size={13} /> Attributed
                          </span>
                        ) : (
                          <span className="inline-flex items-center gap-1 text-amber-400">
                            <AlertCircle size={13} /> Unattributed
                          </span>
                        )}
                      </td>
                      <td className="whitespace-nowrap px-4 py-3 text-right tabular font-medium text-emerald-400">
                        {price ? formatMoney(price) : "—"}
                      </td>
                      <td className="whitespace-nowrap px-4 py-3 text-ink-dim">
                        {r.touchpoint_count} touch{r.touchpoint_count === 1 ? "" : "es"}
                        {r.time_to_convert ? ` · ${r.time_to_convert}` : ""}
                      </td>
                      <td className="whitespace-nowrap px-4 py-3 text-ink-faint">
                        {r.order_id ? (
                          <span className="inline-flex items-center gap-1 font-mono text-[12px]">
                            <Hash size={11} /> {r.order_id.length > 14 ? `${r.order_id.slice(0, 14)}…` : r.order_id}
                          </span>
                        ) : "—"}
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
            {!loading && !error && pageRows.length > 0 && (
              <tfoot>
                <Header bottom />
              </tfoot>
            )}
          </table>
        </div>

        {/* Pagination */}
        <div className="flex flex-wrap items-center justify-center gap-2 border-t border-[var(--card-border)] px-4 py-3 text-[13px]">
          <PagerBtn disabled={page === 1} onClick={() => setPage(1)}><ChevronsLeft size={15} /></PagerBtn>
          <PagerBtn disabled={page === 1} onClick={() => setPage((p) => Math.max(1, p - 1))}><ChevronLeft size={15} /></PagerBtn>
          {pageNumbers.map((n, idx) => {
            const prev = pageNumbers[idx - 1];
            const gap = prev && n - prev > 1;
            return (
              <span key={n} className="flex items-center gap-2">
                {gap && <span className="text-ink-faint">…</span>}
                <button
                  onClick={() => setPage(n)}
                  className={`min-w-[30px] rounded-md px-2 py-1 ${
                    n === page ? "border border-white/15 bg-white/5 font-medium text-ink-bright" : "text-ink-dim hover:text-ink"
                  }`}
                >
                  {n}
                </button>
              </span>
            );
          })}
          <PagerBtn disabled={page === totalPages} onClick={() => setPage((p) => Math.min(totalPages, p + 1))}><ChevronRight size={15} /></PagerBtn>
          <PagerBtn disabled={page === totalPages} onClick={() => setPage(totalPages)}><ChevronsRight size={15} /></PagerBtn>
          {selected.size > 0 && (
            <span className="ml-3 rounded-md bg-brand-500/10 px-2 py-1 text-[12px] font-medium text-brand-300">
              {selected.size} selected
            </span>
          )}
          <span className="ml-3 flex items-center gap-2 text-ink-dim">
            Showing
            <select
              value={pageSize}
              onChange={(e) => setPageSize(Number(e.target.value))}
              className="rounded-md border border-[var(--card-border)] bg-[var(--surface-2)] px-1.5 py-0.5 text-ink focus:outline-none"
            >
              {[20, 50, 100].map((n) => <option key={n} value={n}>{n}</option>)}
            </select>
            of {display.length} results
          </span>
        </div>
      </div>
    </div>
  );
}

function SegPill({
  label,
  value,
  active,
  muted,
  onClick,
}: {
  label: string;
  value: number;
  active?: boolean;
  muted?: boolean;
  onClick?: () => void;
}) {
  const base = "rounded-lg px-4 py-1.5 text-[13px] transition-colors";
  if (muted) {
    return (
      <span className={`${base} cursor-default text-ink-dim`} title="Refunds tracked across all sales">
        {label} <span className="ml-1.5 font-semibold text-ink-bright">{value.toLocaleString()}</span>
      </span>
    );
  }
  return (
    <button
      onClick={onClick}
      className={`${base} ${active ? "bg-white/[0.06] text-ink-bright" : "text-ink-dim hover:text-ink"}`}
    >
      {label} <span className="ml-1.5 font-semibold text-ink-bright">{value.toLocaleString()}</span>
    </button>
  );
}

function PagerBtn({ children, disabled, onClick }: { children: React.ReactNode; disabled?: boolean; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className="flex h-7 w-7 items-center justify-center rounded-md text-ink-dim hover:text-ink disabled:opacity-30"
    >
      {children}
    </button>
  );
}
