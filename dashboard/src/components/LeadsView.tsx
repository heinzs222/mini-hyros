"use client";

import { useEffect, useMemo, useState } from "react";
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
  UserPlus,
  PhoneCall,
} from "lucide-react";
import { fetchLeadJourneys } from "@/lib/api";
import { formatMoney } from "@/lib/utils";

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

type TabKey = "sales" | "subscriptions" | "leads" | "calls" | "phone";

const TABS: { key: TabKey; label: string; icon: React.ReactNode }[] = [
  { key: "sales", label: "Sales", icon: <CircleDollarSign size={14} /> },
  { key: "subscriptions", label: "Subscriptions", icon: <RefreshCw size={14} /> },
  { key: "leads", label: "Leads", icon: <UserPlus size={14} /> },
  { key: "calls", label: "Calls", icon: <PhoneCall size={14} /> },
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

const PAGE_SIZE = 20;
const COLUMNS = ["Date", "Name", "Price", "Origin Source", "Last Source", "Lead", "Status", "Info"];

export default function LeadsView({ startDate, endDate }: Props) {
  const [rows, setRows] = useState<LeadRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [tab, setTab] = useState<TabKey>("sales");
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState<"date" | "price">("date");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [page, setPage] = useState(1);

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchLeadJourneys({
        start_date: startDate,
        end_date: endDate,
        limit: 200,
        include_purchases: true,
      });
      setRows(data?.rows || []);
    } catch (e: any) {
      setError(e?.message || "Failed to load leads");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [startDate, endDate]);

  useEffect(() => setPage(1), [tab, search, sortKey, sortDir]);

  const counts = useMemo(() => {
    const c: Record<TabKey, number> = { sales: 0, subscriptions: 0, leads: 0, calls: 0, phone: 0 };
    for (const r of rows) {
      const b = typeBucket(r.conversion_type);
      if (b) c[b] += 1;
    }
    return c;
  }, [rows]);

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    let list = rows.filter((r) => typeBucket(r.conversion_type) === tab);
    if (q) {
      list = list.filter((r) =>
        [r.customer_key_short, r.conversion_type, (r.path || []).join(" "), r.order_id]
          .join(" ")
          .toLowerCase()
          .includes(q),
      );
    }
    const dir = sortDir === "asc" ? 1 : -1;
    list = [...list].sort((a, b) => {
      if (sortKey === "price") return (a.gross - b.gross || a.value - b.value) * dir;
      return (a.conversion_ts < b.conversion_ts ? -1 : a.conversion_ts > b.conversion_ts ? 1 : 0) * dir;
    });
    return list;
  }, [rows, tab, search, sortKey, sortDir]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const pageRows = filtered.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);

  const toggleSort = (key: "date" | "price") => {
    if (sortKey === key) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    else {
      setSortKey(key);
      setSortDir("desc");
    }
  };

  const pageNumbers = useMemo(() => {
    const nums: number[] = [];
    const span = 3;
    for (let i = 1; i <= totalPages; i++) {
      if (i === 1 || i === totalPages || (i >= page - span && i <= page + span)) nums.push(i);
    }
    return nums;
  }, [totalPages, page]);

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-[28px] font-semibold tracking-tight text-ink-bright">Leads</h1>
          <p className="mt-1 text-[13px] text-ink-dim">All tracked sales, leads and booked calls</p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => void load()}
            className="flex h-9 items-center gap-1.5 rounded-lg border border-[var(--card-border)] bg-[var(--surface)] px-3 text-[13px] text-ink-dim hover:text-ink"
          >
            <RefreshCw size={14} className={loading ? "animate-spin" : ""} />
          </button>
          <button className="flex h-9 items-center gap-1.5 rounded-lg border border-[var(--card-border)] bg-[var(--surface)] px-3 text-[13px] font-medium text-ink hover:bg-white/5">
            <Download size={14} /> Export sales
          </button>
          <button className="flex h-9 items-center gap-1.5 rounded-lg border border-[var(--card-border)] bg-[var(--surface)] px-3 text-[13px] font-medium text-ink hover:bg-white/5">
            <Upload size={14} /> Import sales
          </button>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex items-center gap-6 border-b border-[var(--card-border)]">
        {TABS.map((t) => {
          const active = tab === t.key;
          return (
            <button
              key={t.key}
              onClick={() => setTab(t.key)}
              className={`-mb-px flex items-center gap-1.5 border-b-2 px-1 py-3 text-[13px] transition-colors ${
                active
                  ? "border-ink-bright font-medium text-ink-bright"
                  : "border-transparent text-ink-dim hover:text-ink"
              }`}
            >
              {t.icon} {t.label}
            </button>
          );
        })}
      </div>

      {/* Summary chips + controls */}
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-2 rounded-xl border border-[var(--card-border)] bg-[var(--surface)] p-1">
          <Chip label="Sales" value={counts.sales} active={tab === "sales"} onClick={() => setTab("sales")} />
          <Chip label="Leads" value={counts.leads} active={tab === "leads"} onClick={() => setTab("leads")} />
          <Chip label="Calls" value={counts.calls} active={tab === "calls"} onClick={() => setTab("calls")} />
        </div>
        <div className="flex items-center gap-2">
          <div className="flex h-9 items-center gap-2 rounded-lg border border-[var(--card-border)] bg-[var(--surface)] px-3">
            <Search size={14} className="text-ink-faint" />
            <input
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search lead, source…"
              className="w-44 bg-transparent text-[13px] text-ink placeholder:text-ink-faint focus:outline-none"
            />
          </div>
          <button className="flex h-9 w-9 items-center justify-center rounded-lg border border-[var(--card-border)] bg-[var(--surface)] text-ink-dim hover:text-ink">
            <Filter size={14} />
          </button>
          <button className="flex h-9 w-9 items-center justify-center rounded-lg border border-[var(--card-border)] bg-[var(--surface)] text-ink-dim hover:text-ink">
            <SlidersHorizontal size={14} />
          </button>
        </div>
      </div>

      {/* Table */}
      <div className="hpanel overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full min-w-[920px] text-left">
            <thead>
              <tr className="border-b border-[var(--card-border)] text-[12px] text-ink-dim">
                <th className="w-10 px-4 py-3">
                  <input type="checkbox" className="accent-brand-500" />
                </th>
                {COLUMNS.map((c) => {
                  const sortable = c === "Date" || c === "Price";
                  const key = c === "Date" ? "date" : "price";
                  return (
                    <th key={c} className="whitespace-nowrap px-4 py-3 font-medium">
                      <button
                        type="button"
                        disabled={!sortable}
                        onClick={() => sortable && toggleSort(key as "date" | "price")}
                        className={`flex items-center gap-1 ${sortable ? "hover:text-ink" : "cursor-default"}`}
                      >
                        {c}
                        {sortable && (
                          <ChevronDown
                            size={12}
                            className={`transition-transform ${
                              sortKey === key && sortDir === "asc" ? "rotate-180" : ""
                            } ${sortKey === key ? "text-ink" : "text-ink-faint"}`}
                          />
                        )}
                      </button>
                    </th>
                  );
                })}
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={COLUMNS.length + 1} className="px-4 py-16 text-center text-ink-dim">
                    <RefreshCw size={20} className="mx-auto animate-spin text-brand-500" />
                  </td>
                </tr>
              ) : error ? (
                <tr>
                  <td colSpan={COLUMNS.length + 1} className="px-4 py-16 text-center text-rose-400">
                    {error}
                  </td>
                </tr>
              ) : pageRows.length === 0 ? (
                <tr>
                  <td colSpan={COLUMNS.length + 1} className="px-4 py-16 text-center text-ink-dim">
                    No {TABS.find((t) => t.key === tab)?.label.toLowerCase()} in this range.
                  </td>
                </tr>
              ) : (
                pageRows.map((r, i) => {
                  const price = r.gross || r.value;
                  const origin = r.path?.[0] || "Direct";
                  const last = r.path && r.path.length >= 2 ? r.path[r.path.length - 2] : origin;
                  const attributed = r.touchpoint_count > 0;
                  return (
                    <tr
                      key={`${r.customer_key}-${r.conversion_ts}-${i}`}
                      className="border-b border-[var(--card-border)]/60 text-[13px] text-ink transition-colors hover:bg-white/[0.02]"
                    >
                      <td className="px-4 py-3">
                        <input type="checkbox" className="accent-brand-500" />
                      </td>
                      <td className="whitespace-nowrap px-4 py-3 tabular text-ink-dim">{fmtDate(r.conversion_ts)}</td>
                      <td className="whitespace-nowrap px-4 py-3 font-medium">{titleCase(r.conversion_type) || "Conversion"}</td>
                      <td className="whitespace-nowrap px-4 py-3 tabular font-medium text-emerald-400">
                        {price ? formatMoney(price) : "—"}
                      </td>
                      <td className="max-w-[200px] truncate px-4 py-3 text-ink-dim" title={origin}>{origin}</td>
                      <td className="max-w-[200px] truncate px-4 py-3 text-ink-dim" title={last}>{last}</td>
                      <td className="whitespace-nowrap px-4 py-3 tabular text-ink-dim">{r.customer_key_short}</td>
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
                      <td className="whitespace-nowrap px-4 py-3 text-ink-dim">
                        {r.touchpoint_count} touch{r.touchpoint_count === 1 ? "" : "es"}
                        {r.time_to_convert ? ` · ${r.time_to_convert}` : ""}
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
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
                    n === page
                      ? "border border-white/15 bg-white/5 font-medium text-ink-bright"
                      : "text-ink-dim hover:text-ink"
                  }`}
                >
                  {n}
                </button>
              </span>
            );
          })}
          <PagerBtn disabled={page === totalPages} onClick={() => setPage((p) => Math.min(totalPages, p + 1))}><ChevronRight size={15} /></PagerBtn>
          <PagerBtn disabled={page === totalPages} onClick={() => setPage(totalPages)}><ChevronsRight size={15} /></PagerBtn>
          <span className="ml-3 text-ink-dim">
            Showing {filtered.length === 0 ? 0 : (page - 1) * PAGE_SIZE + 1}–
            {Math.min(page * PAGE_SIZE, filtered.length)} of {filtered.length} results
          </span>
        </div>
      </div>
    </div>
  );
}

function Chip({ label, value, active, onClick }: { label: string; value: number; active: boolean; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      className={`rounded-lg px-4 py-1.5 text-[13px] transition-colors ${
        active ? "bg-white/[0.06] text-ink-bright" : "text-ink-dim hover:text-ink"
      }`}
    >
      {label} <span className="ml-1.5 font-semibold text-ink-bright">{value}</span>
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
