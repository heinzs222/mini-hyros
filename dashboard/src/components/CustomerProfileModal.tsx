"use client";

import { useEffect, useMemo, useState } from "react";
import {
  X,
  Map as MapIcon,
  Info,
  CircleDollarSign,
  MousePointerClick,
  Target,
  Globe,
  Flag,
  RefreshCw,
  ArrowUpRight,
} from "lucide-react";
import { fetchCustomerJourney } from "@/lib/api";
import { formatMoney } from "@/lib/utils";

interface Props {
  customerKey: string;
  label?: string;
  email?: string;
  onClose: () => void;
}

type TimelineEvent = {
  type: "session" | "touchpoint" | "conversion" | "order";
  ts: string;
  details: Record<string, any>;
};

type Journey = {
  customer_key: string;
  summary: {
    total_sessions: number;
    total_touchpoints: number;
    total_orders: number;
    total_conversions: number;
    total_revenue: number;
    first_touch: string;
    first_order: string;
    time_to_convert: string;
  };
  timeline: TimelineEvent[];
};

type TabKey = "journey" | "info" | "purchases" | "clicks" | "conversions";

const TABS: { key: TabKey; label: string; icon: React.ReactNode }[] = [
  { key: "journey", label: "Journey", icon: <MapIcon size={14} /> },
  { key: "info", label: "Info", icon: <Info size={14} /> },
  { key: "purchases", label: "Purchases", icon: <CircleDollarSign size={14} /> },
  { key: "clicks", label: "Clicks", icon: <MousePointerClick size={14} /> },
  { key: "conversions", label: "Conversions", icon: <Target size={14} /> },
];

function fmtDateTime(iso: string): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso.slice(0, 19).replace("T", " ");
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function eventMeta(ev: TimelineEvent): { icon: React.ReactNode; color: string; title: string; tag: string } {
  const d = ev.details || {};
  switch (ev.type) {
    case "order":
      return {
        icon: <CircleDollarSign size={14} />,
        color: "#22c55e",
        title: `Purchased ${formatMoney(Number(d.gross || 0))}`,
        tag: d.order_id ? `#${String(d.order_id).slice(0, 12)}` : "order",
      };
    case "conversion":
      return {
        icon: <Flag size={14} />,
        color: "#eab308",
        title: `Achieved ${String(d.conversion_type || "Conversion")}`,
        tag: Number(d.value) > 0 ? formatMoney(Number(d.value)) : String(d.conversion_type || ""),
      };
    case "touchpoint":
      return {
        icon: <MousePointerClick size={14} />,
        color: "#8b5cf6",
        title: `${d.platform || d.channel || "Direct"} click`,
        tag: d.campaign_id ? String(d.campaign_id).slice(0, 16) : (d.channel || "click"),
      };
    default:
      return {
        icon: <Globe size={14} />,
        color: "#22d3ee",
        title: d.landing_page ? `Visited ${d.landing_page}` : (d.event_name || "Session"),
        tag: d.utm_source ? `${d.utm_source}${d.utm_medium ? ` / ${d.utm_medium}` : ""}` : "direct",
      };
  }
}

export default function CustomerProfileModal({ customerKey, label, email, onClose }: Props) {
  const [data, setData] = useState<Journey | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [tab, setTab] = useState<TabKey>("journey");

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [onClose]);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetchCustomerJourney(customerKey)
      .then((res) => { if (!cancelled) setData(res); })
      .catch((e) => { if (!cancelled) setError(e?.message || "Failed to load customer"); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [customerKey]);

  const timeline = useMemo(() => {
    const t = [...(data?.timeline || [])];
    t.sort((a, b) => (a.ts < b.ts ? 1 : -1)); // newest first, like Hyros
    return t;
  }, [data]);

  const purchases = timeline.filter((e) => e.type === "order");
  const clicks = timeline.filter((e) => e.type === "touchpoint" || e.type === "session");
  const conversions = timeline.filter((e) => e.type === "conversion");
  const summary = data?.summary;

  const visible =
    tab === "purchases" ? purchases :
    tab === "clicks" ? clicks :
    tab === "conversions" ? conversions :
    timeline;

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center overflow-y-auto bg-black/70 p-4 backdrop-blur-sm" onClick={onClose}>
      <div
        className="animate-hpop mt-[3vh] w-full max-w-3xl rounded-2xl border border-[var(--card-border)] bg-[#0c0c11] shadow-2xl"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-start justify-between gap-3 border-b border-[var(--card-border)] px-5 py-4">
          <div className="min-w-0">
            <div className="flex items-center gap-2 text-[12px] uppercase tracking-wide text-ink-dim">
              <Info size={13} /> Lead details
            </div>
            <div className="mt-1 truncate text-[18px] font-semibold text-ink-bright">
              {label || "Customer"}
            </div>
            <div className="mt-0.5 truncate font-mono text-[12px] text-ink-faint">
              {email || `${customerKey.slice(0, 18)}…`}
            </div>
          </div>
          <button
            onClick={onClose}
            className="rounded-lg border border-[var(--card-border)] p-1.5 text-ink-dim transition-colors hover:text-ink"
            aria-label="Close"
          >
            <X size={16} />
          </button>
        </div>

        {/* Summary strip */}
        {summary && (
          <div className="grid grid-cols-2 gap-px bg-[var(--card-border)] sm:grid-cols-4">
            {[
              { label: "Revenue", value: formatMoney(summary.total_revenue) },
              { label: "Orders", value: String(summary.total_orders) },
              { label: "Touchpoints", value: String(summary.total_touchpoints) },
              { label: "Time to convert", value: summary.time_to_convert || "—" },
            ].map((s) => (
              <div key={s.label} className="bg-[#0c0c11] px-4 py-3">
                <div className="text-[10px] uppercase tracking-wide text-ink-dim">{s.label}</div>
                <div className="mt-0.5 text-[15px] font-semibold text-ink-bright">{s.value}</div>
              </div>
            ))}
          </div>
        )}

        {/* Tabs */}
        <div className="flex items-center gap-4 overflow-x-auto border-b border-[var(--card-border)] px-5">
          {TABS.map((t) => {
            const active = tab === t.key;
            const count =
              t.key === "purchases" ? purchases.length :
              t.key === "clicks" ? clicks.length :
              t.key === "conversions" ? conversions.length : 0;
            return (
              <button
                key={t.key}
                onClick={() => setTab(t.key)}
                className={`-mb-px flex items-center gap-1.5 whitespace-nowrap border-b-2 py-3 text-[13px] transition-colors ${
                  active ? "border-ink-bright font-medium text-ink-bright" : "border-transparent text-ink-dim hover:text-ink"
                }`}
              >
                {t.icon} {t.label}
                {count > 0 && t.key !== "info" && t.key !== "journey" && (
                  <span className="rounded bg-white/5 px-1 text-[10px] text-ink-dim">{count}</span>
                )}
              </button>
            );
          })}
        </div>

        {/* Body */}
        <div className="max-h-[55vh] overflow-y-auto px-5 py-4">
          {loading ? (
            <div className="py-16 text-center text-ink-dim">
              <RefreshCw size={20} className="mx-auto animate-spin text-brand-500" />
            </div>
          ) : error ? (
            <div className="py-16 text-center text-rose-400">{error}</div>
          ) : tab === "info" ? (
            <InfoTab data={data} email={email} customerKey={customerKey} />
          ) : visible.length === 0 ? (
            <div className="py-16 text-center text-ink-dim">No {tab} recorded for this customer.</div>
          ) : (
            <ol className="relative space-y-2">
              {visible.map((ev, i) => {
                const m = eventMeta(ev);
                return (
                  <li
                    key={`${ev.type}-${ev.ts}-${i}`}
                    className="flex items-center gap-3 rounded-xl border border-[var(--card-border)] bg-white/[0.01] px-3 py-2.5"
                  >
                    <span
                      className="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg"
                      style={{ background: `${m.color}1f`, color: m.color }}
                    >
                      {m.icon}
                    </span>
                    <div className="min-w-0 flex-1">
                      <div className="truncate text-[13px] text-ink">{m.title}</div>
                      <div className="text-[11px] tabular text-ink-faint">{fmtDateTime(ev.ts)}</div>
                    </div>
                    <span className="shrink-0 rounded-md border border-[var(--card-border)] px-2 py-0.5 text-[11px] text-ink-dim">
                      {m.tag}
                    </span>
                  </li>
                );
              })}
            </ol>
          )}
        </div>
      </div>
    </div>
  );
}

function InfoTab({ data, email, customerKey }: { data: Journey | null; email?: string; customerKey: string }) {
  const s = data?.summary;
  const rows: { label: string; value: string }[] = [
    { label: "Email", value: email || "—" },
    { label: "Customer key", value: customerKey },
    { label: "First touch", value: s?.first_touch ? fmtDateTime(s.first_touch) : "—" },
    { label: "First order", value: s?.first_order ? fmtDateTime(s.first_order) : "—" },
    { label: "Sessions", value: String(s?.total_sessions ?? 0) },
    { label: "Conversions", value: String(s?.total_conversions ?? 0) },
    { label: "Lifetime revenue", value: formatMoney(s?.total_revenue ?? 0) },
  ];
  return (
    <div className="divide-y divide-[var(--card-border)]">
      {rows.map((r) => (
        <div key={r.label} className="flex items-center justify-between gap-4 py-2.5">
          <span className="text-[12px] uppercase tracking-wide text-ink-dim">{r.label}</span>
          <span className="max-w-[60%] truncate text-right font-mono text-[12px] text-ink">{r.value}</span>
        </div>
      ))}
      <div className="pt-3 text-[11px] text-ink-faint">
        <ArrowUpRight size={11} className="mr-1 inline" />
        Identity is stitched across Stripe, the tracking pixel and GoHighLevel by hashed email.
      </div>
    </div>
  );
}
