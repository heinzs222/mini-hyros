"use client";

import { useEffect, useMemo, useState } from "react";
import {
  X,
  Map as MapIcon,
  Info,
  CircleDollarSign,
  MousePointerClick,
  Target,
  Tags,
  Flag,
  Plus,
  RefreshCw,
  Copy,
  Check,
  Search,
  Download,
  ChevronRight,
  ChevronDown,
  MoreVertical,
  Link2,
  ArrowUpRight,
  type LucideIcon,
} from "lucide-react";
import { fetchCustomerJourney } from "@/lib/api";
import { formatMoney } from "@/lib/utils";
import PlatformBadge, { isKnownPlatform } from "@/components/PlatformBadge";

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

type TabKey = "journey" | "info" | "purchases" | "clicks" | "conversions" | "properties";

const TABS: { key: TabKey; label: string; icon: React.ReactNode }[] = [
  { key: "journey", label: "Journey", icon: <MapIcon size={14} /> },
  { key: "info", label: "Info", icon: <Info size={14} /> },
  { key: "purchases", label: "Purchases", icon: <CircleDollarSign size={14} /> },
  { key: "clicks", label: "Clicks", icon: <MousePointerClick size={14} /> },
  { key: "conversions", label: "Conversions", icon: <Target size={14} /> },
  { key: "properties", label: "Properties", icon: <Tags size={14} /> },
];

/* ────────────────────────────── small pure helpers ────────────────────────────── */

function fmtDateTime(iso: string): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso.slice(0, 19).replace("T", " ");
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

/** Title-case a raw source string; empty input reads as "Direct" (no attribution). */
function titleCaseOrDirect(raw: string): string {
  const cleaned = String(raw || "").replace(/[_/|]+/g, " ").replace(/\s+/g, " ").trim();
  if (!cleaned) return "Direct";
  return cleaned
    .split(" ")
    .map((w) => (w ? w[0].toUpperCase() + w.slice(1) : w))
    .join(" ");
}

/** Lowercase, hyphenated slug used for `!tag` / `@tag` pills. */
function slugify(raw: string): string {
  const s = String(raw || "")
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return s || "unknown";
}

/** First two alphanumeric characters of the lead's name/email, uppercased. */
function initialsOf(label?: string, email?: string): string {
  const source = (label && label.trim()) || (email && email.trim()) || "Customer";
  const alnum = source.replace(/[^a-zA-Z0-9]/g, "");
  return (alnum.slice(0, 2) || "CU").toUpperCase();
}

function downloadCsv(filename: string, headers: string[], rows: (string | number)[][]) {
  const csv = [headers, ...rows]
    .map((cols) => cols.map((c) => `"${String(c).replace(/"/g, '""')}"`).join(","))
    .join("\n");
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

/* ────────────────────────────── event → node/title/tags ────────────────────────────── */

type NodeTone = { bg: string; border: string; color: string };
const TONE_CYAN: NodeTone = { bg: "rgba(34,211,238,.13)", border: "rgba(34,211,238,.28)", color: "#22d3ee" };
const TONE_AMBER: NodeTone = { bg: "rgba(234,179,8,.13)", border: "rgba(234,179,8,.28)", color: "#eab308" };
const TONE_VIOLET: NodeTone = { bg: "rgba(139,92,246,.15)", border: "rgba(139,92,246,.30)", color: "#a78bfa" };
const TONE_MINT: NodeTone = { bg: "rgba(62,224,161,.13)", border: "rgba(62,224,161,.28)", color: "#3ee0a1" };

interface EventMeta {
  tone: NodeTone;
  Icon: LucideIcon;
  title: string;
  /** Lowercased blob of everything searchable about this event (title/tag/amount/etc). */
  searchBlob: string;
  /** Plain-text detail used for the Journey CSV export. */
  csvDetail: string;
  renderTags: () => React.ReactNode;
}

function buildEventMeta(ev: TimelineEvent): EventMeta {
  const d = ev.details || {};

  if (ev.type === "session") {
    const eventName = String(d.event_name || "");
    const isOptIn = /opt.?in|form/i.test(eventName);
    const landingPage = String(d.landing_page || "");
    const title = isOptIn ? "Opted in" : landingPage ? `Visited ${landingPage}` : eventName || "Session";
    const url = landingPage || String(d.referrer || "");
    return {
      tone: TONE_CYAN,
      Icon: Plus,
      title,
      searchBlob: `${title} ${url}`.toLowerCase(),
      csvDetail: url,
      renderTags: () =>
        url ? (
          <span className="inline-flex max-w-[280px] items-center gap-[7px] truncate rounded-[8px] border border-[#262631] bg-[#15151c] px-[10px] py-[5px] font-mono text-[11.5px] text-[#b9bcc8]">
            <Link2 size={11} className="shrink-0 text-[#6d7180]" />
            <span className="truncate">{url}</span>
          </span>
        ) : null,
    };
  }

  if (ev.type === "conversion") {
    const ctype = String(d.conversion_type || "conversion");
    const tag = `!${slugify(ctype)}`;
    const title = `Achieved ${ctype}`;
    const value = Number(d.value || 0);
    return {
      tone: TONE_AMBER,
      Icon: Flag,
      title,
      searchBlob: `${title} ${tag} ${value > 0 ? formatMoney(value) : ""}`.toLowerCase(),
      csvDetail: tag,
      renderTags: () => (
        <span className="rounded-[8px] border border-[rgba(234,179,8,.26)] bg-[rgba(234,179,8,.12)] px-[10px] py-[5px] font-mono text-[11.5px] font-semibold text-[#e5c158]">
          {tag}
        </span>
      ),
    };
  }

  if (ev.type === "touchpoint") {
    const platform = String(d.platform || "");
    const channel = String(d.channel || "");
    const title = `Clicked ${titleCaseOrDirect(platform || channel)}`;
    const adId = String(d.ad_id || "");
    const groupSource = String(d.campaign_id || d.adset_id || "");
    const groupTag = groupSource ? `@${slugify(groupSource)}` : "";
    const known = isKnownPlatform(platform);
    return {
      tone: TONE_VIOLET,
      Icon: MousePointerClick,
      title,
      searchBlob: `${title} ${adId} ${groupTag} ${channel} ${platform}`.toLowerCase(),
      csvDetail: [adId ? `Ad Id: ${adId}` : "", groupTag].filter(Boolean).join(" "),
      renderTags: () => (
        <>
          {adId && (
            <span className="rounded-[8px] border border-[#262631] bg-[#15151c] px-[9px] py-[5px] font-mono text-[11px] text-[#8b8f9c]">
              Ad Id: {adId}
            </span>
          )}
          {known && <PlatformBadge platform={platform} showLabel={false} size={26} />}
          {groupTag && (
            <span className="rounded-[8px] border border-[rgba(139,92,246,.28)] bg-[rgba(139,92,246,.13)] px-[10px] py-[5px] font-mono text-[11.5px] font-semibold text-[#b9a3ef]">
              {groupTag}
            </span>
          )}
        </>
      ),
    };
  }

  // order
  const gross = Number(d.gross || 0);
  const rounded = Math.round(gross);
  const title = `Purchased Stripe ${rounded} for ${formatMoney(gross)}`;
  const purchaseTag = `$stripe-${rounded}`;
  return {
    tone: TONE_MINT,
    Icon: CircleDollarSign,
    title,
    searchBlob: `${title} ${purchaseTag}`.toLowerCase(),
    csvDetail: purchaseTag,
    renderTags: () => (
      <>
        <span className="inline-flex items-center rounded-[6px] bg-[#635bff] px-2 py-1 text-[10px] font-bold tracking-[.02em] text-white">
          stripe
        </span>
        <span className="rounded-[8px] border border-[rgba(62,224,161,.26)] bg-[rgba(62,224,161,.13)] px-[10px] py-[5px] font-mono text-[11.5px] font-semibold text-[#3ee0a1]">
          {purchaseTag}
        </span>
      </>
    ),
  };
}

/** "Tracked" per the Clicks tab: any click-id flag present, or a touchpoint with an ad id. */
function isTrackedEvent(ev: TimelineEvent): boolean {
  const d = ev.details || {};
  if (d.has_gclid || d.has_fbclid || d.has_ttclid) return true;
  if (ev.type === "touchpoint" && d.ad_id) return true;
  return false;
}

function clickLabel(ev: TimelineEvent): string {
  const d = ev.details || {};
  if (ev.type === "session") return String(d.landing_page || d.referrer || "—");
  return String(d.campaign_id || d.adset_id || d.channel || d.platform || "—");
}

/* ────────────────────────────── properties (derived from real session data) ────────────────────────────── */

type PropertyRow = { key: string; value: string };

const UTM_KEYS = ["utm_source", "utm_medium", "utm_campaign", "utm_content"];
const SESSION_KEYS = ["landing_page", "device", "referrer"];

function buildProperties(timelineAsc: TimelineEvent[]): PropertyRow[] {
  const latest: Record<string, string> = {};
  const clickIdsSeen = new Set<string>();

  for (const ev of timelineAsc) {
    const d = ev.details || {};
    if (ev.type === "session" || ev.type === "touchpoint") {
      if (d.has_gclid) clickIdsSeen.add("gclid");
      if (d.has_fbclid) clickIdsSeen.add("fbclid");
      if (d.has_ttclid) clickIdsSeen.add("ttclid");
    }
    if (ev.type !== "session") continue;
    for (const k of [...UTM_KEYS, ...SESSION_KEYS]) {
      const v = d[k];
      if (v !== undefined && v !== null && String(v).trim() !== "") latest[k] = String(v);
    }
    if (d.custom_data && typeof d.custom_data === "object") {
      for (const [ck, cv] of Object.entries(d.custom_data as Record<string, unknown>)) {
        if (cv === undefined || cv === null || String(cv).trim() === "") continue;
        latest[ck] = typeof cv === "object" ? JSON.stringify(cv) : String(cv);
      }
    }
  }

  const rows: PropertyRow[] = [];
  for (const k of [...UTM_KEYS, ...SESSION_KEYS]) {
    if (latest[k]) rows.push({ key: k, value: latest[k] });
  }
  for (const [k, v] of Object.entries(latest)) {
    if (UTM_KEYS.includes(k) || SESSION_KEYS.includes(k)) continue;
    rows.push({ key: k, value: v });
  }
  if (clickIdsSeen.size > 0) {
    rows.push({ key: "click_ids", value: Array.from(clickIdsSeen).join(", ") });
  }
  return rows;
}

/* ────────────────────────────── modal ────────────────────────────── */

export default function CustomerProfileModal({ customerKey, label, email, onClose }: Props) {
  const [data, setData] = useState<Journey | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [tab, setTab] = useState<TabKey>("journey");
  const [copied, setCopied] = useState(false);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [onClose]);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetchCustomerJourney(customerKey)
      .then((res) => {
        if (!cancelled) setData(res);
      })
      .catch((e) => {
        if (!cancelled) setError(e?.message || "Failed to load customer");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [customerKey]);

  const timeline = useMemo(() => {
    const t = [...(data?.timeline || [])];
    t.sort((a, b) => (a.ts < b.ts ? 1 : -1)); // newest first, like Hyros
    return t;
  }, [data]);

  const timelineAsc = useMemo(() => {
    const t = [...(data?.timeline || [])];
    t.sort((a, b) => (a.ts < b.ts ? -1 : 1));
    return t;
  }, [data]);

  const purchases = useMemo(() => timeline.filter((e) => e.type === "order"), [timeline]);
  const clicks = useMemo(() => timeline.filter((e) => e.type === "touchpoint" || e.type === "session"), [timeline]);
  const conversions = useMemo(() => timeline.filter((e) => e.type === "conversion"), [timeline]);
  const properties = useMemo(() => buildProperties(timelineAsc), [timelineAsc]);

  const summary = data?.summary;
  const initials = initialsOf(label, email);

  const handleCopy = async () => {
    const text = email || customerKey;
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch {
      // clipboard unavailable in this context — fail silently
    }
  };

  const counts: Record<TabKey, number> = {
    journey: 0,
    info: 0,
    purchases: purchases.length,
    clicks: clicks.length,
    conversions: conversions.length,
    properties: properties.length,
  };

  return (
    <div
      className="fixed inset-0 z-[60] flex items-start justify-center overflow-y-auto px-5 pb-10 pt-[5vh]"
      style={{ background: "rgba(4,4,7,.72)", backdropFilter: "blur(4px)" }}
      onClick={onClose}
    >
      <div
        className="animate-hpop w-[920px] max-w-[96vw] overflow-hidden rounded-[20px] border border-[#24242f] bg-[#0f0f14] shadow-[0_40px_100px_-30px_rgba(0,0,0,.9)]"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-start justify-between gap-4 border-b border-[#1e1e28] px-[22px] pb-[18px] pt-5">
          <div className="flex min-w-0 items-center gap-[14px]">
            <span
              className="flex h-[46px] w-[46px] shrink-0 items-center justify-center rounded-[13px] text-[16px] font-extrabold text-white"
              style={{ background: "linear-gradient(135deg,#8b5cf6,#6366f1)" }}
            >
              {initials}
            </span>
            <div className="min-w-0">
              <div className="text-[10.5px] font-medium uppercase tracking-[.12em] text-[#7e828f]">Lead details</div>
              <div className="mt-0.5 truncate text-[18px] font-bold tracking-[-.01em] text-[#f0f1f6]">
                {label || "Customer"}
              </div>
              <div className="mt-0.5 flex items-center gap-[7px]">
                <span className="truncate font-mono text-[13px] font-medium text-[#a2a6b2]">
                  {email || `${customerKey.slice(0, 18)}…`}
                </span>
                <button
                  onClick={handleCopy}
                  aria-label="Copy"
                  title={copied ? "Copied" : "Copy"}
                  className="shrink-0 text-[#565a67] transition-colors hover:text-[#a2a6b2]"
                >
                  {copied ? <Check size={13} className="text-[#3ee0a1]" /> : <Copy size={13} />}
                </button>
              </div>
            </div>
          </div>
          <div className="flex shrink-0 items-center gap-2">
            <button
              disabled
              title="Coming soon"
              className="inline-flex h-8 cursor-not-allowed items-center gap-[7px] rounded-[9px] border border-[#262631] bg-[#17171f] px-[13px] text-[12px] font-semibold text-[#c4c7d2] opacity-60"
            >
              <Plus size={12} className="text-[#8b8f9c]" /> Assign parent
            </button>
            <button
              onClick={onClose}
              aria-label="Close"
              className="flex h-8 w-8 items-center justify-center rounded-[9px] border border-[#262631] bg-[#17171f] text-[#9aa0ad] transition-colors hover:text-[#eceef4]"
            >
              <X size={15} />
            </button>
          </div>
        </div>

        {/* Summary strip */}
        {summary && (
          <div className="flex gap-px border-b border-[#1e1e28] bg-[#1e1e28]">
            {[
              { label: "Revenue", value: formatMoney(summary.total_revenue), color: "#3ee0a1" },
              { label: "Orders", value: String(summary.total_orders), color: "#eceef4" },
              { label: "Touchpoints", value: String(summary.total_touchpoints), color: "#eceef4" },
              { label: "Time to convert", value: summary.time_to_convert || "—", color: "#eceef4" },
            ].map((s) => (
              <div key={s.label} className="flex-1 bg-[#0f0f14] px-[18px] py-[14px]">
                <div className="text-[10px] font-semibold uppercase tracking-[.1em] text-[#7e828f]">{s.label}</div>
                <div className="mt-1 text-[17px] font-bold tabular-nums" style={{ color: s.color }}>
                  {s.value}
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Tab bar */}
        <div className="flex items-center gap-0.5 overflow-x-auto border-b border-[#1e1e28] px-[14px]">
          {TABS.map((t) => {
            const active = tab === t.key;
            const count = counts[t.key];
            return (
              <button
                key={t.key}
                onClick={() => setTab(t.key)}
                className="-mb-px inline-flex items-center gap-[7px] whitespace-nowrap border-b-2 px-[12px] py-[14px] text-[13px] font-semibold transition-colors"
                style={{ borderColor: active ? "#8b5cf6" : "transparent", color: active ? "#f0f1f6" : "#797d8a" }}
              >
                {t.label}
                {count > 0 && (
                  <span
                    className="rounded-[6px] px-[6px] py-[2px] font-mono text-[10.5px] font-bold tabular-nums"
                    style={{
                      color: active ? "#c4b5fd" : "#7e828f",
                      background: active ? "rgba(139,92,246,.16)" : "rgba(255,255,255,.05)",
                    }}
                  >
                    {count}
                  </span>
                )}
              </button>
            );
          })}
        </div>

        {/* Body */}
        <div className="max-h-[56vh] overflow-y-auto px-[22px] pb-[22px] pt-[18px]">
          {loading ? (
            <div className="py-16 text-center text-ink-dim">
              <RefreshCw size={20} className="mx-auto animate-spin text-brand-500" />
            </div>
          ) : error ? (
            <div className="py-16 text-center text-rose-400">{error}</div>
          ) : tab === "journey" ? (
            <JourneyTab events={timeline} customerKey={customerKey} />
          ) : tab === "info" ? (
            <InfoTab data={data} email={email} customerKey={customerKey} />
          ) : tab === "purchases" ? (
            <PurchasesTab events={purchases} customerKey={customerKey} />
          ) : tab === "clicks" ? (
            <ClicksTab events={clicks} customerKey={customerKey} />
          ) : tab === "conversions" ? (
            <ConversionsTab events={conversions} />
          ) : (
            <PropertiesTab rows={properties} />
          )}
        </div>
      </div>
    </div>
  );
}

/* ────────────────────────────── shared bits ────────────────────────────── */

function EmptyState({ text }: { text: string }) {
  return <div className="py-16 text-center text-ink-dim">{text}</div>;
}

function ToolbarSearch({
  value,
  onChange,
  placeholder,
  width = 190,
}: {
  value: string;
  onChange: (v: string) => void;
  placeholder: string;
  width?: number;
}) {
  return (
    <label className="flex h-[34px] items-center gap-2 rounded-[9px] border border-[#24242e] bg-[#15151c] px-3">
      <Search size={12} className="shrink-0 text-[#797d8a]" />
      <input
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        style={{ width }}
        className="bg-transparent text-[12px] text-[#c4c7d2] outline-none placeholder:text-[#797d8a]"
      />
    </label>
  );
}

function ExportChip({ onClick, label }: { onClick: () => void; label: string }) {
  return (
    <button
      onClick={onClick}
      className="inline-flex h-[34px] items-center gap-[7px] whitespace-nowrap rounded-[9px] border border-[#262631] bg-[#17171f] px-[13px] text-[12px] font-semibold text-[#c4c7d2] transition-colors hover:text-[#eceef4]"
    >
      <Download size={12} className="text-[#8b8f9c]" /> {label}
    </button>
  );
}

/* ────────────────────────────── Journey tab ────────────────────────────── */

function JourneyTab({ events, customerKey }: { events: TimelineEvent[]; customerKey: string }) {
  const [search, setSearch] = useState("");

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return events;
    return events.filter((ev) => {
      const meta = buildEventMeta(ev);
      return meta.title.toLowerCase().includes(q) || fmtDateTime(ev.ts).toLowerCase().includes(q) || meta.searchBlob.includes(q);
    });
  }, [events, search]);

  const handleExport = () => {
    const rows = filtered.map((ev) => {
      const meta = buildEventMeta(ev);
      return [fmtDateTime(ev.ts), ev.type, meta.title, meta.csvDetail];
    });
    downloadCsv(`journey-${customerKey.slice(0, 16)}.csv`, ["Timestamp", "Type", "Title", "Detail"], rows);
  };

  return (
    <div>
      <div className="mb-[18px] flex flex-wrap items-center justify-between gap-3">
        <ToolbarSearch value={search} onChange={setSearch} placeholder="Date, tag, type, income" width={200} />
        <ExportChip onClick={handleExport} label="Export Journey" />
      </div>
      {filtered.length === 0 ? (
        <EmptyState text="No journey events recorded for this customer." />
      ) : (
        <ol className="m-0 list-none p-0">
          {filtered.map((ev, i) => {
            const meta = buildEventMeta(ev);
            const isLast = i === filtered.length - 1;
            return (
              <li key={`${ev.type}-${ev.ts}-${i}`} className="relative flex gap-[15px] pb-[18px]">
                {!isLast && <span className="absolute bottom-[-2px] left-[16px] top-[38px] w-[2px] bg-[#22232e]" />}
                <span
                  className="relative z-[1] flex h-[34px] w-[34px] shrink-0 items-center justify-center rounded-[10px] border"
                  style={{ background: meta.tone.bg, borderColor: meta.tone.border, color: meta.tone.color }}
                >
                  <meta.Icon size={15} />
                </span>
                <div className="flex min-w-0 flex-1 items-center justify-between gap-3 rounded-[12px] border border-[#22222c] bg-[#14141b] px-[14px] py-[12px]">
                  <div className="min-w-0">
                    <div className="truncate text-[13.5px] font-semibold text-[#e4e6ee]">{meta.title}</div>
                    <div className="mt-0.5 font-mono text-[11.5px] font-medium tabular-nums text-[#6d7180]">
                      {fmtDateTime(ev.ts)}
                    </div>
                  </div>
                  <div className="flex shrink-0 items-center gap-2">{meta.renderTags()}</div>
                </div>
              </li>
            );
          })}
        </ol>
      )}
    </div>
  );
}

/* ────────────────────────────── Info tab ────────────────────────────── */

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
    <div>
      <div className="overflow-hidden rounded-[12px] border border-[#22222c]">
        {rows.map((r, i) => (
          <div
            key={r.label}
            className={`flex items-center justify-between gap-4 px-[15px] py-3 ${i % 2 ? "bg-[#101017]" : "bg-[#14141b]"} ${
              i < rows.length - 1 ? "border-b border-[#1c1c25]" : ""
            }`}
          >
            <span className="text-[11px] font-semibold uppercase tracking-[.06em] text-[#7e828f]">{r.label}</span>
            <span className="max-w-[62%] truncate text-right font-mono text-[12.5px] text-[#d0d3dc]">{r.value}</span>
          </div>
        ))}
      </div>
      <div className="mt-3 text-[11px] text-ink-faint">
        <ArrowUpRight size={11} className="mr-1 inline" />
        Identity is stitched across Stripe, the tracking pixel and GoHighLevel by hashed email.
      </div>
    </div>
  );
}

/* ────────────────────────────── Purchases tab ────────────────────────────── */

function PurchasesTab({ events, customerKey }: { events: TimelineEvent[]; customerKey: string }) {
  const [search, setSearch] = useState("");
  const [openKeys, setOpenKeys] = useState<Set<string>>(new Set());

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return events;
    return events.filter((ev) => {
      const gross = Number(ev.details?.gross || 0);
      const label = `stripe ${Math.round(gross)}`;
      return label.includes(q) || String(ev.details?.order_id || "").toLowerCase().includes(q);
    });
  }, [events, search]);

  const togglePurchase = (key: string) => {
    setOpenKeys((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  const handleExport = () => {
    const rows = filtered.map((ev) => {
      const d = ev.details || {};
      return [
        fmtDateTime(ev.ts),
        String(d.order_id || ""),
        formatMoney(Number(d.gross || 0)),
        formatMoney(Number(d.net || 0)),
        formatMoney(Number(d.refunds || 0)),
      ];
    });
    downloadCsv(
      `purchases-${customerKey.slice(0, 16)}.csv`,
      ["Timestamp", "Order Id", "Gross", "Net", "Refunds"],
      rows
    );
  };

  return (
    <div>
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <ToolbarSearch value={search} onChange={setSearch} placeholder="Search by product" width={170} />
        <div className="flex items-center gap-2">
          <ExportChip onClick={handleExport} label="Export Purchases" />
          <button
            disabled
            title="Coming soon"
            className="inline-flex h-[34px] cursor-not-allowed items-center gap-[6px] rounded-[9px] bg-[#8b5cf6] px-[13px] text-[12px] font-bold text-white opacity-60"
          >
            <Plus size={12} /> Add sale
          </button>
        </div>
      </div>
      {filtered.length === 0 ? (
        <EmptyState text="No purchases recorded for this customer." />
      ) : (
        <div className="overflow-hidden rounded-[12px] border border-[#22222c]">
          {filtered.map((ev, i) => {
            const d = ev.details || {};
            const gross = Number(d.gross || 0);
            const rounded = Math.round(gross);
            const key = `${d.order_id || "order"}-${ev.ts}`;
            const isOpen = openKeys.has(key);
            const isLastRow = i === filtered.length - 1;
            return (
              <div key={key} className={!isLastRow ? "border-b border-[#1c1c25]" : ""}>
                <button
                  onClick={() => togglePurchase(key)}
                  className={`flex w-full items-center justify-between gap-3 bg-[#14141b] px-[15px] py-[14px] text-left ${
                    isOpen ? "border-b border-[#1c1c25]" : ""
                  }`}
                >
                  <span className="flex items-center gap-[11px]">
                    <ChevronRight
                      size={14}
                      className={`text-[#8b8f9c] transition-transform duration-150 ${isOpen ? "rotate-90" : ""}`}
                    />
                    <span className="text-[13.5px] font-bold text-[#e4e6ee]">Stripe {rounded}</span>
                  </span>
                  <span className="flex items-center gap-3">
                    <span className="inline-flex items-center rounded-[6px] bg-[#635bff] px-2 py-1 text-[10px] font-bold tracking-[.02em] text-white">
                      stripe
                    </span>
                    <span className="text-[13.5px] font-bold tabular-nums text-[#3ee0a1]">{formatMoney(gross)}</span>
                  </span>
                </button>
                {isOpen && (
                  <div className="flex items-center justify-between gap-3 bg-[#0f0f14] py-[13px] pl-10 pr-[15px]">
                    <span className="font-mono text-[12.5px] font-medium tabular-nums text-[#8b8f9c]">
                      {fmtDateTime(ev.ts)}
                    </span>
                    <span className="flex items-center gap-[14px]">
                      <span className="text-[13px] font-semibold tabular-nums text-[#d0d3dc]">{formatMoney(gross)}</span>
                      <MoreVertical size={15} className="text-[#565a67]" />
                    </span>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

/* ────────────────────────────── Clicks tab ────────────────────────────── */

function ClicksTab({ events, customerKey }: { events: TimelineEvent[]; customerKey: string }) {
  const [trackedOnly, setTrackedOnly] = useState(false);

  const filtered = useMemo(() => (trackedOnly ? events.filter(isTrackedEvent) : events), [events, trackedOnly]);

  const handleExport = () => {
    const rows = filtered.map((ev) => [fmtDateTime(ev.ts), ev.type, clickLabel(ev), isTrackedEvent(ev) ? "Yes" : "No"]);
    downloadCsv(`clicks-${customerKey.slice(0, 16)}.csv`, ["Timestamp", "Type", "URL / Label", "Tracked"], rows);
  };

  return (
    <div>
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <button
          onClick={() => setTrackedOnly((v) => !v)}
          className="inline-flex items-center gap-[9px] text-[12.5px] font-medium text-[#b9bcc8]"
        >
          Show only tracked URL
          <span
            className="relative inline-block h-5 w-9 rounded-full transition-colors"
            style={{ background: trackedOnly ? "#8b5cf6" : "rgba(255,255,255,.14)" }}
          >
            <span
              className="absolute top-[3px] h-3.5 w-3.5 rounded-full bg-white transition-all"
              style={{ left: trackedOnly ? "19px" : "3px" }}
            />
          </span>
        </button>
        <ExportChip onClick={handleExport} label="Export Clicks" />
      </div>
      {filtered.length === 0 ? (
        <EmptyState text="No clicks recorded for this customer." />
      ) : (
        <div className="flex flex-col gap-2">
          {filtered.map((ev, i) => {
            const d = ev.details || {};
            const tracked = isTrackedEvent(ev);
            const label = clickLabel(ev);
            const known = ev.type === "touchpoint" && isKnownPlatform(String(d.platform || ""));
            return (
              <div
                key={`${ev.type}-${ev.ts}-${i}`}
                className="flex items-center gap-3 rounded-[11px] border border-[#22222c] bg-[#14141b] px-[14px] py-[11px]"
              >
                <ChevronDown size={12} className="shrink-0 text-[#6d7180]" />
                <span className="shrink-0 whitespace-nowrap font-mono text-[12px] font-semibold tabular-nums text-[#8b8f9c]">
                  {fmtDateTime(ev.ts)}
                </span>
                <span className="min-w-0 flex-1 truncate rounded-[8px] border border-[#22222c] bg-[#101017] px-[10px] py-[5px] font-mono text-[11.5px] text-[#b9bcc8]">
                  {label}
                </span>
                {tracked && (
                  <>
                    {known && <PlatformBadge platform={String(d.platform || "")} showLabel={false} size={24} />}
                    <span className="inline-flex shrink-0 items-center gap-[5px] text-[11px] font-bold text-[#3ee0a1]">
                      <Check size={12} /> Tracked
                    </span>
                  </>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

/* ────────────────────────────── Conversions tab ────────────────────────────── */

function ConversionsTab({ events }: { events: TimelineEvent[] }) {
  if (events.length === 0) return <EmptyState text="No conversions recorded for this customer." />;
  return (
    <div className="flex flex-col gap-2">
      {events.map((ev, i) => {
        const d = ev.details || {};
        const ctype = String(d.conversion_type || "conversion");
        const tag = `!${slugify(ctype)}`;
        return (
          <div
            key={`${ev.ts}-${i}`}
            className="flex items-center gap-[13px] rounded-[12px] border border-[#22222c] bg-[#14141b] px-[15px] py-3"
          >
            <span
              className="flex h-[34px] w-[34px] shrink-0 items-center justify-center rounded-[10px]"
              style={{ background: "rgba(234,179,8,.12)", border: "1px solid rgba(234,179,8,.26)", color: "#eab308" }}
            >
              <Flag size={15} />
            </span>
            <div className="min-w-0 flex-1">
              <div className="truncate text-[13.5px] font-semibold text-[#e4e6ee]">Achieved {ctype}</div>
              <div className="mt-0.5 font-mono text-[11.5px] font-medium tabular-nums text-[#6d7180]">
                {fmtDateTime(ev.ts)}
              </div>
            </div>
            <span className="shrink-0 rounded-[8px] border border-[rgba(234,179,8,.26)] bg-[rgba(234,179,8,.12)] px-[10px] py-[5px] font-mono text-[11.5px] font-semibold text-[#e5c158]">
              {tag}
            </span>
          </div>
        );
      })}
    </div>
  );
}

/* ────────────────────────────── Properties tab ────────────────────────────── */

function PropertiesTab({ rows }: { rows: PropertyRow[] }) {
  if (rows.length === 0) return <EmptyState text="No properties recorded for this customer." />;
  return (
    <div className="overflow-hidden rounded-[12px] border border-[#22222c]">
      {rows.map((r, i) => (
        <div
          key={r.key}
          className={`flex items-center justify-between gap-4 px-[15px] py-3 ${i % 2 ? "bg-[#101017]" : "bg-[#14141b]"} ${
            i < rows.length - 1 ? "border-b border-[#1c1c25]" : ""
          }`}
        >
          <span className="font-mono text-[11.5px] font-semibold text-[#a78bfa]">{r.key}</span>
          <span className="max-w-[64%] truncate text-right font-mono text-[12.5px] text-[#d0d3dc]">{r.value}</span>
        </div>
      ))}
    </div>
  );
}
