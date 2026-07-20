"use client";

import { Fragment, useEffect, useState } from "react";
import { fetchCommonPaths, fetchCustomerJourney, fetchJourneyStats, fetchLeadJourneys } from "@/lib/api";
import { formatMoney, formatNumber } from "@/lib/utils";
import {
  ArrowRight,
  ChevronDown,
  ChevronRight,
  Clock,
  Info,
  MousePointer,
  Route,
  Search,
  ShoppingCart,
  UserRound,
} from "lucide-react";

const PLATFORM_COLORS: Record<string, string> = {
  meta: "bg-blue-600/20 text-blue-300",
  facebook: "bg-blue-600/20 text-blue-300",
  google: "bg-emerald-600/20 text-emerald-300",
  tiktok: "bg-pink-600/20 text-pink-300",
  email: "bg-yellow-600/20 text-yellow-300",
  organic: "bg-green-600/20 text-green-300",
  direct: "bg-gray-600/20 text-gray-300",
};

type JourneyPanelProps = {
  startDate?: string;
  endDate?: string;
};

function timelineIcon(type: string) {
  if (type === "session") return <MousePointer size={11} />;
  if (type === "conversion") return <ShoppingCart size={11} />;
  if (type === "order") return <ShoppingCart size={11} />;
  if (type === "touchpoint") return <Route size={11} />;
  return <Clock size={11} />;
}

function formatTs(ts: string) {
  try {
    return new Date(ts).toLocaleString();
  } catch {
    return ts;
  }
}

function chipClass(label: string) {
  const lower = String(label || "").toLowerCase();
  const key = Object.keys(PLATFORM_COLORS).find((platform) => lower.includes(platform));
  return key ? PLATFORM_COLORS[key] : "bg-brand-600/10 text-brand-300";
}

function eventTitle(event: any) {
  const details = event?.details || {};
  if (event.type === "touchpoint") {
    return details.platform || details.channel || "Touchpoint";
  }
  if (event.type === "session") {
    return details.event_name || details.page_title || details.landing_page || "Session";
  }
  if (event.type === "conversion") {
    return details.conversion_type || "Conversion";
  }
  if (event.type === "order") {
    return `Order ${details.order_id || ""}`.trim();
  }
  return event.type || "Event";
}

function eventMeta(event: any) {
  const details = event?.details || {};
  if (event.type === "touchpoint") {
    return [
      details.campaign_name || details.campaign_id,
      details.adset_name || details.adset_id,
      details.ad_name || details.ad_id,
    ].filter(Boolean).join(" / ");
  }
  if (event.type === "session") {
    return [details.landing_page, details.device, details.utm_source].filter(Boolean).join(" / ");
  }
  if (event.type === "conversion") {
    return [details.order_id, details.value != null ? formatMoney(Number(details.value)) : ""].filter(Boolean).join(" / ");
  }
  if (event.type === "order") {
    return details.gross != null ? formatMoney(Number(details.gross)) : "";
  }
  return "";
}

export default function JourneyPanel({ startDate = "", endDate = "" }: JourneyPanelProps) {
  const [stats, setStats] = useState<any>(null);
  const [paths, setPaths] = useState<any[]>([]);
  const [leadJourneys, setLeadJourneys] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [searching, setSearching] = useState(false);
  const [customerJourney, setCustomerJourney] = useState<any>(null);
  const [searchError, setSearchError] = useState("");
  const [expandedEvents, setExpandedEvents] = useState<Record<number, boolean>>({});
  const [expandedLeads, setExpandedLeads] = useState<Record<string, boolean>>({});

  useEffect(() => {
    const controller = new AbortController();
    async function load() {
      setLoading(true);
      try {
        const [s, p, leads] = await Promise.all([
          fetchJourneyStats(controller.signal),
          fetchCommonPaths(20, 1, controller.signal),
          fetchLeadJourneys({ start_date: startDate, end_date: endDate, limit: 50 }, controller.signal),
        ]);
        setStats(s);
        setPaths(p.rows || []);
        setLeadJourneys(leads.rows || []);
      } catch (err: any) {
        if (err?.name === "AbortError") return;
        setStats(null);
        setPaths([]);
        setLeadJourneys([]);
      }
      if (!controller.signal.aborted) setLoading(false);
    }
    load();
    return () => controller.abort();
  }, [startDate, endDate]);

  async function handleSearch() {
    if (!search.trim()) return;
    setSearching(true);
    setSearchError("");
    setCustomerJourney(null);
    try {
      const data = await fetchCustomerJourney(search.trim());
      if (!data || data.summary?.total_sessions === 0) {
        setSearchError("No journey data found for this customer key.");
      } else {
        setCustomerJourney(data);
      }
    } catch {
      setSearchError("Customer not found.");
    }
    setSearching(false);
  }

  const hasData = Boolean((stats && stats.total_journeys > 0) || leadJourneys.length || paths.length);

  if (loading) return <div className="text-center py-12 text-gray-500 text-sm">Loading journey data...</div>;

  return (
    <div className="space-y-6">
      {!hasData && (
        <div className="rounded-xl border border-yellow-500/20 bg-yellow-500/5 p-4 text-sm text-yellow-300 flex gap-3">
          <Info size={16} className="flex-shrink-0 mt-0.5" />
          <div>
            <div className="font-medium mb-1">Journey data requires tracking setup</div>
            <ol className="text-yellow-400/70 text-xs space-y-1 list-decimal list-inside">
              <li>Install the tracking pixel on your website from Settings / Tracking.</li>
              <li>Send GHL webhooks to {process.env.NEXT_PUBLIC_API_URL || "https://mini-hyros-api.vercel.app"}/api/webhooks/ghl.</li>
              <li>Leads and purchases will appear here once visitors identify or convert.</li>
            </ol>
          </div>
        </div>
      )}

      <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
        {[
          { label: "Recent Paths", value: leadJourneys.length },
          { label: "Total Journeys", value: stats?.total_journeys || 0 },
          { label: "Avg Touchpoints", value: stats?.avg_touchpoints_before_conversion || 0 },
          { label: "Avg Time", value: stats?.avg_time_to_convert_hours ? `${stats.avg_time_to_convert_hours}h` : "-" },
          { label: "Multi-Touch", value: `${stats?.multi_touch_pct || 0}%` },
        ].map((c, i) => (
          <div key={i} className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-4">
            <div className="text-[11px] text-gray-500 uppercase tracking-wider">{c.label}</div>
            <div className="text-xl font-bold text-white mt-1">{c.value}</div>
          </div>
        ))}
      </div>

      <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] overflow-hidden">
        <div className="px-4 py-3 border-b border-[var(--card-border)] flex items-center justify-between gap-3">
          <div>
            <h3 className="text-sm font-semibold text-white flex items-center gap-2">
              <UserRound size={14} className="text-brand-400" /> Lead Paths
            </h3>
            <p className="text-[11px] text-gray-500 mt-0.5">Recent leads and purchases with the path that came before them.</p>
          </div>
          <span className="text-[11px] px-2 py-1 rounded bg-white/5 text-gray-400">
            {startDate && endDate ? `${startDate} to ${endDate}` : "Latest 50"}
          </span>
        </div>

        {leadJourneys.length === 0 ? (
          <div className="px-4 py-10 text-center text-sm text-gray-600">
            No lead paths in this window.
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-[var(--card-border)] text-gray-500">
                  <th className="text-left px-4 py-2.5 font-medium min-w-[170px]">Lead / Customer</th>
                  <th className="text-left px-3 py-2.5 font-medium min-w-[120px]">Conversion</th>
                  <th className="text-left px-3 py-2.5 font-medium min-w-[360px]">Path</th>
                  <th className="text-right px-3 py-2.5 font-medium">Touches</th>
                  <th className="text-right px-3 py-2.5 font-medium">Value</th>
                  <th className="text-right px-4 py-2.5 font-medium">Time</th>
                </tr>
              </thead>
              <tbody>
                {leadJourneys.map((lead: any) => {
                  const key = `${lead.customer_key}-${lead.conversion_id}`;
                  const expanded = Boolean(expandedLeads[key]);
                  return (
                    <Fragment key={key}>
                      <tr className="border-b border-[var(--card-border)] hover:bg-white/[0.02]">
                        <td className="px-4 py-3">
                          <button
                            onClick={() => setExpandedLeads((prev) => ({ ...prev, [key]: !prev[key] }))}
                            className="flex items-center gap-2 text-left"
                          >
                            <span className="text-gray-500">{expanded ? <ChevronDown size={13} /> : <ChevronRight size={13} />}</span>
                            <span>
                              <span className="block text-gray-200 font-medium font-mono">{lead.customer_key_short || "unknown"}</span>
                              <span className="block text-[10px] text-gray-600">{formatTs(lead.conversion_ts)}</span>
                            </span>
                          </button>
                        </td>
                        <td className="px-3 py-3">
                          <div className="text-gray-200">{lead.conversion_type || "Conversion"}</div>
                          {lead.order_id && <div className="text-[10px] text-gray-600 font-mono truncate max-w-[160px]">{lead.order_id}</div>}
                        </td>
                        <td className="px-3 py-3">
                          <div className="flex items-center gap-1 flex-wrap">
                            {(lead.path || []).map((step: string, i: number) => (
                              <span key={`${key}-${step}-${i}`} className="flex items-center gap-1">
                                <span className={`px-2 py-0.5 rounded text-[11px] font-medium max-w-[220px] truncate ${chipClass(step)}`} title={step}>
                                  {step}
                                </span>
                                {i < (lead.path || []).length - 1 && <ArrowRight size={10} className="text-gray-600" />}
                              </span>
                            ))}
                          </div>
                        </td>
                        <td className="px-3 py-3 text-right text-gray-300">{formatNumber(lead.touchpoint_count || 0)}</td>
                        <td className="px-3 py-3 text-right text-emerald-300">{formatMoney(Number(lead.gross ?? lead.value ?? 0))}</td>
                        <td className="px-4 py-3 text-right text-gray-400">{lead.time_to_convert || "-"}</td>
                      </tr>
                      {expanded && (
                        <tr key={`${key}-details`} className="border-b border-[var(--card-border)] bg-white/[0.015]">
                          <td colSpan={6} className="px-4 py-3">
                            <div className="space-y-2 max-h-80 overflow-y-auto pr-1">
                              {(lead.timeline || []).map((event: any, i: number) => (
                                <div key={`${key}-event-${i}`} className="flex items-start gap-3 rounded-lg border border-[var(--card-border)] bg-black/10 p-2">
                                  <span className={`mt-0.5 flex-shrink-0 ${
                                    event.type === "order" ? "text-emerald-400" :
                                    event.type === "conversion" ? "text-brand-400" :
                                    event.type === "touchpoint" ? "text-blue-300" :
                                    "text-gray-500"
                                  }`}>
                                    {timelineIcon(event.type)}
                                  </span>
                                  <div className="min-w-0 flex-1">
                                    <div className="flex items-center gap-2">
                                      <span className="text-gray-200 font-medium">{eventTitle(event)}</span>
                                      <span className="ml-auto text-[10px] text-gray-600">{formatTs(event.ts)}</span>
                                    </div>
                                    {eventMeta(event) && <div className="mt-0.5 text-[10px] text-gray-500 truncate">{eventMeta(event)}</div>}
                                  </div>
                                </div>
                              ))}
                            </div>
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-4">
        <h3 className="text-sm font-semibold text-white mb-3 flex items-center gap-2">
          <Search size={14} className="text-brand-400" /> Look Up Customer Journey
        </h3>
        <div className="flex gap-2">
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleSearch()}
            placeholder="Paste customer_key hash..."
            className="flex-1 bg-transparent border border-[var(--card-border)] rounded-lg px-3 py-2 text-xs text-gray-300 placeholder-gray-600 focus:outline-none focus:border-brand-500"
          />
          <button
            onClick={handleSearch}
            disabled={searching || !search.trim()}
            className="px-4 py-2 rounded-lg bg-brand-600 hover:bg-brand-700 text-white text-xs font-medium disabled:opacity-50 transition-colors"
          >
            {searching ? "Searching..." : "Search"}
          </button>
        </div>
        {searchError && <div className="mt-2 text-xs text-red-400">{searchError}</div>}

        {customerJourney && (
          <div className="mt-4 space-y-3">
            <div className="grid grid-cols-2 md:grid-cols-4 gap-2 text-xs">
              {[
                { label: "Sessions", value: customerJourney.summary.total_sessions },
                { label: "Touchpoints", value: customerJourney.summary.total_touchpoints },
                { label: "Orders", value: customerJourney.summary.total_orders },
                { label: "Revenue", value: formatMoney(Number(customerJourney.summary.total_revenue || 0)) },
              ].map((s, i) => (
                <div key={i} className="rounded-lg bg-white/[0.03] border border-[var(--card-border)] p-2 text-center">
                  <div className="text-gray-500 text-[10px]">{s.label}</div>
                  <div className="text-white font-semibold">{s.value}</div>
                </div>
              ))}
            </div>
            {customerJourney.summary.time_to_convert && (
              <div className="text-xs text-gray-500">Time to convert: <span className="text-brand-300">{customerJourney.summary.time_to_convert}</span></div>
            )}
            <div className="space-y-1 max-h-64 overflow-y-auto pr-1">
              {customerJourney.timeline.map((event: any, i: number) => (
                <div
                  key={i}
                  className={`flex items-start gap-2 p-2 rounded-lg text-xs cursor-pointer ${
                    event.type === "order" ? "bg-emerald-500/5 border border-emerald-500/20" :
                    event.type === "conversion" ? "bg-brand-500/5 border border-brand-500/20" :
                    "bg-white/[0.02] border border-[var(--card-border)]"
                  }`}
                  onClick={() => setExpandedEvents((prev) => ({ ...prev, [i]: !prev[i] }))}
                >
                  <span className={`mt-0.5 flex-shrink-0 ${event.type === "order" ? "text-emerald-400" : event.type === "conversion" ? "text-brand-400" : "text-gray-500"}`}>
                    {timelineIcon(event.type)}
                  </span>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <span className="font-medium text-gray-300 capitalize">{event.type}</span>
                      {event.type === "session" && event.details.utm_source && (
                        <span className={`px-1.5 py-0.5 rounded text-[10px] ${PLATFORM_COLORS[event.details.utm_source] || "bg-gray-600/20 text-gray-400"}`}>
                          {event.details.utm_source}
                        </span>
                      )}
                      {event.type === "order" && (
                        <span className="text-emerald-400 font-medium">{formatMoney(Number(event.details.gross || 0))}</span>
                      )}
                      <span className="text-gray-600 text-[10px] ml-auto">{formatTs(event.ts)}</span>
                    </div>
                    {expandedEvents[i] && event.type === "session" && (
                      <div className="mt-1 text-[10px] text-gray-500 space-y-0.5">
                        {event.details.utm_campaign && <div>Campaign: {event.details.utm_campaign}</div>}
                        {event.details.landing_page && <div>Landing: {event.details.landing_page}</div>}
                        {event.details.device && <div>Device: {event.details.device}</div>}
                      </div>
                    )}
                  </div>
                  <span className="text-gray-600 flex-shrink-0">{expandedEvents[i] ? <ChevronDown size={10} /> : <ChevronRight size={10} />}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-6">
        <h3 className="text-sm font-semibold text-white mb-4 flex items-center gap-2">
          <Route size={14} className="text-brand-400" /> Most Common Conversion Paths
        </h3>
        {paths.length === 0 ? (
          <div className="text-center py-8 text-gray-600 text-sm">No conversion paths yet.</div>
        ) : (
          <div className="space-y-3">
            {paths.map((p: any, i: number) => {
              const steps = String(p.path || "").split(" → ");
              return (
                <div key={i} className="flex items-center gap-3 p-3 rounded-lg bg-white/[0.02] border border-[var(--card-border)]">
                  <div className="w-6 h-6 rounded-full bg-brand-600/20 text-brand-400 flex items-center justify-center text-[10px] font-bold flex-shrink-0">
                    {i + 1}
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-1 flex-wrap">
                      {steps.map((step: string, j: number) => (
                        <span key={j} className="flex items-center gap-1">
                          <span className={`px-2 py-0.5 rounded text-[11px] font-medium ${chipClass(step)}`}>
                            {step}
                          </span>
                          {j < steps.length - 1 && <ArrowRight size={10} className="text-gray-600" />}
                        </span>
                      ))}
                    </div>
                  </div>
                  <div className="flex items-center gap-4 flex-shrink-0 text-xs">
                    <div className="text-right">
                      <div className="text-gray-400">{formatNumber(p.conversions)} conv</div>
                      <div className="text-[10px] text-gray-600">{formatNumber(p.touchpoints)} steps</div>
                    </div>
                    <div className="text-right">
                      <div className="text-emerald-400 font-medium">{formatMoney(Number(p.total_revenue || 0))}</div>
                      <div className="text-[10px] text-gray-600">{formatMoney(Number(p.avg_revenue || 0))} avg</div>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
