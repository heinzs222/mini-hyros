"use client";
import { useEffect, useState } from "react";
import { fetchJourneyStats, fetchCommonPaths, fetchCustomerJourney } from "@/lib/api";
import { Route, ArrowRight, Search, Clock, ShoppingCart, MousePointer, ChevronDown, ChevronRight, Info } from "lucide-react";

const PLATFORM_COLORS: Record<string, string> = {
  meta: "bg-blue-600/20 text-blue-300",
  google: "bg-red-600/20 text-red-300",
  tiktok: "bg-pink-600/20 text-pink-300",
  email: "bg-yellow-600/20 text-yellow-300",
  organic: "bg-green-600/20 text-green-300",
  direct: "bg-gray-600/20 text-gray-300",
};

function timelineIcon(type: string) {
  if (type === "session") return <MousePointer size={11} />;
  if (type === "conversion") return <ShoppingCart size={11} />;
  if (type === "order") return <ShoppingCart size={11} />;
  return <Clock size={11} />;
}

function formatTs(ts: string) {
  try { return new Date(ts).toLocaleString(); } catch { return ts; }
}

export default function JourneyPanel() {
  const [stats, setStats] = useState<any>(null);
  const [paths, setPaths] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [searching, setSearching] = useState(false);
  const [customerJourney, setCustomerJourney] = useState<any>(null);
  const [searchError, setSearchError] = useState("");
  const [expandedEvents, setExpandedEvents] = useState<Record<number, boolean>>({});

  useEffect(() => {
    async function load() {
      setLoading(true);
      try {
        const [s, p] = await Promise.all([fetchJourneyStats(), fetchCommonPaths(20, 1)]);
        setStats(s);
        setPaths(p.rows || []);
      } catch {}
      setLoading(false);
    }
    load();
  }, []);

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

  const hasData = stats && stats.total_journeys > 0;

  if (loading) return <div className="text-center py-12 text-gray-500 text-sm">Loading journey data...</div>;

  return (
    <div className="space-y-6">
      {/* Setup guide when no data */}
      {!hasData && (
        <div className="rounded-xl border border-yellow-500/20 bg-yellow-500/5 p-4 text-sm text-yellow-300 flex gap-3">
          <Info size={16} className="flex-shrink-0 mt-0.5" />
          <div>
            <div className="font-medium mb-1">Journey data requires tracking setup</div>
            <ol className="text-yellow-400/70 text-xs space-y-1 list-decimal list-inside">
              <li>Install the tracking pixel on your website (Settings → Tracking)</li>
              <li>Configure GHL to send webhooks to: <code className="bg-black/20 px-1 rounded">{process.env.NEXT_PUBLIC_API_URL || "https://mini-hyros.onrender.com"}/api/webhooks/ghl</code></li>
              <li>Once customers visit and convert, journeys will appear here automatically</li>
            </ol>
          </div>
        </div>
      )}

      {/* Stats */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
        {[
          { label: "Total Journeys", value: stats?.total_journeys || 0 },
          { label: "Avg Touchpoints", value: stats?.avg_touchpoints_before_conversion || 0 },
          { label: "Avg Time to Convert", value: stats?.avg_time_to_convert_hours ? `${stats.avg_time_to_convert_hours}h` : "—" },
          { label: "Single-Touch", value: `${stats?.single_touch_pct || 0}%` },
          { label: "Multi-Touch", value: `${stats?.multi_touch_pct || 0}%` },
        ].map((c, i) => (
          <div key={i} className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-4">
            <div className="text-[11px] text-gray-500 uppercase tracking-wider">{c.label}</div>
            <div className="text-xl font-bold text-white mt-1">{c.value}</div>
          </div>
        ))}
      </div>

      {/* Customer Search */}
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
            placeholder="Paste customer_key (SHA256) or email hash..."
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

        {/* Customer Journey Timeline */}
        {customerJourney && (
          <div className="mt-4 space-y-3">
            <div className="grid grid-cols-2 md:grid-cols-4 gap-2 text-xs">
              {[
                { label: "Sessions", value: customerJourney.summary.total_sessions },
                { label: "Touchpoints", value: customerJourney.summary.total_touchpoints },
                { label: "Orders", value: customerJourney.summary.total_orders },
                { label: "Revenue", value: `$${customerJourney.summary.total_revenue?.toLocaleString()}` },
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
                  onClick={() => setExpandedEvents(prev => ({ ...prev, [i]: !prev[i] }))}
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
                        <span className="text-emerald-400 font-medium">${event.details.gross?.toLocaleString()}</span>
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

      {/* Common Conversion Paths */}
      <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-6">
        <h3 className="text-sm font-semibold text-white mb-4 flex items-center gap-2">
          <Route size={14} className="text-brand-400" /> Most Common Conversion Paths
        </h3>
        {paths.length === 0 ? (
          <div className="text-center py-8 text-gray-600 text-sm">No conversion paths yet. Paths will appear as customers complete their journeys.</div>
        ) : (
          <div className="space-y-3">
            {paths.map((p: any, i: number) => (
              <div key={i} className="flex items-center gap-3 p-3 rounded-lg bg-white/[0.02] border border-[var(--card-border)]">
                <div className="w-6 h-6 rounded-full bg-brand-600/20 text-brand-400 flex items-center justify-center text-[10px] font-bold flex-shrink-0">
                  {i + 1}
                </div>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-1 flex-wrap">
                    {p.path.split(" → ").map((step: string, j: number, arr: string[]) => (
                      <span key={j} className="flex items-center gap-1">
                        <span className={`px-2 py-0.5 rounded text-[11px] font-medium ${PLATFORM_COLORS[step] || "bg-brand-600/10 text-brand-300"}`}>
                          {step}
                        </span>
                        {j < arr.length - 1 && <ArrowRight size={10} className="text-gray-600" />}
                      </span>
                    ))}
                  </div>
                </div>
                <div className="flex items-center gap-4 flex-shrink-0 text-xs">
                  <div className="text-right">
                    <div className="text-gray-400">{p.conversions} conv</div>
                    <div className="text-[10px] text-gray-600">{p.touchpoints} steps</div>
                  </div>
                  <div className="text-right">
                    <div className="text-emerald-400 font-medium">${p.total_revenue?.toLocaleString()}</div>
                    <div className="text-[10px] text-gray-600">${p.avg_revenue?.toLocaleString()} avg</div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
