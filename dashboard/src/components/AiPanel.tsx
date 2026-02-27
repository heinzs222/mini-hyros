"use client";
import { useEffect, useState } from "react";
import { fetchRecommendations, fetchInsights, fetchRefundSummary } from "@/lib/api";
import { Brain, TrendingUp, TrendingDown, AlertTriangle, Info, ArrowUpRight, Pause, Settings2 } from "lucide-react";

export default function AiPanel() {
  const [recs, setRecs] = useState<any[]>([]);
  const [insights, setInsights] = useState<any[]>([]);
  const [refunds, setRefunds] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      setLoading(true);
      try {
        const [r, i, ref] = await Promise.all([
          fetchRecommendations(),
          fetchInsights(),
          fetchRefundSummary(),
        ]);
        setRecs(r.recommendations || []);
        setInsights(i.insights || []);
        setRefunds(ref);
      } catch {}
      setLoading(false);
    }
    load();
  }, []);

  if (loading) return <div className="text-center py-12 text-gray-500 text-sm">Analyzing your data...</div>;

  const actionIcons: Record<string, any> = {
    scale: <TrendingUp size={14} className="text-emerald-400" />,
    optimize: <Settings2 size={14} className="text-yellow-400" />,
    optimize_or_pause: <AlertTriangle size={14} className="text-orange-400" />,
    pause: <Pause size={14} className="text-red-400" />,
    monitor: <Info size={14} className="text-gray-400" />,
  };

  const actionColors: Record<string, string> = {
    scale: "border-emerald-500/30 bg-emerald-500/5",
    optimize: "border-yellow-500/30 bg-yellow-500/5",
    optimize_or_pause: "border-orange-500/30 bg-orange-500/5",
    pause: "border-red-500/30 bg-red-500/5",
    monitor: "border-gray-500/30 bg-gray-500/5",
  };

  return (
    <div className="space-y-6">
      {/* Insights */}
      {insights.length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          {insights.map((insight: any, i: number) => (
            <div
              key={i}
              className={`rounded-xl border p-4 ${
                insight.priority === "warning" ? "border-yellow-500/30 bg-yellow-500/5" : "border-brand-500/30 bg-brand-500/5"
              }`}
            >
              <div className="flex items-start gap-2">
                {insight.priority === "warning" ? (
                  <AlertTriangle size={14} className="text-yellow-400 mt-0.5 flex-shrink-0" />
                ) : (
                  <Info size={14} className="text-brand-400 mt-0.5 flex-shrink-0" />
                )}
                <div>
                  <div className="text-xs font-semibold text-white">{insight.title}</div>
                  <div className="text-[11px] text-gray-400 mt-1">{insight.detail}</div>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Refund Summary */}
      {refunds && (refunds.total_orders > 0) && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {[
            { label: "Refund Rate", value: `${refunds.refund_rate}%`, warn: refunds.refund_rate > 10 },
            { label: "Chargeback Rate", value: `${refunds.chargeback_rate}%`, warn: refunds.chargeback_rate > 1 },
            { label: "Net After Refunds", value: `$${refunds.net_after_refunds?.toLocaleString()}` },
            { label: "Total Orders", value: refunds.total_orders },
          ].map((c, i) => (
            <div key={i} className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-4">
              <div className="text-[11px] text-gray-500 uppercase tracking-wider">{c.label}</div>
              <div className={`text-xl font-bold mt-1 ${c.warn ? "text-red-400" : "text-white"}`}>{c.value}</div>
            </div>
          ))}
        </div>
      )}

      {/* Ad Recommendations */}
      <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-6">
        <h3 className="text-sm font-semibold text-white mb-4 flex items-center gap-2">
          <Brain size={14} className="text-brand-400" /> Ad Recommendations
        </h3>
        {recs.length === 0 ? (
          <div className="text-center py-8 text-gray-600 text-sm">No recommendations yet. As spend and conversion data flows in, we&apos;ll analyze which campaigns to scale, optimize, or pause.</div>
        ) : (
          <div className="space-y-3">
            {recs.map((rec: any, i: number) => (
              <div key={i} className={`rounded-lg border p-4 ${actionColors[rec.action] || "border-[var(--card-border)]"}`}>
                <div className="flex items-start justify-between gap-4">
                  <div className="flex items-start gap-3 flex-1">
                    {actionIcons[rec.action] || <Info size={14} className="text-gray-400 mt-0.5" />}
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 mb-1">
                        <span className="text-xs font-semibold text-white capitalize">{rec.platform}</span>
                        <span className="text-[10px] text-gray-500 truncate">{rec.campaign_id}</span>
                        <span className={`px-1.5 py-0.5 rounded text-[10px] font-bold uppercase ${
                          rec.action === "scale" ? "bg-emerald-500/20 text-emerald-400" :
                          rec.action === "pause" ? "bg-red-500/20 text-red-400" :
                          rec.action === "optimize" ? "bg-yellow-500/20 text-yellow-400" :
                          rec.action === "optimize_or_pause" ? "bg-orange-500/20 text-orange-400" :
                          "bg-gray-500/20 text-gray-400"
                        }`}>
                          {rec.action.replace("_", " ")}
                        </span>
                      </div>
                      <p className="text-[11px] text-gray-400">{rec.reason}</p>
                      {rec.suggestions && (
                        <ul className="mt-2 space-y-0.5">
                          {rec.suggestions.map((s: string, j: number) => (
                            <li key={j} className="text-[10px] text-gray-500 flex gap-1">
                              <span className="text-gray-600">-</span> {s}
                            </li>
                          ))}
                        </ul>
                      )}
                    </div>
                  </div>
                  <div className="flex-shrink-0 text-right text-xs">
                    <div className="text-gray-400">Spend: ${rec.spend?.toLocaleString()}</div>
                    <div className="text-gray-400">Revenue: ${rec.revenue?.toLocaleString()}</div>
                    <div className={`font-medium ${rec.roas >= 1 ? "text-emerald-400" : "text-red-400"}`}>
                      {rec.roas}x ROAS
                    </div>
                    {rec.suggested_budget_change && (
                      <div className="mt-1 text-emerald-400 font-medium flex items-center gap-0.5 justify-end">
                        <ArrowUpRight size={10} /> {rec.suggested_budget_change}
                      </div>
                    )}
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
