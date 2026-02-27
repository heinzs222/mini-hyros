"use client";
import { useEffect, useState } from "react";
import { fetchFunnelReport, fetchFunnelBySource } from "@/lib/api";
import { Filter } from "lucide-react";

export default function FunnelPanel() {
  const [funnel, setFunnel] = useState<any>(null);
  const [bySource, setBySource] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      setLoading(true);
      try {
        const [f, s] = await Promise.all([fetchFunnelReport(), fetchFunnelBySource()]);
        setFunnel(f);
        setBySource(s.rows || []);
      } catch {}
      setLoading(false);
    }
    load();
  }, []);

  if (loading) return <div className="text-center py-12 text-gray-500 text-sm">Loading funnel data...</div>;

  const stages = funnel?.stages || [];
  const maxCount = stages.length > 0 ? Math.max(...stages.map((s: any) => s.count), 1) : 1;

  return (
    <div className="space-y-6">
      {/* Funnel Visualization */}
      <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-6">
        <h3 className="text-sm font-semibold text-white mb-6 flex items-center gap-2">
          <Filter size={14} className="text-brand-400" /> Conversion Funnel
        </h3>
        {stages.length === 0 ? (
          <div className="text-center py-8 text-gray-600 text-sm">No funnel data yet. Events will populate as leads, bookings, and purchases come in.</div>
        ) : (
          <div className="space-y-3">
            {stages.map((stage: any, i: number) => {
              const widthPct = Math.max((stage.count / maxCount) * 100, 8);
              const colors = ["bg-brand-500", "bg-blue-500", "bg-cyan-500", "bg-emerald-500", "bg-green-500"];
              return (
                <div key={stage.key} className="flex items-center gap-4">
                  <div className="w-28 text-right text-xs text-gray-400 flex-shrink-0">{stage.label}</div>
                  <div className="flex-1">
                    <div
                      className={`${colors[i % colors.length]} rounded-md h-9 flex items-center px-3 transition-all`}
                      style={{ width: `${widthPct}%` }}
                    >
                      <span className="text-xs font-bold text-white">{stage.count.toLocaleString()}</span>
                    </div>
                  </div>
                  <div className="w-20 text-right flex-shrink-0">
                    {i > 0 && (
                      <div className="text-xs">
                        <span className={stage.step_rate >= 50 ? "text-emerald-400" : stage.step_rate >= 20 ? "text-yellow-400" : "text-red-400"}>
                          {stage.step_rate}%
                        </span>
                        <span className="text-gray-600 ml-1">step</span>
                      </div>
                    )}
                    <div className="text-[10px] text-gray-500">{stage.overall_rate}% total</div>
                  </div>
                  {i > 0 && stage.drop_off > 0 && (
                    <div className="w-16 text-right flex-shrink-0 text-[10px] text-red-400/60">
                      -{stage.drop_off.toLocaleString()}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Funnel by Source */}
      {bySource.length > 0 && (
        <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] overflow-hidden">
          <div className="p-4 border-b border-[var(--card-border)]">
            <h3 className="text-sm font-semibold text-white">Funnel by Traffic Source</h3>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-[var(--card-border)] text-gray-500">
                  <th className="text-left p-3 font-medium">Source</th>
                  <th className="text-right p-3 font-medium">Visits</th>
                  <th className="text-right p-3 font-medium">Leads</th>
                  <th className="text-right p-3 font-medium">Lead %</th>
                  <th className="text-right p-3 font-medium">Bookings</th>
                  <th className="text-right p-3 font-medium">Book %</th>
                  <th className="text-right p-3 font-medium">Purchases</th>
                  <th className="text-right p-3 font-medium">Conv %</th>
                  <th className="text-right p-3 font-medium">Revenue</th>
                </tr>
              </thead>
              <tbody>
                {bySource.map((r: any, i: number) => (
                  <tr key={i} className="border-b border-[var(--card-border)] hover:bg-white/[0.02]">
                    <td className="p-3 text-gray-300 font-medium">{r.source || "—"}</td>
                    <td className="p-3 text-right text-gray-400">{r.visits?.toLocaleString()}</td>
                    <td className="p-3 text-right text-gray-300">{r.leads}</td>
                    <td className="p-3 text-right text-blue-400">{r.lead_rate}%</td>
                    <td className="p-3 text-right text-gray-300">{r.bookings}</td>
                    <td className="p-3 text-right text-cyan-400">{r.booking_rate}%</td>
                    <td className="p-3 text-right text-emerald-400 font-medium">{r.purchases}</td>
                    <td className="p-3 text-right text-emerald-400">{r.purchase_rate}%</td>
                    <td className="p-3 text-right text-white font-medium">${r.revenue?.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
