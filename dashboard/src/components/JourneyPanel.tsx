"use client";
import { useEffect, useState } from "react";
import { fetchJourneyStats, fetchCommonPaths } from "@/lib/api";
import { Route, ArrowRight } from "lucide-react";

export default function JourneyPanel() {
  const [stats, setStats] = useState<any>(null);
  const [paths, setPaths] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      setLoading(true);
      try {
        const [s, p] = await Promise.all([fetchJourneyStats(), fetchCommonPaths(15)]);
        setStats(s);
        setPaths(p.rows || []);
      } catch {}
      setLoading(false);
    }
    load();
  }, []);

  if (loading) return <div className="text-center py-12 text-gray-500 text-sm">Loading journey data...</div>;

  return (
    <div className="space-y-6">
      {/* Journey Stats */}
      {stats && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {[
            { label: "Avg Touchpoints", value: stats.avg_touchpoints_before_conversion || 0 },
            { label: "Avg Time to Convert", value: stats.avg_time_to_convert_hours ? `${stats.avg_time_to_convert_hours}h` : "—" },
            { label: "Single-Touch", value: `${stats.single_touch_pct || 0}%` },
            { label: "Multi-Touch", value: `${stats.multi_touch_pct || 0}%` },
          ].map((c, i) => (
            <div key={i} className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-4">
              <div className="text-[11px] text-gray-500 uppercase tracking-wider">{c.label}</div>
              <div className="text-xl font-bold text-white mt-1">{c.value}</div>
            </div>
          ))}
        </div>
      )}

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
                        <span className="px-2 py-0.5 rounded bg-brand-600/10 text-brand-300 text-[11px] font-medium">
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
