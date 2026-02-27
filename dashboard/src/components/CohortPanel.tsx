"use client";
import { useEffect, useState } from "react";
import { fetchCohortAnalysis } from "@/lib/api";
import { Grid3x3 } from "lucide-react";

export default function CohortPanel() {
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      setLoading(true);
      try {
        const d = await fetchCohortAnalysis("month");
        setData(d);
      } catch {}
      setLoading(false);
    }
    load();
  }, []);

  if (loading) return <div className="text-center py-12 text-gray-500 text-sm">Loading cohort data...</div>;

  const cohorts = data?.cohorts || [];
  const periods = data?.periods || [];

  return (
    <div className="space-y-6">
      {/* Cohort Summary Cards */}
      {cohorts.length > 0 && (
        <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
          {[
            { label: "Total Cohorts", value: cohorts.length },
            { label: "Total Customers", value: cohorts.reduce((s: number, c: any) => s + c.customers, 0) },
            { label: "Best Cohort LTV", value: `$${Math.max(...cohorts.map((c: any) => c.avg_ltv || 0)).toLocaleString()}` },
          ].map((c, i) => (
            <div key={i} className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-4">
              <div className="text-[11px] text-gray-500 uppercase tracking-wider">{c.label}</div>
              <div className="text-xl font-bold text-white mt-1">{c.value}</div>
            </div>
          ))}
        </div>
      )}

      {/* Cohort Matrix */}
      <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] overflow-hidden">
        <div className="p-4 border-b border-[var(--card-border)]">
          <h3 className="text-sm font-semibold text-white flex items-center gap-2">
            <Grid3x3 size={14} className="text-brand-400" /> Cohort LTV Matrix
          </h3>
          <p className="text-[11px] text-gray-500 mt-1">Each cell shows cumulative LTV per customer for that cohort at that period</p>
        </div>
        {cohorts.length === 0 ? (
          <div className="text-center py-8 text-gray-600 text-sm p-4">No cohort data yet. Cohorts will form as customers are acquired over time.</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-[var(--card-border)] text-gray-500">
                  <th className="text-left p-3 font-medium sticky left-0 bg-[var(--card)]">Cohort</th>
                  <th className="text-right p-3 font-medium">Customers</th>
                  {periods.map((p: number) => (
                    <th key={p} className="text-right p-3 font-medium">M{p}</th>
                  ))}
                  <th className="text-right p-3 font-medium">Total LTV</th>
                </tr>
              </thead>
              <tbody>
                {cohorts.map((cohort: any, i: number) => {
                  const maxLtv = Math.max(...cohorts.map((c: any) => c.avg_ltv || 0), 1);
                  return (
                    <tr key={i} className="border-b border-[var(--card-border)] hover:bg-white/[0.02]">
                      <td className="p-3 text-gray-300 font-medium sticky left-0 bg-[var(--card)]">{cohort.cohort}</td>
                      <td className="p-3 text-right text-gray-400">{cohort.customers}</td>
                      {periods.map((p: number) => {
                        const periodData = cohort.periods?.find((pd: any) => pd.period === p);
                        const ltv = periodData?.ltv_per_customer || 0;
                        const intensity = ltv > 0 ? Math.min(ltv / maxLtv, 1) : 0;
                        return (
                          <td
                            key={p}
                            className="p-3 text-right text-gray-300"
                            style={{
                              backgroundColor: ltv > 0 ? `rgba(99, 102, 241, ${intensity * 0.3})` : "transparent",
                            }}
                          >
                            {ltv > 0 ? `$${ltv.toLocaleString()}` : "—"}
                          </td>
                        );
                      })}
                      <td className="p-3 text-right text-emerald-400 font-medium">
                        ${cohort.avg_ltv?.toLocaleString()}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
