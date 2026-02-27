"use client";
import { useEffect, useState } from "react";
import { fetchLtvBySource, fetchLtvSummary } from "@/lib/api";
import { TrendingUp } from "lucide-react";

export default function LtvPanel() {
  const [summary, setSummary] = useState<any>(null);
  const [rows, setRows] = useState<any[]>([]);
  const [breakdown, setBreakdown] = useState("platform");
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      setLoading(true);
      try {
        const [s, r] = await Promise.all([fetchLtvSummary(), fetchLtvBySource(breakdown)]);
        setSummary(s);
        setRows(r.rows || []);
      } catch {}
      setLoading(false);
    }
    load();
  }, [breakdown]);

  if (loading) return <div className="text-center py-12 text-gray-500 text-sm">Loading LTV data...</div>;

  return (
    <div className="space-y-6">
      {/* Summary Cards */}
      {summary && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {[
            { label: "Avg LTV", value: `$${summary.avg_ltv?.toLocaleString()}` },
            { label: "Avg Order Value", value: `$${summary.avg_order_value?.toLocaleString()}` },
            { label: "Repeat Rate", value: `${summary.repeat_purchase_rate}%` },
            { label: "Avg Orders/Customer", value: summary.avg_orders_per_customer },
          ].map((c, i) => (
            <div key={i} className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-4">
              <div className="text-[11px] text-gray-500 uppercase tracking-wider">{c.label}</div>
              <div className="text-xl font-bold text-white mt-1">{c.value}</div>
            </div>
          ))}
        </div>
      )}

      {/* Breakdown selector */}
      <div className="flex items-center gap-2">
        <span className="text-xs text-gray-500">Breakdown:</span>
        {["platform", "campaign_id", "ad_id"].map((b) => (
          <button
            key={b}
            onClick={() => setBreakdown(b)}
            className={`px-3 py-1 rounded-lg text-xs font-medium transition-colors ${
              breakdown === b ? "bg-brand-600 text-white" : "bg-[var(--card)] text-gray-400 hover:text-white border border-[var(--card-border)]"
            }`}
          >
            {b.replace("_", " ").replace(/\b\w/g, (c) => c.toUpperCase())}
          </button>
        ))}
      </div>

      {/* LTV Table */}
      <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] overflow-hidden">
        <div className="p-4 border-b border-[var(--card-border)]">
          <h3 className="text-sm font-semibold text-white flex items-center gap-2">
            <TrendingUp size={14} className="text-brand-400" /> LTV by {breakdown.replace("_", " ")}
          </h3>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-[var(--card-border)] text-gray-500">
                <th className="text-left p-3 font-medium">Source</th>
                <th className="text-right p-3 font-medium">Customers</th>
                <th className="text-right p-3 font-medium">Avg LTV</th>
                <th className="text-right p-3 font-medium">30d LTV</th>
                <th className="text-right p-3 font-medium">60d LTV</th>
                <th className="text-right p-3 font-medium">90d LTV</th>
                <th className="text-right p-3 font-medium">Total Revenue</th>
              </tr>
            </thead>
            <tbody>
              {rows.length === 0 && (
                <tr><td colSpan={7} className="text-center py-8 text-gray-600">No LTV data yet. Revenue will appear here as orders come in.</td></tr>
              )}
              {rows.map((r: any, i: number) => (
                <tr key={i} className="border-b border-[var(--card-border)] hover:bg-white/[0.02]">
                  <td className="p-3 text-gray-300 font-medium">{r.dimension || "—"}</td>
                  <td className="p-3 text-right text-gray-400">{r.customers}</td>
                  <td className="p-3 text-right text-emerald-400 font-medium">${r.avg_ltv?.toLocaleString()}</td>
                  <td className="p-3 text-right text-gray-300">${r.avg_ltv_30d?.toLocaleString()}</td>
                  <td className="p-3 text-right text-gray-300">${r.avg_ltv_60d?.toLocaleString()}</td>
                  <td className="p-3 text-right text-gray-300">${r.avg_ltv_90d?.toLocaleString()}</td>
                  <td className="p-3 text-right text-white font-medium">${r.total_revenue?.toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
