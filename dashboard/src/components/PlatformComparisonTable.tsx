"use client";

import { formatMoney, formatNumber, formatPercentValue, formatRatio, profitColor } from "@/lib/utils";

interface PlatformRow {
  platform: string;
  label: string;
  clicks: number;
  clicks_share: number;
  orders: number;
  cost: number;
  revenue: number;
  profit: number;
  roas: number | null;
  cpa: number | null;
  cpc: number | null;
  cvr: number | null;
}

interface Props {
  rows: PlatformRow[];
  compareRows?: PlatformRow[];
  compareLabel?: string;
}

const PLATFORM_STYLES: Record<string, string> = {
  meta: "bg-blue-500/15 text-blue-300 border border-blue-500/30",
  google: "bg-emerald-500/15 text-emerald-300 border border-emerald-500/30",
  tiktok: "bg-pink-500/15 text-pink-300 border border-pink-500/30",
};

export default function PlatformComparisonTable({ rows, compareRows = [], compareLabel = "" }: Props) {
  const compareByPlatform = new Map(compareRows.map((r) => [r.platform, r]));

  const deltaClass = (delta: number | null | undefined) => {
    if (delta == null) return "text-gray-500";
    if (delta > 0) return "text-emerald-400";
    if (delta < 0) return "text-red-400";
    return "text-gray-500";
  };

  const moneyDelta = (cur: number | null | undefined, prev: number | null | undefined) => {
    if (cur == null || prev == null) return undefined;
    const diff = cur - prev;
    const sign = diff > 0 ? "+" : diff < 0 ? "-" : "";
    return `${sign}${formatMoney(Math.abs(diff))}`;
  };

  const ratioDelta = (cur: number | null | undefined, prev: number | null | undefined) => {
    if (cur == null || prev == null) return undefined;
    const diff = cur - prev;
    const sign = diff > 0 ? "+" : diff < 0 ? "-" : "";
    return `${sign}${Math.abs(diff).toFixed(2)}x`;
  };

  const percentPointDelta = (cur: number | null | undefined, prev: number | null | undefined) => {
    if (cur == null || prev == null) return undefined;
    const diff = cur - prev;
    const sign = diff > 0 ? "+" : diff < 0 ? "-" : "";
    return `${sign}${Math.abs(diff).toFixed(2)} pp`;
  };

  return (
    <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] overflow-hidden">
      <div className="px-4 py-3 border-b border-[var(--card-border)] flex items-center justify-between">
        <div>
          <h3 className="text-sm font-semibold text-white">Source Comparison</h3>
          <p className="text-[11px] text-gray-500 mt-0.5">Compare Facebook, TikTok, and Google side by side.</p>
        </div>
        {compareRows.length > 0 && (
          <span className="text-[11px] px-2 py-1 rounded bg-blue-500/10 text-blue-300 border border-blue-500/25">
            Delta vs {compareLabel || "comparison"}
          </span>
        )}
      </div>

      {rows.length === 0 ? (
        <div className="px-4 py-10 text-center text-sm text-gray-600">
          No platform data yet. Traffic will appear after the first tracked sessions.
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-[var(--card-border)] text-gray-500">
                <th className="text-left px-4 py-2.5 font-medium min-w-[150px]">Platform</th>
                <th className="text-right px-3 py-2.5 font-medium">Clicks</th>
                <th className="text-right px-3 py-2.5 font-medium">Orders</th>
                <th className="text-right px-3 py-2.5 font-medium">Spend</th>
                <th className="text-right px-3 py-2.5 font-medium">Revenue</th>
                <th className="text-right px-3 py-2.5 font-medium">ROAS</th>
                <th className="text-right px-3 py-2.5 font-medium">CPA</th>
                <th className="text-right px-3 py-2.5 font-medium">CVR</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => {
                const cmp = compareByPlatform.get(r.platform);
                const revenueDelta = moneyDelta(r.revenue, cmp?.revenue);
                const roasDelta = ratioDelta(r.roas, cmp?.roas);
                const cvrDelta = percentPointDelta(r.cvr, cmp?.cvr);

                return (
                  <tr key={r.platform} className="border-b border-[var(--card-border)] hover:bg-white/[0.02]">
                    <td className="px-4 py-2.5">
                      <div className="flex items-center gap-2">
                        <span className={`px-2 py-0.5 rounded text-[10px] font-medium ${PLATFORM_STYLES[r.platform] || "bg-white/10 text-gray-300"}`}>
                          {r.label}
                        </span>
                      </div>
                    </td>
                    <td className="px-3 py-2.5 text-right">
                      <div className="text-gray-200">{formatNumber(r.clicks)}</div>
                      <div className="text-[10px] text-gray-500">{formatPercentValue(r.clicks_share, 1)}</div>
                    </td>
                    <td className="px-3 py-2.5 text-right text-gray-300">{formatNumber(r.orders)}</td>
                    <td className="px-3 py-2.5 text-right text-gray-300">{formatMoney(r.cost)}</td>
                    <td className="px-3 py-2.5 text-right">
                      <div className="text-gray-100">{formatMoney(r.revenue)}</div>
                      {revenueDelta && (
                        <div className={`text-[10px] ${deltaClass((r.revenue ?? 0) - (cmp?.revenue ?? 0))}`}>{revenueDelta}</div>
                      )}
                    </td>
                    <td className="px-3 py-2.5 text-right">
                      <span className={r.roas != null && r.roas >= 1 ? "text-emerald-400" : "text-red-400"}>
                        {formatRatio(r.roas)}
                      </span>
                      {roasDelta && (
                        <div className={`text-[10px] ${deltaClass((r.roas ?? 0) - (cmp?.roas ?? 0))}`}>{roasDelta}</div>
                      )}
                    </td>
                    <td className="px-3 py-2.5 text-right text-gray-300">{formatMoney(r.cpa)}</td>
                    <td className="px-3 py-2.5 text-right">
                      <span className={profitColor((r.cvr || 0) - 2)}>{formatPercentValue(r.cvr, 2)}</span>
                      {cvrDelta && (
                        <div className={`text-[10px] ${deltaClass((r.cvr ?? 0) - (cmp?.cvr ?? 0))}`}>{cvrDelta}</div>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
