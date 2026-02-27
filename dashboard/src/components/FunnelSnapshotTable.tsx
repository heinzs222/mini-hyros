"use client";

import { formatMoney, formatNumber, formatPercentValue } from "@/lib/utils";

interface FunnelRow {
  funnel_id: string;
  funnel_name: string;
  visits: number;
  leads: number;
  purchases: number;
  lead_rate: number;
  purchase_rate: number;
  revenue: number;
  aov: number | null;
}

interface Props {
  rows: FunnelRow[];
  compareRows?: FunnelRow[];
  compareLabel?: string;
}

function deltaClass(delta: number | null | undefined): string {
  if (delta == null) return "text-gray-500";
  if (delta > 0) return "text-emerald-400";
  if (delta < 0) return "text-red-400";
  return "text-gray-500";
}

function numberDelta(current: number | null | undefined, previous: number | null | undefined): string | undefined {
  if (current == null || previous == null) return undefined;
  const diff = current - previous;
  const sign = diff > 0 ? "+" : diff < 0 ? "-" : "";
  return `${sign}${formatNumber(Math.abs(diff))}`;
}

function moneyDelta(current: number | null | undefined, previous: number | null | undefined): string | undefined {
  if (current == null || previous == null) return undefined;
  const diff = current - previous;
  const sign = diff > 0 ? "+" : diff < 0 ? "-" : "";
  return `${sign}${formatMoney(Math.abs(diff))}`;
}

export default function FunnelSnapshotTable({ rows, compareRows = [], compareLabel = "" }: Props) {
  const compareById = new Map(compareRows.map((r) => [r.funnel_id, r]));

  return (
    <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] overflow-hidden">
      <div className="px-4 py-3 border-b border-[var(--card-border)] flex items-center justify-between gap-2">
        <div>
          <h3 className="text-sm font-semibold text-white">Top Funnels</h3>
          <p className="text-[11px] text-gray-500 mt-0.5">See which funnel pages generate visits, leads, and purchases.</p>
        </div>
        {compareRows.length > 0 && (
          <span className="text-[11px] px-2 py-1 rounded bg-blue-500/10 text-blue-300 border border-blue-500/25">
            Delta vs {compareLabel || "comparison"}
          </span>
        )}
      </div>

      {rows.length === 0 ? (
        <div className="px-4 py-10 text-center text-sm text-gray-600">
          No funnel traffic yet. Funnel names appear once sessions and conversions are tracked.
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-[var(--card-border)] text-gray-500">
                <th className="text-left px-4 py-2.5 font-medium min-w-[180px]">Funnel</th>
                <th className="text-right px-3 py-2.5 font-medium">Visits</th>
                <th className="text-right px-3 py-2.5 font-medium">Leads</th>
                <th className="text-right px-3 py-2.5 font-medium">Lead %</th>
                <th className="text-right px-3 py-2.5 font-medium">Purchases</th>
                <th className="text-right px-3 py-2.5 font-medium">Conv %</th>
                <th className="text-right px-3 py-2.5 font-medium">Revenue</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => {
                const cmp = compareById.get(r.funnel_id);
                const visitsDelta = numberDelta(r.visits, cmp?.visits);
                const purchasesDelta = numberDelta(r.purchases, cmp?.purchases);
                const revenueDelta = moneyDelta(r.revenue, cmp?.revenue);

                return (
                  <tr key={r.funnel_id} className="border-b border-[var(--card-border)] hover:bg-white/[0.02]">
                    <td className="px-4 py-2.5">
                      <div className="text-gray-200 font-medium truncate max-w-[220px]" title={`${r.funnel_name} (${r.funnel_id})`}>
                        {r.funnel_name}
                      </div>
                      <div className="text-[10px] text-gray-600 font-mono">{r.funnel_id}</div>
                    </td>
                    <td className="px-3 py-2.5 text-right">
                      <div className="text-gray-300">{formatNumber(r.visits)}</div>
                      {visitsDelta && (
                        <div className={`text-[10px] ${deltaClass((r.visits ?? 0) - (cmp?.visits ?? 0))}`}>{visitsDelta}</div>
                      )}
                    </td>
                    <td className="px-3 py-2.5 text-right text-blue-300">{formatNumber(r.leads)}</td>
                    <td className="px-3 py-2.5 text-right text-blue-400">{formatPercentValue(r.lead_rate, 2)}</td>
                    <td className="px-3 py-2.5 text-right">
                      <div className="text-emerald-300">{formatNumber(r.purchases)}</div>
                      {purchasesDelta && (
                        <div className={`text-[10px] ${deltaClass((r.purchases ?? 0) - (cmp?.purchases ?? 0))}`}>{purchasesDelta}</div>
                      )}
                    </td>
                    <td className="px-3 py-2.5 text-right text-emerald-400">{formatPercentValue(r.purchase_rate, 2)}</td>
                    <td className="px-3 py-2.5 text-right text-white">
                      <div>{formatMoney(r.revenue)}</div>
                      {revenueDelta && (
                        <div className={`text-[10px] ${deltaClass((r.revenue ?? 0) - (cmp?.revenue ?? 0))}`}>{revenueDelta}</div>
                      )}
                      {r.aov != null && (
                        <div className="text-[10px] text-gray-500">AOV {formatMoney(r.aov)}</div>
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
