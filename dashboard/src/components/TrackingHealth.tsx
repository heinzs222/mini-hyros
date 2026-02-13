"use client";

import { ShieldCheck, ShieldAlert } from "lucide-react";

interface Props {
  tracking: {
    tracking_percentage: number;
    coverage_breakdown: {
      orders_with_source: number;
      orders_total: number;
      sessions_with_click_id: number;
      sessions_total: number;
    };
    top_tracking_gaps: Array<{ issue: string; impact: string; fix: string }>;
  };
}

function ProgressBar({ value, color }: { value: number; color: string }) {
  return (
    <div className="w-full h-2 rounded-full bg-white/5 overflow-hidden">
      <div
        className={`h-full rounded-full transition-all ${color}`}
        style={{ width: `${Math.min(100, Math.max(0, value))}%` }}
      />
    </div>
  );
}

export default function TrackingHealth({ tracking }: Props) {
  const pct = tracking.tracking_percentage;
  const color =
    pct >= 90 ? "bg-emerald-500" : pct >= 70 ? "bg-yellow-500" : "bg-red-500";
  const Icon = pct >= 80 ? ShieldCheck : ShieldAlert;

  const cb = tracking.coverage_breakdown;
  const orderRate = cb.orders_total > 0 ? (cb.orders_with_source / cb.orders_total) * 100 : 0;
  const clickRate = cb.sessions_total > 0 ? (cb.sessions_with_click_id / cb.sessions_total) * 100 : 0;

  return (
    <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-4">
      <div className="flex items-center gap-2 mb-3">
        <Icon size={16} className={pct >= 80 ? "text-emerald-400" : "text-yellow-400"} />
        <h3 className="text-sm font-semibold text-gray-300">Tracking Health</h3>
        <span className={`ml-auto text-lg font-bold ${pct >= 90 ? "text-emerald-400" : pct >= 70 ? "text-yellow-400" : "text-red-400"}`}>
          {pct.toFixed(1)}%
        </span>
      </div>

      <ProgressBar value={pct} color={color} />

      <div className="mt-4 grid grid-cols-2 gap-3 text-xs">
        <div>
          <div className="text-gray-500 mb-1">Orders with Source</div>
          <div className="text-gray-300">
            {cb.orders_with_source} / {cb.orders_total}{" "}
            <span className="text-gray-500">({orderRate.toFixed(0)}%)</span>
          </div>
        </div>
        <div>
          <div className="text-gray-500 mb-1">Sessions with Click ID</div>
          <div className="text-gray-300">
            {cb.sessions_with_click_id} / {cb.sessions_total}{" "}
            <span className="text-gray-500">({clickRate.toFixed(0)}%)</span>
          </div>
        </div>
      </div>

      {tracking.top_tracking_gaps.length > 0 && (
        <div className="mt-4 space-y-2">
          {tracking.top_tracking_gaps.map((gap, i) => (
            <div key={i} className="rounded-lg bg-yellow-500/5 border border-yellow-500/20 p-2.5 text-xs">
              <div className="font-medium text-yellow-400">{gap.issue}</div>
              <div className="text-gray-400 mt-0.5">{gap.impact}</div>
              <div className="text-gray-500 mt-0.5">Fix: {gap.fix}</div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
