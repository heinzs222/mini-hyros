"use client";

import { ArrowDown, ArrowUp } from "lucide-react";
import Sparkline from "./Sparkline";

interface Props {
  label: string;
  value: string;
  /** Percentage change vs comparison period, e.g. -15 for "down 15%". null hides the badge. */
  deltaPct?: number | null;
  /** When the metric going up is a good thing (revenue, ROAS). Cost/CAC/CPL => false. */
  goodWhenUp?: boolean;
  caption?: string;
  data: Array<number | null | undefined>;
  /** Optional per-point labels (short dates) surfaced in the sparkline hover. */
  labels?: string[];
  /** Formats the hovered sparkline value (money / ratio / number). */
  valueFormatter?: (value: number) => string;
  color?: string;
}

export default function KpiCard({
  label,
  value,
  deltaPct,
  goodWhenUp = true,
  caption,
  data,
  labels,
  valueFormatter,
  color = "#22c55e",
}: Props) {
  const hasDelta = deltaPct != null && Number.isFinite(deltaPct);
  const up = (deltaPct ?? 0) >= 0;
  const isGood = hasDelta ? (up ? goodWhenUp : !goodWhenUp) : true;
  const deltaColor = isGood ? "text-emerald-400" : "text-rose-400";
  const deltaBg = isGood ? "bg-emerald-500/10" : "bg-rose-500/10";

  return (
    <div className="hpanel group relative p-4 transition-colors hover:border-white/10">
      <div className="mb-1">
        <span className="h-label text-ink-dim">{label}</span>
      </div>

      <div className="flex items-end justify-between gap-3">
        <div className="min-w-0">
          <div className="h-num text-[26px] leading-tight">{value}</div>
          <div className="mt-1.5 flex items-center gap-1.5">
            {hasDelta && (
              <span
                className={`inline-flex items-center gap-0.5 rounded-md px-1.5 py-0.5 text-[11px] font-semibold ${deltaColor} ${deltaBg}`}
              >
                {up ? <ArrowUp size={11} /> : <ArrowDown size={11} />}
                {Math.abs(deltaPct as number).toFixed(0)}%
              </span>
            )}
            {caption && <span className="text-[11px] font-medium text-ink-dim">{caption}</span>}
          </div>
        </div>
        <div className="h-[58px] w-[52%] shrink-0 self-end">
          <Sparkline data={data} labels={labels} valueFormatter={valueFormatter} color={color} height={58} />
        </div>
      </div>
    </div>
  );
}
