"use client";

import { Area, AreaChart, ResponsiveContainer, Tooltip, YAxis } from "recharts";

interface Props {
  data: Array<number | null | undefined>;
  /** Optional per-point labels (e.g. short dates) shown in the hover tooltip. */
  labels?: string[];
  /** Formats the value in the hover tooltip (defaults to a localized number). */
  valueFormatter?: (value: number) => string;
  color?: string;
  height?: number;
  strokeWidth?: number;
  /** Set false to render a static trend glyph with no hover (rarely needed). */
  interactive?: boolean;
}

function SparkTooltip({
  active,
  payload,
  valueFormatter,
}: {
  active?: boolean;
  payload?: any[];
  valueFormatter?: (value: number) => string;
}) {
  if (!active || !payload?.length) return null;
  const point = payload[0]?.payload || {};
  if (point.v == null) return null;
  const value = Number(point.v);
  return (
    <div className="pointer-events-none rounded-lg border border-[var(--card-border)] bg-[#0c0c11] px-2.5 py-1.5 text-[11px] shadow-xl">
      {point.label && <div className="mb-0.5 font-medium text-ink-dim">{point.label}</div>}
      <div className="tabular font-semibold text-ink-bright">
        {valueFormatter ? valueFormatter(value) : value.toLocaleString()}
      </div>
    </div>
  );
}

/**
 * Tiny gradient area chart used inside Hyros-style KPI cards. Interactive by
 * default: hovering shows the point's value (and label, if provided) in a small
 * tooltip with a guide line + active dot, matching the larger dashboard charts.
 */
export default function Sparkline({
  data,
  labels,
  valueFormatter,
  color = "#22c55e",
  height = 60,
  strokeWidth = 2,
  interactive = true,
}: Props) {
  // Map null/undefined/non-finite points to null (not 0) so Recharts breaks the
  // line into gaps instead of plotting fake dips to zero.
  const series = (data && data.length ? data : [0, 0]).map((v, i) => {
    const n = Number(v);
    return { i, v: v == null || !Number.isFinite(n) ? null : n, label: labels?.[i] };
  });
  const gradientId = `spark-${color.replace(/[^a-z0-9]/gi, "")}`;

  return (
    <div style={{ height }} className="w-full">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={series} margin={{ top: 8, right: 0, bottom: 0, left: 0 }}>
          <defs>
            <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={color} stopOpacity={0.4} />
              <stop offset="100%" stopColor={color} stopOpacity={0} />
            </linearGradient>
          </defs>
          <YAxis hide domain={["dataMin", "dataMax"]} />
          {interactive && (
            <Tooltip
              content={<SparkTooltip valueFormatter={valueFormatter} />}
              cursor={{ stroke: color, strokeOpacity: 0.35, strokeWidth: 1 }}
              wrapperStyle={{ zIndex: 50, outline: "none" }}
              isAnimationActive={false}
              allowEscapeViewBox={{ x: false, y: true }}
            />
          )}
          <Area
            type="monotone"
            dataKey="v"
            stroke={color}
            strokeWidth={strokeWidth}
            fill={`url(#${gradientId})`}
            isAnimationActive={false}
            dot={false}
            activeDot={interactive ? { r: 2.5, fill: color, stroke: "#0c0c11", strokeWidth: 1 } : false}
            connectNulls
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
