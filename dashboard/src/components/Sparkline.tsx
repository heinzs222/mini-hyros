"use client";

import { Area, AreaChart, ResponsiveContainer, YAxis } from "recharts";

interface Props {
  data: Array<number | null | undefined>;
  color?: string;
  height?: number;
  strokeWidth?: number;
}

/**
 * Tiny gradient area chart used inside Hyros-style KPI cards.
 * No axes, no tooltip — purely a trend glyph.
 */
export default function Sparkline({ data, color = "#22c55e", height = 60, strokeWidth = 2 }: Props) {
  // Map null/undefined/non-finite points to null (not 0) so Recharts breaks the
  // line into gaps instead of plotting fake dips to zero.
  const series = (data && data.length ? data : [0, 0]).map((v, i) => {
    const n = Number(v);
    return { i, v: v == null || !Number.isFinite(n) ? null : n };
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
          <Area
            type="monotone"
            dataKey="v"
            stroke={color}
            strokeWidth={strokeWidth}
            fill={`url(#${gradientId})`}
            isAnimationActive={false}
            dot={false}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
