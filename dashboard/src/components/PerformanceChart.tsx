"use client";

import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";

interface DataPoint {
  date: string;
  cost: number;
  revenue: number;
  profit: number;
  clicks: number;
}

interface Props {
  data: DataPoint[];
  compareData?: DataPoint[];
  compareLabel?: string;
}

function buildAlignedSeries(data: DataPoint[], compareData: DataPoint[]) {
  const current = [...data].sort((a, b) => a.date.localeCompare(b.date));
  const compare = [...compareData].sort((a, b) => a.date.localeCompare(b.date));
  const maxLen = Math.max(current.length, compare.length);

  return Array.from({ length: maxLen }, (_, idx) => {
    const cur = current[idx];
    const cmp = compare[idx];
    return {
      day: `D${idx + 1}`,
      current_date: cur?.date,
      compare_date: cmp?.date,
      revenue: cur?.revenue ?? null,
      cost: cur?.cost ?? null,
      profit: cur?.profit ?? null,
      compare_revenue: cmp?.revenue ?? null,
      compare_profit: cmp?.profit ?? null,
    };
  });
}

function CustomTooltip({ active, payload, label, compareLabel }: any) {
  if (!active || !payload) return null;

  const currentDate = payload.find((p: any) => p.payload?.current_date)?.payload?.current_date;
  const comparisonDate = payload.find((p: any) => p.payload?.compare_date)?.payload?.compare_date;

  return (
    <div className="rounded-lg border border-[var(--card-border)] bg-[var(--card)] p-3 text-xs shadow-xl">
      <p className="text-gray-400 mb-1">{label}</p>
      {currentDate && <p className="text-[10px] text-gray-500">Current: {currentDate}</p>}
      {comparisonDate && compareLabel && (
        <p className="text-[10px] text-blue-400">Compare ({compareLabel}): {comparisonDate}</p>
      )}
      {payload.map((p: any) => (
        <p key={p.name} style={{ color: p.color }}>
          {p.name}: ${Number(p.value).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
        </p>
      ))}
    </div>
  );
}

export default function PerformanceChart({ data, compareData = [], compareLabel = "" }: Props) {
  const chartData = buildAlignedSeries(data, compareData);

  return (
    <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-4">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-semibold text-gray-300">Revenue vs Cost vs Profit</h3>
        {compareData.length > 0 && (
          <span className="text-[11px] px-2 py-1 rounded bg-blue-500/10 text-blue-300 border border-blue-500/25">
            Comparison: {compareLabel || "Enabled"}
          </span>
        )}
      </div>
      <div className="h-72">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={chartData} margin={{ top: 5, right: 10, left: 0, bottom: 0 }}>
            <defs>
              <linearGradient id="colorRevenue" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#22c55e" stopOpacity={0.3} />
                <stop offset="95%" stopColor="#22c55e" stopOpacity={0} />
              </linearGradient>
              <linearGradient id="colorCost" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#ef4444" stopOpacity={0.3} />
                <stop offset="95%" stopColor="#ef4444" stopOpacity={0} />
              </linearGradient>
              <linearGradient id="colorProfit" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#6366f1" stopOpacity={0.3} />
                <stop offset="95%" stopColor="#6366f1" stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
            <XAxis
              dataKey="day"
              tick={{ fill: "#888", fontSize: 11 }}
              stroke="#1e1e2e"
            />
            <YAxis
              tick={{ fill: "#888", fontSize: 11 }}
              tickFormatter={(v) => `$${(v / 1000).toFixed(0)}K`}
              stroke="#1e1e2e"
              width={55}
            />
            <Tooltip content={<CustomTooltip compareLabel={compareLabel} />} />
            <Legend
              wrapperStyle={{ fontSize: 11, color: "#888" }}
            />
            <Area
              type="monotone"
              dataKey="revenue"
              stroke="#22c55e"
              fill="url(#colorRevenue)"
              strokeWidth={2}
              name="Revenue"
            />
            <Area
              type="monotone"
              dataKey="cost"
              stroke="#ef4444"
              fill="url(#colorCost)"
              strokeWidth={2}
              name="Cost"
            />
            <Area
              type="monotone"
              dataKey="profit"
              stroke="#6366f1"
              fill="url(#colorProfit)"
              strokeWidth={2}
              name="Profit"
            />
            {compareData.length > 0 && (
              <>
                <Area
                  type="monotone"
                  dataKey="compare_revenue"
                  stroke="#60a5fa"
                  fill="none"
                  strokeDasharray="4 4"
                  strokeWidth={2}
                  name="Revenue (Compare)"
                />
                <Area
                  type="monotone"
                  dataKey="compare_profit"
                  stroke="#c084fc"
                  fill="none"
                  strokeDasharray="4 4"
                  strokeWidth={2}
                  name="Profit (Compare)"
                />
              </>
            )}
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
