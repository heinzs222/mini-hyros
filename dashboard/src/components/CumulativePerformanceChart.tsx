"use client";

import {
  ResponsiveContainer,
  LineChart,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  Line,
} from "recharts";

interface Point {
  date: string;
  revenue: number;
  cost: number;
  profit: number;
}

interface Props {
  data: Point[];
  compareData?: Point[];
  compareLabel?: string;
}

function buildCumulative(points: Point[]) {
  const sorted = [...points].sort((a, b) => a.date.localeCompare(b.date));
  let revenue = 0;
  let cost = 0;
  let profit = 0;

  return sorted.map((p) => {
    revenue += Number(p.revenue || 0);
    cost += Number(p.cost || 0);
    profit += Number(p.profit || 0);
    return {
      date: p.date,
      revenue,
      cost,
      profit,
    };
  });
}

function align(current: Point[], compare: Point[]) {
  const cur = buildCumulative(current);
  const cmp = buildCumulative(compare);
  const maxLen = Math.max(cur.length, cmp.length);

  return Array.from({ length: maxLen }, (_, idx) => {
    const c = cur[idx];
    const p = cmp[idx];
    return {
      day: `D${idx + 1}`,
      current_date: c?.date,
      compare_date: p?.date,
      cumulative_revenue: c?.revenue ?? null,
      cumulative_cost: c?.cost ?? null,
      cumulative_profit: c?.profit ?? null,
      compare_revenue: p?.revenue ?? null,
      compare_profit: p?.profit ?? null,
    };
  });
}

export default function CumulativePerformanceChart({ data, compareData = [], compareLabel = "" }: Props) {
  const chartData = align(data, compareData);
  const hasCompare = compareData.length > 0;

  return (
    <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-4">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-semibold text-gray-300">Cumulative Performance</h3>
        <span className="text-[11px] text-gray-500">Revenue, cost, and profit trajectory</span>
      </div>

      <div className="h-64">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
            <XAxis dataKey="day" tick={{ fill: "#888", fontSize: 11 }} stroke="#1e1e2e" />
            <YAxis
              tick={{ fill: "#888", fontSize: 11 }}
              tickFormatter={(v) => `$${(Number(v) / 1000).toFixed(0)}K`}
              stroke="#1e1e2e"
              width={54}
            />
            <Tooltip
              contentStyle={{
                border: "1px solid var(--card-border)",
                background: "var(--card)",
                borderRadius: 10,
                fontSize: 12,
              }}
              formatter={(value: number) => [`$${Number(value).toLocaleString(undefined, { maximumFractionDigits: 2 })}`]}
              labelFormatter={(label, payload) => {
                const cur = payload?.[0]?.payload?.current_date;
                const cmp = payload?.[0]?.payload?.compare_date;
                if (!hasCompare) return `${label}${cur ? ` • ${cur}` : ""}`;
                return `${label}${cur ? ` • Current ${cur}` : ""}${cmp ? ` • Compare ${cmp}` : ""}`;
              }}
            />
            <Legend wrapperStyle={{ fontSize: 11 }} />
            <Line type="monotone" dataKey="cumulative_revenue" name="Cum Revenue" stroke="#22c55e" strokeWidth={2} dot={false} />
            <Line type="monotone" dataKey="cumulative_cost" name="Cum Cost" stroke="#ef4444" strokeWidth={2} dot={false} />
            <Line type="monotone" dataKey="cumulative_profit" name="Cum Profit" stroke="#6366f1" strokeWidth={2} dot={false} />
            {hasCompare && (
              <>
                <Line
                  type="monotone"
                  dataKey="compare_revenue"
                  name={`Cum Revenue (${compareLabel || "Compare"})`}
                  stroke="#60a5fa"
                  strokeWidth={2}
                  dot={false}
                  strokeDasharray="4 4"
                />
                <Line
                  type="monotone"
                  dataKey="compare_profit"
                  name={`Cum Profit (${compareLabel || "Compare"})`}
                  stroke="#a78bfa"
                  strokeWidth={2}
                  dot={false}
                  strokeDasharray="4 4"
                />
              </>
            )}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
