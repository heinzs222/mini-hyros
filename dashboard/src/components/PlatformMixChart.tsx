"use client";

import {
  ResponsiveContainer,
  BarChart,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  Bar,
} from "recharts";

interface PlatformRow {
  platform: string;
  label: string;
  clicks: number;
  cost: number;
  revenue: number;
  profit: number;
  clicks_share: number;
  revenue_share: number;
}

interface Props {
  rows: PlatformRow[];
  compareRows?: PlatformRow[];
  compareLabel?: string;
}

function buildRows(rows: PlatformRow[], compareRows: PlatformRow[]) {
  const compareByPlatform = new Map(compareRows.map((r) => [r.platform, r]));

  return rows.map((row) => {
    const cmp = compareByPlatform.get(row.platform);
    return {
      platform: row.label,
      revenue: row.revenue,
      cost: row.cost,
      profit: row.profit,
      compare_revenue: cmp?.revenue ?? null,
      compare_profit: cmp?.profit ?? null,
      revenue_share: row.revenue_share,
    };
  });
}

export default function PlatformMixChart({ rows, compareRows = [], compareLabel = "" }: Props) {
  const chartRows = buildRows(rows, compareRows);
  const hasCompare = compareRows.length > 0;

  return (
    <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-4">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h3 className="text-sm font-semibold text-gray-300">Platform Revenue & Profit Mix</h3>
          <p className="text-[11px] text-gray-500 mt-0.5">Quickly compare where spend returns the most value.</p>
        </div>
      </div>

      {chartRows.length === 0 ? (
        <div className="h-48 flex items-center justify-center text-sm text-gray-600">No platform mix data yet.</div>
      ) : (
        <div className="h-72">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={chartRows} margin={{ top: 10, right: 14, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
              <XAxis dataKey="platform" tick={{ fill: "#888", fontSize: 11 }} stroke="#1e1e2e" />
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
                formatter={(value: number, name: string, item: any) => {
                  if (name === "Revenue Share") return [`${Number(value).toFixed(1)}%`, name];
                  return [`$${Number(value).toLocaleString(undefined, { maximumFractionDigits: 2 })}`, name];
                }}
                labelFormatter={(label, payload) => {
                  const row = payload?.[0]?.payload;
                  if (!row) return label;
                  return `${label} • Revenue Share ${Number(row.revenue_share || 0).toFixed(1)}%`;
                }}
              />
              <Legend wrapperStyle={{ fontSize: 11 }} />

              <Bar dataKey="revenue" name="Revenue" fill="#22c55e" radius={[4, 4, 0, 0]} />
              <Bar dataKey="profit" name="Profit" fill="#6366f1" radius={[4, 4, 0, 0]} />
              {hasCompare && (
                <>
                  <Bar
                    dataKey="compare_revenue"
                    name={`Revenue (${compareLabel || "Compare"})`}
                    fill="#60a5fa"
                    radius={[4, 4, 0, 0]}
                  />
                  <Bar
                    dataKey="compare_profit"
                    name={`Profit (${compareLabel || "Compare"})`}
                    fill="#a78bfa"
                    radius={[4, 4, 0, 0]}
                  />
                </>
              )}
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}
