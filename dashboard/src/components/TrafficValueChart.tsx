"use client";

import {
  ResponsiveContainer,
  ComposedChart,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  Bar,
  Line,
} from "recharts";

interface Point {
  date: string;
  clicks: number;
  revenue: number;
}

interface Props {
  data: Point[];
  compareData?: Point[];
  compareLabel?: string;
}

function alignSeries(data: Point[], compareData: Point[]) {
  const current = [...data].sort((a, b) => a.date.localeCompare(b.date));
  const compare = [...compareData].sort((a, b) => a.date.localeCompare(b.date));
  const maxLen = Math.max(current.length, compare.length);

  return Array.from({ length: maxLen }, (_, idx) => {
    const cur = current[idx];
    const cmp = compare[idx];
    const currentRpc = cur && cur.clicks > 0 ? cur.revenue / cur.clicks : null;
    const compareRpc = cmp && cmp.clicks > 0 ? cmp.revenue / cmp.clicks : null;

    return {
      day: `D${idx + 1}`,
      current_date: cur?.date,
      compare_date: cmp?.date,
      clicks: cur?.clicks ?? null,
      compare_clicks: cmp?.clicks ?? null,
      rpc: currentRpc,
      compare_rpc: compareRpc,
    };
  });
}

function fmtMoney(v: number | null | undefined): string {
  if (v == null) return "—";
  return `$${Number(v).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

export default function TrafficValueChart({ data, compareData = [], compareLabel = "" }: Props) {
  const chartData = alignSeries(data, compareData);
  const hasCompare = compareData.length > 0;

  return (
    <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-4">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-semibold text-gray-300">Traffic Volume vs Value</h3>
        <span className="text-[11px] text-gray-500">Clicks + RPC</span>
      </div>

      <div className="h-64">
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
            <XAxis dataKey="day" tick={{ fill: "#888", fontSize: 11 }} stroke="#1e1e2e" />
            <YAxis yAxisId="left" tick={{ fill: "#888", fontSize: 11 }} stroke="#1e1e2e" width={40} />
            <YAxis
              yAxisId="right"
              orientation="right"
              tick={{ fill: "#888", fontSize: 11 }}
              tickFormatter={(v) => `$${Number(v).toFixed(1)}`}
              stroke="#1e1e2e"
              width={48}
            />
            <Tooltip
              contentStyle={{
                border: "1px solid var(--card-border)",
                background: "var(--card)",
                borderRadius: 10,
                fontSize: 12,
              }}
              formatter={(value: number, name: string) => {
                if (name.toLowerCase().includes("rpc")) return [fmtMoney(value), name];
                return [Number(value).toLocaleString(), name];
              }}
              labelFormatter={(label, payload) => {
                const cur = payload?.[0]?.payload?.current_date;
                const cmp = payload?.[0]?.payload?.compare_date;
                if (!hasCompare) return `${label}${cur ? ` • ${cur}` : ""}`;
                return `${label}${cur ? ` • Current ${cur}` : ""}${cmp ? ` • Compare ${cmp}` : ""}`;
              }}
            />
            <Legend wrapperStyle={{ fontSize: 11 }} />
            <Bar yAxisId="left" dataKey="clicks" name="Clicks" fill="#3b82f6" radius={[4, 4, 0, 0]} />
            <Line yAxisId="right" type="monotone" dataKey="rpc" name="RPC" stroke="#22c55e" strokeWidth={2} dot={false} />
            {hasCompare && (
              <>
                <Bar
                  yAxisId="left"
                  dataKey="compare_clicks"
                  name={`Clicks (${compareLabel || "Compare"})`}
                  fill="#1e40af"
                  radius={[4, 4, 0, 0]}
                />
                <Line
                  yAxisId="right"
                  type="monotone"
                  dataKey="compare_rpc"
                  name={`RPC (${compareLabel || "Compare"})`}
                  stroke="#a78bfa"
                  strokeWidth={2}
                  dot={false}
                  strokeDasharray="4 4"
                />
              </>
            )}
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
