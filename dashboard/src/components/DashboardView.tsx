"use client";

import { useMemo } from "react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { Image as ImageIcon } from "lucide-react";
import { formatMoney, formatNumber, formatRatio } from "@/lib/utils";
import KpiCard from "./KpiCard";

type TsRow = {
  date: string;
  cost: number;
  revenue: number;
  profit: number;
  clicks: number;
  orders: number;
  roas: number | null;
  cvr: number | null;
};

interface Props {
  report: any;
  compareReport?: any;
  compareCaption?: string;
}

const COLORS = {
  green: "#22c55e",
  red: "#f43f5e",
  yellow: "#eab308",
  cyan: "#22d3ee",
  purple: "#8b5cf6",
};

function num(v: any): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}
function deltaPct(cur: number, prev: number | null | undefined): number | null {
  if (prev == null || prev === 0) return null;
  return ((cur - prev) / Math.abs(prev)) * 100;
}
function shortAxisDate(iso: string): string {
  const d = new Date(`${iso}T00:00:00`);
  return d
    .toLocaleDateString("en-US", { month: "short", day: "numeric" })
    .toUpperCase();
}

function trackedRevenue(s: any): number {
  return num(s?.tracked_revenue ?? s?.all_orders_revenue ?? s?.revenue);
}
function trackedOrders(s: any): number {
  return num(s?.tracked_orders ?? s?.all_orders_count ?? s?.orders);
}

function ChartTooltip({ active, payload, label, fmt }: any) {
  if (!active || !payload?.length) return null;
  return (
    <div className="rounded-lg border border-[var(--card-border)] bg-[#0c0c11] px-3 py-2 text-[11px] shadow-xl">
      <div className="mb-1 font-medium text-ink-dim">{label}</div>
      {payload.map((p: any, i: number) => (
        <div key={i} className="flex items-center gap-2">
          <span className="inline-block h-2 w-2 rounded-full" style={{ background: p.color || p.stroke }} />
          <span className="text-ink-dim">{p.name}:</span>
          <span className="tabular font-medium text-ink-bright">{fmt ? fmt(p.value) : p.value}</span>
        </div>
      ))}
    </div>
  );
}

function WidgetHeader({ title }: { title: string }) {
  return (
    <div className="mb-3 flex items-center justify-between">
      <div className="flex items-center gap-2">
        <ImageIcon size={15} className="text-ink-faint" />
        <span className="text-[15px] font-semibold text-ink-bright">{title}</span>
      </div>
    </div>
  );
}

function StatTile({
  label,
  value,
  delta,
  color,
}: {
  label: string;
  value: string;
  delta: number | null;
  color: string;
}) {
  const up = (delta ?? 0) >= 0;
  return (
    <div className="rounded-xl border border-[var(--card-border)] bg-[var(--surface)] p-3">
      <div className="flex items-center gap-1.5">
        <span className="h-2 w-2 rounded-full" style={{ background: color }} />
        {delta != null && (
          <span className={`text-[11px] font-semibold ${up ? "text-emerald-400" : "text-rose-400"}`}>
            {up ? "▲" : "▼"} {Math.abs(delta).toFixed(0)}%
          </span>
        )}
      </div>
      <div className="mt-2 h-label uppercase text-ink-dim">{label}</div>
      <div className="h-num mt-0.5 text-[20px]">{value}</div>
    </div>
  );
}

export default function DashboardView({ report, compareReport, compareCaption }: Props) {
  const ts: TsRow[] = report?.charts?.time_series || [];
  const sum = report?.summary_totals || {};
  const csum = compareReport?.summary_totals || null;
  const caption = compareCaption ? `from ${compareCaption}` : undefined;

  const derived = useMemo(() => {
    const cost = num(sum.cost);
    const revenue = trackedRevenue(sum);
    const orders = trackedOrders(sum);
    const roas = sum.blended_roas ?? sum.roas ?? (cost > 0 ? revenue / cost : null);
    const aov = sum.blended_aov ?? (orders > 0 ? revenue / orders : null);
    const totalLeads = (report?.funnels?.rows || []).reduce((a: number, r: any) => a + num(r.leads), 0);
    const cpl = totalLeads > 0 ? cost / totalLeads : sum.cpa ?? (orders > 0 ? cost / orders : null);
    const cac = sum.cac ?? (orders > 0 ? cost / orders : null);
    const clicks = num(sum.clicks);

    const cCost = csum ? num(csum.cost) : null;
    const cRevenue = csum ? trackedRevenue(csum) : null;
    const cOrders = csum ? trackedOrders(csum) : null;
    const cRoas = csum ? (csum.blended_roas ?? csum.roas ?? (cCost ? cRevenue! / cCost : null)) : null;
    const cAov = csum ? (csum.blended_aov ?? (cOrders ? cRevenue! / cOrders : null)) : null;
    const cClicks = csum ? num(csum.clicks) : null;
    const cCac = csum ? (csum.cac ?? (cOrders ? cCost! / cOrders : null)) : null;
    const cLeads = (compareReport?.funnels?.rows || []).reduce((a: number, r: any) => a + num(r.leads), 0);
    const cCpl = csum
      ? cLeads > 0
        ? cCost! / cLeads
        : csum.cpa ?? (cOrders ? cCost! / cOrders : null)
      : null;

    let running = 0;
    const cumulative = ts.map((r) => {
      running += num(r.revenue);
      return { date: r.date, label: shortAxisDate(r.date), v: Math.round(running * 100) / 100 };
    });
    const series = ts.map((r) => ({
      date: r.date,
      label: shortAxisDate(r.date),
      cost: num(r.cost),
      revenue: num(r.revenue),
      orders: num(r.orders),
      clicks: num(r.clicks),
      roas: r.roas == null ? 0 : num(r.roas),
      aov: num(r.orders) > 0 ? Math.round((num(r.revenue) / num(r.orders)) * 100) / 100 : 0,
      cpa: num(r.orders) > 0 ? Math.round((num(r.cost) / num(r.orders)) * 100) / 100 : 0,
    }));

    return {
      cost, revenue, orders, roas, aov, cpl, cac, clicks,
      cCost, cRevenue, cOrders, cRoas, cAov, cClicks, cCac, cCpl,
      cumulative, series,
    };
  }, [sum, csum, ts, report, compareReport]);

  if (!report) return null;
  const d = derived;

  return (
    <div className="space-y-5">
      {/* KPI cards */}
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-3 xl:grid-cols-5">
        <KpiCard
          label="Total Revenue"
          value={formatMoney(d.revenue)}
          deltaPct={deltaPct(d.revenue, d.cRevenue)}
          goodWhenUp
          caption={caption}
          data={d.series.map((r) => r.revenue)}
          color={COLORS.green}
        />
        <KpiCard
          label="Cost"
          value={formatMoney(d.cost)}
          deltaPct={deltaPct(d.cost, d.cCost)}
          goodWhenUp={false}
          caption={caption}
          data={d.series.map((r) => r.cost)}
          color={COLORS.red}
        />
        <KpiCard
          label="ROAS"
          value={d.roas == null ? "—" : formatRatio(d.roas).replace("x", "")}
          deltaPct={deltaPct(num(d.roas), d.cRoas == null ? null : num(d.cRoas))}
          goodWhenUp
          caption={caption}
          data={d.series.map((r) => r.roas)}
          color={COLORS.yellow}
        />
        <KpiCard
          label="Cost per Lead"
          value={d.cpl == null ? "—" : formatMoney(num(d.cpl))}
          deltaPct={deltaPct(num(d.cpl), d.cCpl == null ? null : num(d.cCpl))}
          goodWhenUp={false}
          caption={caption}
          data={d.series.map((r) => r.cpa)}
          color={COLORS.red}
        />
        <KpiCard
          label="NET CAC"
          value={d.cac == null ? "—" : formatMoney(num(d.cac))}
          deltaPct={deltaPct(num(d.cac), d.cCac == null ? null : num(d.cCac))}
          goodWhenUp={false}
          caption={caption}
          data={d.series.map((r) => r.cpa)}
          color={COLORS.red}
        />
      </div>

      {/* Widget row */}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
        {/* New Customers */}
        <div className="hpanel group p-4 transition-colors hover:border-white/10">
          <WidgetHeader title="New Customers" />
          <div className="mb-1 flex items-baseline gap-2">
            <span className="h-num text-[34px]">{formatNumber(d.orders)}</span>
            {deltaPct(d.orders, d.cOrders) != null && (
              <span
                className={`text-[12px] font-semibold ${
                  d.orders >= (d.cOrders ?? 0) ? "text-emerald-400" : "text-rose-400"
                }`}
              >
                {d.orders >= (d.cOrders ?? 0) ? "▲" : "▼"} {Math.abs(deltaPct(d.orders, d.cOrders)!).toFixed(0)}%
              </span>
            )}
          </div>
          <div className="h-[180px]">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={d.series} margin={{ top: 8, right: 6, left: -4, bottom: 0 }}>
                <defs>
                  <linearGradient id="g-cust" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor={COLORS.purple} stopOpacity={0.4} />
                    <stop offset="100%" stopColor={COLORS.purple} stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false} />
                <XAxis dataKey="label" tick={{ fill: "#595c68", fontSize: 10 }} tickLine={false} axisLine={false} minTickGap={24} />
                <YAxis tick={{ fill: "#595c68", fontSize: 10 }} tickLine={false} axisLine={false} width={42} />
                <Tooltip content={<ChartTooltip fmt={formatNumber} />} />
                <Area type="monotone" name="New Customers" dataKey="orders" stroke={COLORS.purple} strokeWidth={2} fill="url(#g-cust)" isAnimationActive={false} dot={false} />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Clicks + Sales */}
        <div className="hpanel group p-4 transition-colors hover:border-white/10">
          <WidgetHeader title="Clicks + Sales" />
          <div className="h-[150px]">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={d.series} margin={{ top: 8, right: 6, left: -4, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false} />
                <XAxis dataKey="label" tick={{ fill: "#595c68", fontSize: 10 }} tickLine={false} axisLine={false} minTickGap={24} />
                <YAxis tick={{ fill: "#595c68", fontSize: 10 }} tickLine={false} axisLine={false} width={42} />
                <Tooltip content={<ChartTooltip fmt={formatNumber} />} />
                <Line type="monotone" name="Clicks" dataKey="clicks" stroke={COLORS.purple} strokeWidth={2} dot={false} isAnimationActive={false} />
                <Line type="monotone" name="Sales" dataKey="orders" stroke="#c4b5fd" strokeWidth={2} dot={false} isAnimationActive={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
          <div className="mt-3 grid grid-cols-2 gap-3">
            <StatTile label="Clicks" value={formatNumber(d.clicks)} delta={deltaPct(d.clicks, d.cClicks)} color={COLORS.purple} />
            <StatTile label="Sales" value={formatNumber(d.orders)} delta={deltaPct(d.orders, d.cOrders)} color="#c4b5fd" />
          </div>
        </div>

        {/* Average Order Value */}
        <div className="hpanel group p-4 transition-colors hover:border-white/10">
          <WidgetHeader title="Average Order Value" />
          <div className="mb-1 flex items-baseline gap-2">
            <span className="h-num text-[26px]">{d.aov == null ? "—" : formatMoney(num(d.aov))}</span>
            {deltaPct(num(d.aov), d.cAov == null ? null : num(d.cAov)) != null && (
              <span
                className={`text-[12px] font-semibold ${
                  num(d.aov) >= num(d.cAov) ? "text-emerald-400" : "text-rose-400"
                }`}
              >
                {num(d.aov) >= num(d.cAov) ? "▲" : "▼"}{" "}
                {Math.abs(deltaPct(num(d.aov), d.cAov == null ? null : num(d.cAov))!).toFixed(0)}%
              </span>
            )}
          </div>
          <div className="h-[150px]">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={d.series} margin={{ top: 8, right: 6, left: -4, bottom: 0 }}>
                <defs>
                  <linearGradient id="g-aov" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor={COLORS.cyan} stopOpacity={0.4} />
                    <stop offset="100%" stopColor={COLORS.cyan} stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false} />
                <XAxis dataKey="label" tick={{ fill: "#595c68", fontSize: 10 }} tickLine={false} axisLine={false} minTickGap={24} />
                <YAxis tick={{ fill: "#595c68", fontSize: 10 }} tickLine={false} axisLine={false} width={42} />
                <Tooltip content={<ChartTooltip fmt={formatMoney} />} />
                <Area type="monotone" name="AOV" dataKey="aov" stroke={COLORS.cyan} strokeWidth={2} fill="url(#g-aov)" isAnimationActive={false} dot={false} />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* Cumulative revenue (LTV-style) */}
      <div className="hpanel group p-4">
        <WidgetHeader title="Cumulative Revenue" />
        <div className="h-[260px]">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={d.cumulative} margin={{ top: 8, right: 8, left: 4, bottom: 0 }}>
              <defs>
                <linearGradient id="g-ltv" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={COLORS.green} stopOpacity={0.35} />
                  <stop offset="100%" stopColor={COLORS.green} stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false} />
              <XAxis dataKey="label" tick={{ fill: "#595c68", fontSize: 10 }} tickLine={false} axisLine={false} minTickGap={32} />
              <YAxis tick={{ fill: "#595c68", fontSize: 10 }} tickLine={false} axisLine={false} width={58} tickFormatter={(v) => formatMoney(v)} />
              <Tooltip content={<ChartTooltip fmt={formatMoney} />} />
              <Area type="monotone" name="Cumulative" dataKey="v" stroke={COLORS.green} strokeWidth={2.5} fill="url(#g-ltv)" isAnimationActive={false} dot={false} />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}
