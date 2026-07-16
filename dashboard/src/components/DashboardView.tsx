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
import { AlertTriangle, Image as ImageIcon } from "lucide-react";
import { formatMoney, formatMoneyCompact, formatNumber, formatRatio } from "@/lib/utils";
import KpiCard from "./KpiCard";

type TsRow = {
  date: string;
  cost: number;
  revenue: number;
  tracked_revenue?: number;
  profit: number;
  clicks: number;
  orders: number;
  tracked_orders?: number;
  new_customers?: number;
  roas: number | null;
  blended_roas?: number | null;
  cvr: number | null;
};

interface Props {
  report: any;
  compareReport?: any;
  currentRangeCaption?: string;
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

function rangeStartLabel(iso: string | null | undefined): string | undefined {
  if (!iso) return undefined;
  const [year, month, day] = iso.split("-").map(Number);
  if (!year || !month || !day) return undefined;
  return new Date(year, month - 1, day).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
  });
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

export default function DashboardView({ report, compareReport, currentRangeCaption }: Props) {
  const ts: TsRow[] = report?.charts?.time_series || [];
  const sum = report?.summary_totals || {};
  const csum = compareReport?.summary_totals || null;
  // Prefer the range echoed by the response. This keeps labels attached to the
  // exact dataset currently rendered if a new request is still in flight.
  const responseRangeStart = rangeStartLabel(report?.report_meta?.date_range?.start);
  const captionStart = responseRangeStart || currentRangeCaption;
  const caption = captionStart ? `from ${captionStart}` : undefined;

  const derived = useMemo(() => {
    const cost = num(sum.cost);
    const revenue = trackedRevenue(sum);
    // HYROS "Sales" = non-renewal, non-refunded sale groups (validated 214/214
    // against a HYROS export); fall back to raw order count on older payloads.
    const orders = num(sum.hyros_sales_count ?? trackedOrders(sum));
    const roas = sum.blended_roas ?? sum.roas ?? (cost > 0 ? revenue / cost : null);
    // HYROS AOV = revenue / ALL sale groups (incl. refunded + unattributed).
    // The old source_aov (attribution-filtered) inflated AOV ~31%.
    const aov = sum.all_orders_aov ?? sum.source_aov ?? null;
    const clicks = num(sum.clicks);

    // HYROS "New Customers" = non-renewal sale groups, refunds included
    // (validated 236/236 against a HYROS export).
    const newCustomers = num(sum.hyros_new_customers ?? sum.new_customers ?? orders);
    // Leads come from lead/opt-in conversions; fall back to the funnel snapshot.
    const funnelLeads = (report?.funnels?.rows || []).reduce((a: number, r: any) => a + num(r.leads), 0);
    const leads = sum.leads != null ? num(sum.leads) : funnelLeads;
    // Cost per Lead = spend / leads. NET CAC = spend / net-new customers. Both are
    // distinct from CPA (spend / orders) — the backend now provides them directly.
    const cpl = sum.cpl != null ? num(sum.cpl) : leads > 0 ? cost / leads : null;
    const cac = sum.cac != null ? num(sum.cac) : newCustomers > 0 ? cost / newCustomers : null;

    const cCost = csum ? num(csum.cost) : null;
    const cRevenue = csum ? trackedRevenue(csum) : null;
    const cOrders = csum ? num(csum.hyros_sales_count ?? trackedOrders(csum)) : null;
    const cRoas = csum ? (csum.blended_roas ?? csum.roas ?? (cCost ? cRevenue! / cCost : null)) : null;
    const cAov = csum ? (csum.all_orders_aov ?? csum.source_aov ?? null) : null;
    const cClicks = csum ? num(csum.clicks) : null;
    const cNewCustomers = csum ? num(csum.hyros_new_customers ?? csum.new_customers ?? cOrders ?? 0) : null;
    const cLeadsFunnel = (compareReport?.funnels?.rows || []).reduce((a: number, r: any) => a + num(r.leads), 0);
    const cLeads = csum ? (csum.leads != null ? num(csum.leads) : cLeadsFunnel) : null;
    const cCac = csum
      ? csum.cac != null
        ? num(csum.cac)
        : cNewCustomers && cNewCustomers > 0
        ? cCost! / cNewCustomers
        : null
      : null;
    const cCpl = csum
      ? csum.cpl != null
        ? num(csum.cpl)
        : cLeads && cLeads > 0
        ? cCost! / cLeads
        : null
      : null;

    // Charts use tracked (all-orders) per-day figures so they match the headline
    // tracked numbers even when attribution is incomplete (falling back to
    // attributed values when tracked-by-day isn't available).
    const dayRevenue = (r: any) => num(r.tracked_revenue ?? r.revenue);
    const dayOrders = (r: any) => num(r.tracked_orders ?? r.orders);

    let running = 0;
    const cumulative = ts.map((r) => {
      running += dayRevenue(r);
      return { date: r.date, label: shortAxisDate(r.date), v: Math.round(running * 100) / 100 };
    });
    const series = ts.map((r) => {
      const rev = dayRevenue(r);
      const ord = dayOrders(r);
      const dayCost = num(r.cost);
      const dayLeads = num((r as any).leads ?? 0);
      const dayNewCustomers = num((r as any).new_customers ?? 0);
      const dayNetNewCustomers = num((r as any).net_new_customers ?? 0);
      return {
        date: r.date,
        label: shortAxisDate(r.date),
        cost: dayCost,
        revenue: rev,
        orders: ord,
        newCustomers: dayNewCustomers,
        leads: dayLeads,
        clicks: num(r.clicks),
        roas: num((r as any).blended_roas ?? r.roas ?? 0),
        aov: (r as any).source_aov == null ? null : num((r as any).source_aov),
        cpa: ord > 0 ? Math.round((dayCost / ord) * 100) / 100 : 0,
        // Cost per Lead = spend / leads. NET CAC = spend / net_new_customers —
        // the per-day deduplicated first-time buyer count, matching the headline
        // net_cac denominator (new_customers counts non-recurring sales and
        // would disagree with it). Both are distinct from CPA (spend / orders).
        cpl: dayLeads > 0 ? Math.round((dayCost / dayLeads) * 100) / 100 : null,
        cac: dayNetNewCustomers > 0 ? Math.round((dayCost / dayNetNewCustomers) * 100) / 100 : null,
      };
    });

    return {
      cost, revenue, orders, roas, aov, cpl, cac, clicks, newCustomers, leads,
      cCost, cRevenue, cOrders, cRoas, cAov, cClicks, cCac, cCpl, cNewCustomers, cLeads,
      cumulative, series,
    };
  }, [sum, csum, ts, report, compareReport]);

  if (!report) return null;
  const d = derived;
  const dataErrors = report?.diagnostics?.data_errors || [];

  return (
    <div className="space-y-5">
      {dataErrors.length > 0 && (
        <div className="flex items-start gap-2 rounded-xl border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-[13px] leading-relaxed text-amber-200">
          <AlertTriangle size={16} className="mt-0.5 flex-shrink-0" />
          <div>
            <span className="font-semibold">A data error occurred while building this report</span> — some
            numbers may be missing or shown as zero. Check the backend logs.
          </div>
        </div>
      )}
      {/* KPI cards */}
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-3 xl:grid-cols-5">
        <KpiCard
          label="Total Revenue"
          value={formatMoney(d.revenue)}
          deltaPct={deltaPct(d.revenue, d.cRevenue)}
          goodWhenUp
          caption={caption}
          data={d.series.map((r) => r.revenue)}
          labels={d.series.map((r) => r.label)}
          valueFormatter={formatMoney}
          color={COLORS.green}
        />
        <KpiCard
          label="Cost"
          value={formatMoney(d.cost)}
          deltaPct={deltaPct(d.cost, d.cCost)}
          goodWhenUp={false}
          caption={caption}
          data={d.series.map((r) => r.cost)}
          labels={d.series.map((r) => r.label)}
          valueFormatter={formatMoney}
          color={COLORS.red}
        />
        <KpiCard
          label="ROAS"
          value={d.roas == null ? "—" : formatRatio(d.roas).replace("x", "")}
          deltaPct={deltaPct(num(d.roas), d.cRoas == null ? null : num(d.cRoas))}
          goodWhenUp
          caption={caption}
          data={d.series.map((r) => r.roas)}
          labels={d.series.map((r) => r.label)}
          valueFormatter={(v) => formatRatio(v)}
          color={COLORS.yellow}
        />
        <KpiCard
          label="Cost per Lead"
          value={d.cpl == null ? "—" : formatMoney(num(d.cpl))}
          deltaPct={deltaPct(num(d.cpl), d.cCpl == null ? null : num(d.cCpl))}
          goodWhenUp={false}
          caption={caption}
          data={d.series.map((r) => r.cpl)}
          labels={d.series.map((r) => r.label)}
          valueFormatter={formatMoney}
          color={COLORS.red}
        />
        <KpiCard
          label="NET CAC"
          value={d.cac == null ? "—" : formatMoney(num(d.cac))}
          deltaPct={deltaPct(num(d.cac), d.cCac == null ? null : num(d.cCac))}
          goodWhenUp={false}
          caption={caption}
          data={d.series.map((r) => r.cac)}
          labels={d.series.map((r) => r.label)}
          valueFormatter={formatMoney}
          color={COLORS.red}
        />
      </div>

      {/* Widget row */}
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
        {/* New Customers */}
        <div className="hpanel group p-4 transition-colors hover:border-white/10">
          <WidgetHeader title="New Customers" />
          <div className="mb-1 flex items-baseline gap-2">
            <span className="h-num text-[34px]">{formatNumber(d.newCustomers)}</span>
            {deltaPct(d.newCustomers, d.cNewCustomers) != null && (
              <span
                className={`text-[12px] font-semibold ${
                  d.newCustomers >= (d.cNewCustomers ?? 0) ? "text-emerald-400" : "text-rose-400"
                }`}
              >
                {d.newCustomers >= (d.cNewCustomers ?? 0) ? "▲" : "▼"} {Math.abs(deltaPct(d.newCustomers, d.cNewCustomers)!).toFixed(0)}%
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
                <Area type="monotone" name="New Customers" dataKey="newCustomers" stroke={COLORS.purple} strokeWidth={2} fill="url(#g-cust)" isAnimationActive={false} dot={false} />
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
              <YAxis tick={{ fill: "#595c68", fontSize: 10 }} tickLine={false} axisLine={false} width={58} tickFormatter={(v) => formatMoneyCompact(v)} />
              <Tooltip content={<ChartTooltip fmt={formatMoney} />} />
              <Area type="monotone" name="Cumulative" dataKey="v" stroke={COLORS.green} strokeWidth={2.5} fill="url(#g-ltv)" isAnimationActive={false} dot={false} />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}
