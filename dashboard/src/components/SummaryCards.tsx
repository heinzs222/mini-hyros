"use client";

import { formatMoney, formatNumber, formatPercentValue, formatRatio, profitColor } from "@/lib/utils";
import {
  DollarSign,
  TrendingUp,
  ShoppingCart,
  Percent,
  MousePointerClick,
  Target,
  Gauge,
  TrendingDown,
} from "lucide-react";

type SummaryTotals = {
  clicks: number;
  impressions?: number;
  orders: number;
  cost: number;
  cpc: number | null;
  cpm?: number | null;
  ctr?: number | null;
  cpa: number | null;
  cvr: number | null;
  revenue: number;
  total_revenue: number;
  aov: number | null;
  rpc: number | null;
  profit: number;
  margin_pct: number | null;
  net_profit: number;
  roas: number | null;
  mer: number | null;
  cac: number | null;
  reported: number | null;
  reported_delta: number | null;
  all_orders_count?: number;
  all_orders_revenue?: number;
  blended_roas?: number | null;
  blended_cvr?: number | null;
  blended_aov?: number | null;
  blended_profit?: number | null;
  blended_cpa?: number | null;
};

interface Props {
  totals: SummaryTotals;
  compareTotals?: SummaryTotals | null;
  compareLabel?: string;
  showCompareBanner?: boolean;
}

function deltaColor(delta: number | null | undefined): string {
  if (delta == null) return "text-gray-500";
  if (delta > 0) return "text-emerald-400";
  if (delta < 0) return "text-red-400";
  return "text-gray-500";
}

function moneyDelta(current: number | null | undefined, previous: number | null | undefined): string | undefined {
  if (current == null || previous == null) return undefined;
  const diff = current - previous;
  const sign = diff > 0 ? "+" : diff < 0 ? "-" : "";
  const absMoney = formatMoney(Math.abs(diff));
  return `${sign}${absMoney}`;
}

function numberDelta(current: number | null | undefined, previous: number | null | undefined): string | undefined {
  if (current == null || previous == null) return undefined;
  const diff = current - previous;
  const sign = diff > 0 ? "+" : diff < 0 ? "-" : "";
  return `${sign}${formatNumber(Math.abs(diff))}`;
}

function ratioDelta(current: number | null | undefined, previous: number | null | undefined): string | undefined {
  if (current == null || previous == null) return undefined;
  const diff = current - previous;
  const sign = diff > 0 ? "+" : diff < 0 ? "-" : "";
  return `${sign}${Math.abs(diff).toFixed(2)}x`;
}

function percentPointDelta(current: number | null | undefined, previous: number | null | undefined): string | undefined {
  if (current == null || previous == null) return undefined;
  const diff = current - previous;
  const sign = diff > 0 ? "+" : diff < 0 ? "-" : "";
  return `${sign}${Math.abs(diff).toFixed(2)} pp`;
}

function Card({
  label,
  value,
  sub,
  delta,
  deltaClass,
  icon,
  colorClass,
}: {
  label: string;
  value: string;
  sub?: string;
  delta?: string;
  deltaClass?: string;
  icon: React.ReactNode;
  colorClass?: string;
}) {
  return (
    <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-4 flex flex-col gap-1.5 min-w-0">
      <div className="flex items-center gap-2 text-[11px] text-gray-400 uppercase tracking-wide">
        {icon}
        {label}
      </div>
      <div className={`text-xl md:text-2xl font-bold truncate ${colorClass || "text-white"}`}>
        {value}
      </div>
      {sub && <div className="text-[11px] text-gray-500">{sub}</div>}
      {delta && <div className={`text-[10px] ${deltaClass || "text-gray-500"}`}>Δ {delta}</div>}
    </div>
  );
}

export default function SummaryCards({ totals, compareTotals, compareLabel, showCompareBanner = true }: Props) {
  const delta = totals.reported_delta;

  return (
    <div className="space-y-2">
      {showCompareBanner && compareTotals && (
        <div className="rounded-lg border border-[var(--card-border)] bg-white/[0.02] px-3 py-2 text-[11px] text-gray-400">
          Comparison baseline: {compareLabel || "Selected comparison"}
        </div>
      )}

      <div className="grid grid-cols-2 md:grid-cols-4 xl:grid-cols-8 gap-3">
        <Card
          label="Ad Spend"
          value={formatMoney(totals.cost)}
          sub={`CPC: ${formatMoney(totals.cpc)} | CPA: ${formatMoney(totals.cpa ?? totals.cac)}`}
          delta={moneyDelta(totals.cost, compareTotals?.cost)}
          deltaClass={deltaColor((totals.cost ?? 0) - (compareTotals?.cost ?? 0))}
          icon={<DollarSign size={14} />}
        />
        <Card
          label={totals.revenue > 0 ? "Revenue (Attr.)" : "Revenue (Stripe)"}
          value={formatMoney(totals.revenue > 0 ? totals.revenue : (totals.all_orders_revenue ?? 0))}
          sub={totals.revenue > 0
            ? `All Orders: ${formatMoney(totals.all_orders_revenue)} (${totals.all_orders_count ?? 0})`
            : `${totals.all_orders_count ?? 0} orders · attr: $0`}
          delta={moneyDelta(totals.revenue > 0 ? totals.revenue : (totals.all_orders_revenue ?? 0), compareTotals?.revenue)}
          deltaClass={deltaColor((totals.revenue ?? 0) - (compareTotals?.revenue ?? 0))}
          icon={<TrendingUp size={14} />}
          colorClass="text-emerald-400"
        />
        <Card
          label={totals.blended_profit != null && totals.revenue === 0 ? "Profit (Blended)" : "Profit"}
          value={formatMoney(totals.revenue === 0 && totals.blended_profit != null ? totals.blended_profit : totals.profit)}
          sub={totals.revenue === 0 && totals.blended_profit != null
            ? `Stripe rev − ad spend`
            : `Margin: ${formatPercentValue(totals.margin_pct)} | Net: ${formatMoney(totals.net_profit)}`}
          delta={moneyDelta(totals.profit, compareTotals?.profit)}
          deltaClass={deltaColor((totals.profit ?? 0) - (compareTotals?.profit ?? 0))}
          icon={<TrendingDown size={14} />}
          colorClass={profitColor(totals.revenue === 0 && totals.blended_profit != null ? totals.blended_profit : totals.profit)}
        />
        <Card
          label={totals.roas ? "ROAS" : "ROAS (Blended)"}
          value={formatRatio(totals.roas || totals.blended_roas)}
          sub={`MER: ${formatRatio(totals.mer)}`}
          delta={ratioDelta(totals.roas, compareTotals?.roas)}
          deltaClass={deltaColor((totals.roas ?? 0) - (compareTotals?.roas ?? 0))}
          icon={<Target size={14} />}
          colorClass={
            (totals.roas || totals.blended_roas || 0) >= 1
              ? "text-emerald-400"
              : "text-red-400"
          }
        />
        <Card
          label="Clicks"
          value={formatNumber(totals.clicks)}
          sub={totals.impressions ? `Impr: ${formatNumber(totals.impressions)} | CTR: ${formatPercentValue(totals.ctr ?? null)}` : `Orders: ${formatNumber(totals.orders)} | CVR: ${formatPercentValue(totals.cvr)}`}
          delta={numberDelta(totals.clicks, compareTotals?.clicks)}
          deltaClass={deltaColor((totals.clicks ?? 0) - (compareTotals?.clicks ?? 0))}
          icon={<MousePointerClick size={14} />}
        />
        <Card
          label={totals.orders > 0 ? "Orders" : "Orders (Stripe)"}
          value={formatNumber(totals.orders > 0 ? totals.orders : (totals.all_orders_count ?? 0))}
          sub={totals.orders > 0
            ? `AOV: ${formatMoney(totals.aov)}`
            : `AOV: ${formatMoney(totals.blended_aov)} · CPA: ${formatMoney(totals.blended_cpa)}`}
          delta={numberDelta(totals.orders, compareTotals?.orders)}
          deltaClass={deltaColor((totals.orders ?? 0) - (compareTotals?.orders ?? 0))}
          icon={<ShoppingCart size={14} />}
        />
        <Card
          label={totals.cvr ? "CVR" : "CVR (Blended)"}
          value={formatPercentValue(totals.cvr || totals.blended_cvr)}
          sub={totals.cvr ? `RPC: ${formatMoney(totals.rpc)}` : `${formatNumber(totals.all_orders_count ?? 0)} orders / ${formatNumber(totals.clicks)} clicks`}
          delta={percentPointDelta(totals.cvr, compareTotals?.cvr)}
          deltaClass={deltaColor((totals.cvr ?? 0) - (compareTotals?.cvr ?? 0))}
          icon={<Percent size={14} />}
          colorClass={(totals.cvr || totals.blended_cvr || 0) >= 1 ? "text-emerald-400" : "text-yellow-400"}
        />
        <Card
          label="Sync Delta"
          value={formatMoney(delta)}
          sub={
            totals.reported != null
              ? `Reported: ${formatMoney(totals.reported)}`
              : undefined
          }
          delta={moneyDelta(totals.reported_delta, compareTotals?.reported_delta)}
          deltaClass={deltaColor((totals.reported_delta ?? 0) - (compareTotals?.reported_delta ?? 0))}
          icon={<Gauge size={14} />}
          colorClass={
            delta != null && delta > 0
              ? "text-emerald-400"
              : delta != null && delta < 0
              ? "text-red-400"
              : "text-gray-400"
          }
        />
      </div>
    </div>
  );
}
