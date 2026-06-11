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
  all_orders_gross_revenue?: number;
  all_orders_cogs?: number;
  all_orders_fees?: number;
  tracked_orders?: number;
  tracked_revenue?: number;
  tracked_gross_revenue?: number;
  attributed_orders?: number;
  attributed_revenue?: number;
  unattributed_orders?: number;
  unattributed_revenue?: number;
  attribution_rate?: number | null;
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
  return `${sign}${formatMoney(Math.abs(diff))}`;
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

function trackedOrders(totals?: SummaryTotals | null): number {
  return Number(totals?.tracked_orders ?? totals?.all_orders_count ?? 0);
}

function trackedRevenue(totals?: SummaryTotals | null): number {
  return Number(totals?.tracked_revenue ?? totals?.all_orders_revenue ?? 0);
}

function attributedOrders(totals?: SummaryTotals | null): number {
  return Number(totals?.attributed_orders ?? totals?.orders ?? 0);
}

function attributedRevenue(totals?: SummaryTotals | null): number {
  return Number(totals?.attributed_revenue ?? totals?.revenue ?? 0);
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
      {delta && <div className={`text-[10px] ${deltaClass || "text-gray-500"}`}>Delta {delta}</div>}
    </div>
  );
}

export default function SummaryCards({ totals, compareTotals, compareLabel, showCompareBanner = true }: Props) {
  const syncDelta = totals.reported_delta;

  const currentTrackedOrders = trackedOrders(totals);
  const currentTrackedRevenue = trackedRevenue(totals);
  const currentAttributedOrders = attributedOrders(totals);
  const currentAttributedRevenue = attributedRevenue(totals);
  const compareTrackedOrders = compareTotals ? trackedOrders(compareTotals) : undefined;
  const compareTrackedRevenue = compareTotals ? trackedRevenue(compareTotals) : undefined;
  const compareAttributedOrders = compareTotals ? attributedOrders(compareTotals) : undefined;
  const compareAttributedRevenue = compareTotals ? attributedRevenue(compareTotals) : undefined;
  const hasAttribution = currentAttributedOrders > 0 || currentAttributedRevenue > 0;
  const compareHasAttribution = Number(compareAttributedOrders ?? 0) > 0 || Number(compareAttributedRevenue ?? 0) > 0;

  const cost = Number(totals.cost ?? 0);
  const clicks = Number(totals.clicks ?? 0);
  const compareCost = Number(compareTotals?.cost ?? 0);
  const compareClicks = Number(compareTotals?.clicks ?? 0);
  const blendedRoas = totals.blended_roas ?? (cost > 0 ? Math.round((currentTrackedRevenue / cost) * 100) / 100 : null);
  const blendedCvr = totals.blended_cvr ?? (clicks > 0 ? Math.round((currentTrackedOrders / clicks) * 100000) / 1000 : null);
  const blendedAov = totals.blended_aov ?? (currentTrackedOrders > 0 ? Math.round((currentTrackedRevenue / currentTrackedOrders) * 100) / 100 : null);
  const blendedProfit = totals.blended_profit ?? Math.round((currentTrackedRevenue - cost) * 100) / 100;
  const blendedCpa = totals.blended_cpa ?? (currentTrackedOrders > 0 ? Math.round((cost / currentTrackedOrders) * 100) / 100 : null);
  const mer = totals.mer ?? (cost > 0 ? Math.round((currentTrackedRevenue / cost) * 100) / 100 : null);
  const compareBlendedRoas = compareTotals
    ? compareTotals.blended_roas ?? (compareCost > 0 ? Math.round((Number(compareTrackedRevenue ?? 0) / compareCost) * 100) / 100 : null)
    : undefined;
  const compareBlendedCvr = compareTotals
    ? compareTotals.blended_cvr ?? (compareClicks > 0 ? Math.round((Number(compareTrackedOrders ?? 0) / compareClicks) * 100000) / 1000 : null)
    : undefined;
  const compareBlendedProfit = compareTotals
    ? compareTotals.blended_profit ?? Math.round((Number(compareTrackedRevenue ?? 0) - compareCost) * 100) / 100
    : undefined;

  const attributionRate = totals.attribution_rate ?? (
    currentTrackedOrders > 0 ? Math.round((currentAttributedOrders / currentTrackedOrders) * 10000) / 100 : null
  );
  const compareAttributionRate = compareTotals?.attribution_rate ?? (
    compareTrackedOrders && compareTrackedOrders > 0 && compareAttributedOrders != null ? (compareAttributedOrders / compareTrackedOrders) * 100 : null
  );

  const roasDisplay = hasAttribution ? totals.roas ?? blendedRoas : blendedRoas;
  const compareRoasDisplay = compareTotals
    ? compareHasAttribution
      ? compareTotals.roas ?? compareBlendedRoas
      : compareBlendedRoas
    : undefined;
  const cvrDisplay = hasAttribution ? totals.cvr ?? blendedCvr : blendedCvr;
  const compareCvrDisplay = compareTotals
    ? compareHasAttribution
      ? compareTotals.cvr ?? compareBlendedCvr
      : compareBlendedCvr
    : undefined;
  const profitDisplay = hasAttribution ? totals.profit : blendedProfit;
  const compareProfitDisplay = compareTotals
    ? compareHasAttribution
      ? compareTotals.profit
      : compareBlendedProfit
    : undefined;

  return (
    <div className="space-y-2">
      {showCompareBanner && compareTotals && (
        <div className="rounded-lg border border-[var(--card-border)] bg-white/[0.02] px-3 py-2 text-[11px] text-gray-400">
          Comparison baseline: {compareLabel || "Selected comparison"}
        </div>
      )}

      <div className="grid grid-cols-2 md:grid-cols-5 xl:grid-cols-10 gap-3">
        <Card
          label="Ad Spend"
          value={formatMoney(totals.cost)}
          sub={`CPC: ${formatMoney(totals.cpc)} | CPA: ${formatMoney(totals.cpa ?? totals.cac)}`}
          delta={moneyDelta(totals.cost, compareTotals?.cost)}
          deltaClass={deltaColor(Number(totals.cost ?? 0) - Number(compareTotals?.cost ?? 0))}
          icon={<DollarSign size={14} />}
        />
        <Card
          label="Tracked Revenue"
          value={formatMoney(currentTrackedRevenue)}
          sub={`${formatNumber(currentTrackedOrders)} tracked orders`}
          delta={moneyDelta(currentTrackedRevenue, compareTrackedRevenue)}
          deltaClass={deltaColor(compareTrackedRevenue == null ? null : currentTrackedRevenue - compareTrackedRevenue)}
          icon={<TrendingUp size={14} />}
          colorClass="text-emerald-400"
        />
        <Card
          label="Attr. Revenue"
          value={formatMoney(currentAttributedRevenue)}
          sub={`${formatNumber(currentAttributedOrders)} attributed orders`}
          delta={moneyDelta(currentAttributedRevenue, compareAttributedRevenue)}
          deltaClass={deltaColor(compareAttributedRevenue == null ? null : currentAttributedRevenue - compareAttributedRevenue)}
          icon={<Target size={14} />}
          colorClass={currentAttributedRevenue > 0 ? "text-emerald-400" : "text-gray-400"}
        />
        <Card
          label={hasAttribution ? "Profit (Attr.)" : "Profit (Tracked)"}
          value={formatMoney(profitDisplay)}
          sub={
            hasAttribution
              ? `Margin: ${formatPercentValue(totals.margin_pct)} | Net: ${formatMoney(totals.net_profit)}`
              : "Tracked revenue - ad spend"
          }
          delta={moneyDelta(profitDisplay, compareProfitDisplay)}
          deltaClass={deltaColor(Number(profitDisplay ?? 0) - Number(compareProfitDisplay ?? 0))}
          icon={<TrendingDown size={14} />}
          colorClass={profitColor(profitDisplay)}
        />
        <Card
          label={hasAttribution ? "ROAS (Attr.)" : "ROAS (Tracked)"}
          value={formatRatio(roasDisplay)}
          sub={`MER: ${formatRatio(mer)}`}
          delta={ratioDelta(roasDisplay, compareRoasDisplay)}
          deltaClass={deltaColor(Number(roasDisplay ?? 0) - Number(compareRoasDisplay ?? 0))}
          icon={<Target size={14} />}
          colorClass={(roasDisplay ?? 0) >= 1 ? "text-emerald-400" : "text-red-400"}
        />
        <Card
          label="Clicks"
          value={formatNumber(totals.clicks)}
          sub={
            totals.impressions
              ? `Impr: ${formatNumber(totals.impressions)} | CTR: ${formatPercentValue(totals.ctr ?? null)}`
              : `Attr. CVR: ${formatPercentValue(totals.cvr)}`
          }
          delta={numberDelta(totals.clicks, compareTotals?.clicks)}
          deltaClass={deltaColor(Number(totals.clicks ?? 0) - Number(compareTotals?.clicks ?? 0))}
          icon={<MousePointerClick size={14} />}
        />
        <Card
          label="Orders"
          value={formatNumber(currentTrackedOrders)}
          sub={`Attr: ${formatNumber(currentAttributedOrders)} | AOV: ${formatMoney(blendedAov)} | CPA: ${formatMoney(blendedCpa)}`}
          delta={numberDelta(currentTrackedOrders, compareTrackedOrders)}
          deltaClass={deltaColor(compareTrackedOrders == null ? null : currentTrackedOrders - compareTrackedOrders)}
          icon={<ShoppingCart size={14} />}
        />
        <Card
          label={hasAttribution ? "CVR (Attr.)" : "CVR (Tracked)"}
          value={formatPercentValue(cvrDisplay)}
          sub={
            hasAttribution
              ? `RPC: ${formatMoney(totals.rpc)} | tracked: ${formatPercentValue(blendedCvr)}`
              : `${formatNumber(currentTrackedOrders)} orders / ${formatNumber(clicks)} clicks`
          }
          delta={percentPointDelta(cvrDisplay, compareCvrDisplay)}
          deltaClass={deltaColor(Number(cvrDisplay ?? 0) - Number(compareCvrDisplay ?? 0))}
          icon={<Percent size={14} />}
          colorClass={(cvrDisplay ?? 0) >= 1 ? "text-emerald-400" : "text-yellow-400"}
        />
        <Card
          label="Attribution Rate"
          value={formatPercentValue(attributionRate)}
          sub={`${formatNumber(totals.unattributed_orders ?? Math.max(currentTrackedOrders - currentAttributedOrders, 0))} unattributed orders`}
          delta={percentPointDelta(attributionRate, compareAttributionRate)}
          deltaClass={deltaColor(Number(attributionRate ?? 0) - Number(compareAttributionRate ?? 0))}
          icon={<Gauge size={14} />}
          colorClass={(attributionRate ?? 0) >= 80 ? "text-emerald-400" : "text-yellow-400"}
        />
        <Card
          label="Sync Delta"
          value={formatMoney(syncDelta)}
          sub={totals.reported != null ? `Reported: ${formatMoney(totals.reported)}` : undefined}
          delta={moneyDelta(totals.reported_delta, compareTotals?.reported_delta)}
          deltaClass={deltaColor(Number(totals.reported_delta ?? 0) - Number(compareTotals?.reported_delta ?? 0))}
          icon={<Gauge size={14} />}
          colorClass={
            syncDelta != null && syncDelta > 0
              ? "text-emerald-400"
              : syncDelta != null && syncDelta < 0
              ? "text-red-400"
              : "text-gray-400"
          }
        />
      </div>
    </div>
  );
}
