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

interface Props {
  totals: {
    clicks: number;
    orders: number;
    cost: number;
    cpc: number | null;
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
  };
}

function Card({
  label,
  value,
  sub,
  icon,
  colorClass,
}: {
  label: string;
  value: string;
  sub?: string;
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
    </div>
  );
}

export default function SummaryCards({ totals }: Props) {
  const delta = totals.reported_delta;
  return (
    <div className="grid grid-cols-2 md:grid-cols-4 xl:grid-cols-8 gap-3">
      <Card
        label="Ad Spend"
        value={formatMoney(totals.cost)}
        sub={`CPC: ${formatMoney(totals.cpc)} | CPA: ${formatMoney(totals.cpa ?? totals.cac)}`}
        icon={<DollarSign size={14} />}
      />
      <Card
        label="Revenue (Attr.)"
        value={formatMoney(totals.revenue)}
        sub={`Total: ${formatMoney(totals.total_revenue)}`}
        icon={<TrendingUp size={14} />}
        colorClass="text-emerald-400"
      />
      <Card
        label="Profit"
        value={formatMoney(totals.profit)}
        sub={`Margin: ${formatPercentValue(totals.margin_pct)} | Net: ${formatMoney(totals.net_profit)}`}
        icon={<TrendingDown size={14} />}
        colorClass={profitColor(totals.profit)}
      />
      <Card
        label="ROAS"
        value={formatRatio(totals.roas)}
        sub={`MER: ${formatRatio(totals.mer)}`}
        icon={<Target size={14} />}
        colorClass={
          totals.roas != null && totals.roas >= 1
            ? "text-emerald-400"
            : "text-red-400"
        }
      />
      <Card
        label="Clicks"
        value={formatNumber(totals.clicks)}
        sub={`Orders: ${formatNumber(totals.orders)} | CVR: ${formatPercentValue(totals.cvr)}`}
        icon={<MousePointerClick size={14} />}
      />
      <Card
        label="Orders"
        value={formatNumber(totals.orders)}
        sub={`AOV: ${formatMoney(totals.aov)}`}
        icon={<ShoppingCart size={14} />}
      />
      <Card
        label="CVR"
        value={formatPercentValue(totals.cvr)}
        sub={`RPC: ${formatMoney(totals.rpc)}`}
        icon={<Percent size={14} />}
        colorClass={totals.cvr != null && totals.cvr >= 2 ? "text-emerald-400" : "text-yellow-400"}
      />
      <Card
        label="Sync Delta"
        value={formatMoney(delta)}
        sub={
          totals.reported != null
            ? `Reported: ${formatMoney(totals.reported)}`
            : undefined
        }
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
  );
}
