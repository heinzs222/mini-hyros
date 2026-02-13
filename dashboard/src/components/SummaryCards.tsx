"use client";

import { formatMoney, formatNumber, profitColor } from "@/lib/utils";
import {
  DollarSign,
  TrendingUp,
  TrendingDown,
  MousePointerClick,
  Target,
  BarChart3,
} from "lucide-react";

interface Props {
  totals: {
    clicks: number;
    cost: number;
    revenue: number;
    total_revenue: number;
    profit: number;
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
    <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-4 flex flex-col gap-1 min-w-0">
      <div className="flex items-center gap-2 text-xs text-gray-400 uppercase tracking-wide">
        {icon}
        {label}
      </div>
      <div className={`text-2xl font-bold truncate ${colorClass || "text-white"}`}>
        {value}
      </div>
      {sub && <div className="text-xs text-gray-500">{sub}</div>}
    </div>
  );
}

export default function SummaryCards({ totals }: Props) {
  return (
    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3">
      <Card
        label="Ad Spend"
        value={formatMoney(totals.cost)}
        icon={<DollarSign size={14} />}
      />
      <Card
        label="Revenue"
        value={formatMoney(totals.revenue)}
        sub={`Total: ${formatMoney(totals.total_revenue)}`}
        icon={<TrendingUp size={14} />}
        colorClass="text-emerald-400"
      />
      <Card
        label="Profit"
        value={formatMoney(totals.profit)}
        sub={`Net: ${formatMoney(totals.net_profit)}`}
        icon={<TrendingDown size={14} />}
        colorClass={profitColor(totals.profit)}
      />
      <Card
        label="ROAS"
        value={totals.roas != null ? `${totals.roas.toFixed(2)}x` : "—"}
        sub={totals.mer != null ? `MER: ${totals.mer.toFixed(2)}x` : undefined}
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
        sub={totals.cac != null ? `CAC: ${formatMoney(totals.cac)}` : undefined}
        icon={<MousePointerClick size={14} />}
      />
      <Card
        label="Reported"
        value={formatMoney(totals.reported)}
        sub={
          totals.reported_delta != null
            ? `Delta: ${formatMoney(totals.reported_delta)}`
            : undefined
        }
        icon={<BarChart3 size={14} />}
        colorClass={
          totals.reported_delta != null && totals.reported_delta > 0
            ? "text-emerald-400"
            : totals.reported_delta != null && totals.reported_delta < 0
            ? "text-red-400"
            : "text-gray-400"
        }
      />
    </div>
  );
}
