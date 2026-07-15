import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatMoney(value: number | null | undefined): string {
  if (value == null) return "\u2014";
  const abs = Math.abs(value);
  const sign = value < 0 ? "-" : "";
  return `${sign}$${abs.toLocaleString("en-CA", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

export function formatMoneyCompact(value: number | null | undefined): string {
  if (value == null) return "\u2014";
  const abs = Math.abs(value);
  const sign = value < 0 ? "-" : "";
  if (abs >= 1_000_000) return `${sign}$${(abs / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${sign}$${(abs / 1_000).toFixed(1)}K`;
  return `${sign}$${abs.toFixed(2)}`;
}

export function formatNumber(value: number | null | undefined): string {
  if (value == null) return "—";
  return value.toLocaleString();
}

export function formatPercent(value: number | null | undefined): string {
  if (value == null) return "—";
  return `${(value * 100).toFixed(1)}%`;
}

export function formatPercentValue(value: number | null | undefined, digits = 1): string {
  if (value == null) return "—";
  return `${Number(value).toFixed(digits)}%`;
}

export function formatRatio(value: number | null | undefined, digits = 2): string {
  if (value == null) return "—";
  return `${Number(value).toFixed(digits)}x`;
}

export function formatSeconds(value: number | null | undefined): string {
  if (value == null) return "—";
  if (value < 60) return `${value.toFixed(1)}s`;
  const m = Math.floor(value / 60);
  const s = Math.round(value % 60);
  return `${m}m ${s}s`;
}

export function profitColor(value: number | null | undefined): string {
  if (value == null) return "text-ink-faint";
  if (value > 0) return "text-positive";
  // text-negative (not red-400) to match ROAS/delta coloring elsewhere in the table.
  if (value < 0) return "text-negative";
  return "text-ink-faint";
}

/**
 * The reporting timezone the backend buckets days into. Configure a deployment
 * via NEXT_PUBLIC_REPORT_TIMEZONE to match the backend's REPORT_TIMEZONE; both
 * default to Etc/GMT+6 (the Hyros parity offset).
 */
export function reportTimeZone(): string {
  return process.env.NEXT_PUBLIC_REPORT_TIMEZONE || "Etc/GMT+6";
}

/**
 * "Today" (YYYY-MM-DD) in the reporting timezone — NOT the browser's zone. Using
 * the browser zone let a user ahead of the reporting zone pick a "Today" window
 * that was entirely in the server's future, which returned an empty report.
 */
export function reportTodayIso(): string {
  try {
    return new Intl.DateTimeFormat("en-CA", { timeZone: reportTimeZone() }).format(new Date());
  } catch {
    const d = new Date();
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
  }
}

/** Shift a YYYY-MM-DD date string by whole days (timezone-agnostic date math). */
export function shiftIso(iso: string, deltaDays: number): string {
  const d = new Date(`${iso}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + deltaDays);
  return d.toISOString().slice(0, 10);
}

/**
 * Returns the YYYY-MM-DD calendar date `n` days before "today" in the reporting
 * timezone. `daysAgo(0)` is today, `daysAgo(1)` is yesterday, etc.
 */
export function daysAgo(n: number): string {
  const days = Number.isFinite(n) ? Math.trunc(n) : 0;
  return shiftIso(reportTodayIso(), -days);
}
