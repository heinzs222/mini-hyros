import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import {
  cn,
  formatMoney,
  formatMoneyCompact,
  formatNumber,
  formatPercent,
  formatPercentValue,
  formatRatio,
  formatSeconds,
  profitColor,
  daysAgo,
} from "@/lib/utils";

describe("cn", () => {
  it("merges and dedupes tailwind classes", () => {
    expect(cn("p-2", "p-4")).toBe("p-4");
    expect(cn("text-white", false && "hidden", "font-bold")).toBe("text-white font-bold");
  });
});

describe("formatMoney", () => {
  it("returns the em dash placeholder for null/undefined", () => {
    expect(formatMoney(null)).toBe("—");
    expect(formatMoney(undefined)).toBe("—");
  });

  it("shows exact grouped dollars and cents instead of compact rounding", () => {
    expect(formatMoney(1_000)).toBe("$1,000.00");
    expect(formatMoney(12_345.67)).toBe("$12,345.67");
    expect(formatMoney(1_500_000)).toBe("$1,500,000.00");
  });

  it("formats values < 1000 with two decimals", () => {
    expect(formatMoney(0)).toBe("$0.00");
    expect(formatMoney(9.5)).toBe("$9.50");
    expect(formatMoney(999.99)).toBe("$999.99");
    expect(formatMoney(123.456)).toBe("$123.46");
  });

  it("handles negative values with a leading minus sign", () => {
    expect(formatMoney(-50)).toBe("-$50.00");
    expect(formatMoney(-1_500)).toBe("-$1,500.00");
    expect(formatMoney(-2_000_000)).toBe("-$2,000,000.00");
    expect(formatMoney(-0.5)).toBe("-$0.50");
  });
});

describe("formatMoneyCompact", () => {
  it("keeps compact labels available for chart axes", () => {
    expect(formatMoneyCompact(null)).toBe("—");
    expect(formatMoneyCompact(999.99)).toBe("$999.99");
    expect(formatMoneyCompact(12_345)).toBe("$12.3K");
    expect(formatMoneyCompact(1_500_000)).toBe("$1.5M");
    expect(formatMoneyCompact(-1_500)).toBe("-$1.5K");
  });
});

describe("formatNumber", () => {
  it("returns the em dash for null/undefined", () => {
    expect(formatNumber(null)).toBe("—");
    expect(formatNumber(undefined)).toBe("—");
  });

  it("uses locale formatting with thousands separators", () => {
    expect(formatNumber(0)).toBe("0");
    expect(formatNumber(1234)).toBe((1234).toLocaleString());
    expect(formatNumber(1_234_567)).toBe((1_234_567).toLocaleString());
  });
});

describe("formatPercent", () => {
  it("returns the em dash for null/undefined", () => {
    expect(formatPercent(null)).toBe("—");
    expect(formatPercent(undefined)).toBe("—");
  });

  it("multiplies a fraction by 100 and appends % (1 decimal)", () => {
    expect(formatPercent(0)).toBe("0.0%");
    expect(formatPercent(0.5)).toBe("50.0%");
    expect(formatPercent(0.1234)).toBe("12.3%");
    expect(formatPercent(1)).toBe("100.0%");
  });
});

describe("formatPercentValue", () => {
  it("returns the em dash for null/undefined", () => {
    expect(formatPercentValue(null)).toBe("—");
    expect(formatPercentValue(undefined)).toBe("—");
  });

  it("treats the value as an already-scaled percentage", () => {
    expect(formatPercentValue(0)).toBe("0.0%");
    expect(formatPercentValue(12.34)).toBe("12.3%");
    expect(formatPercentValue(80)).toBe("80.0%");
  });

  it("honors the digits argument", () => {
    expect(formatPercentValue(12.345, 2)).toBe("12.35%");
    expect(formatPercentValue(50, 0)).toBe("50%");
  });
});

describe("formatRatio", () => {
  it("returns the em dash for null/undefined", () => {
    expect(formatRatio(null)).toBe("—");
    expect(formatRatio(undefined)).toBe("—");
  });

  it("appends an x suffix (default 2 decimals)", () => {
    expect(formatRatio(0)).toBe("0.00x");
    expect(formatRatio(1.5)).toBe("1.50x");
    expect(formatRatio(3.456)).toBe("3.46x");
  });

  it("honors the digits argument", () => {
    expect(formatRatio(2.5, 1)).toBe("2.5x");
    expect(formatRatio(2, 0)).toBe("2x");
  });
});

describe("formatSeconds", () => {
  it("returns the em dash for null/undefined", () => {
    expect(formatSeconds(null)).toBe("—");
    expect(formatSeconds(undefined)).toBe("—");
  });

  it("formats sub-minute durations in seconds (1 decimal)", () => {
    expect(formatSeconds(0)).toBe("0.0s");
    expect(formatSeconds(5)).toBe("5.0s");
    expect(formatSeconds(59.9)).toBe("59.9s");
  });

  it("formats durations >= 60s as minutes and seconds", () => {
    expect(formatSeconds(60)).toBe("1m 0s");
    expect(formatSeconds(90)).toBe("1m 30s");
    expect(formatSeconds(125)).toBe("2m 5s");
  });

  it("rounds the remaining seconds", () => {
    expect(formatSeconds(119.4)).toBe("1m 59s");
    // 119.6 rounds the remainder (59.6) up to 60.
    expect(formatSeconds(119.6)).toBe("1m 60s");
  });
});

describe("profitColor", () => {
  it("returns muted for null/undefined", () => {
    expect(profitColor(null)).toBe("text-ink-faint");
    expect(profitColor(undefined)).toBe("text-ink-faint");
  });

  it("returns emerald for positive values", () => {
    expect(profitColor(1)).toBe("text-emerald-400");
    expect(profitColor(0.01)).toBe("text-emerald-400");
  });

  it("returns rose for negative values", () => {
    expect(profitColor(-1)).toBe("text-rose-400");
    expect(profitColor(-0.01)).toBe("text-rose-400");
  });

  it("returns muted for exactly zero", () => {
    expect(profitColor(0)).toBe("text-ink-faint");
  });
});

describe("daysAgo", () => {
  // daysAgo buckets "today" in the reporting timezone (Etc/GMT+6 by default),
  // so comparing against the runner's local date flakes near midnight. Pin the
  // clock and zone instead so the expected dates are literal.
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-03-15T12:00:00Z"));
    vi.stubEnv("NEXT_PUBLIC_REPORT_TIMEZONE", "UTC");
  });
  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllEnvs();
  });

  it("returns a zero-padded YYYY-MM-DD string", () => {
    const result = daysAgo(0);
    expect(result).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    expect(result).toBe("2026-03-15");
  });

  it("offsets backwards by the given number of days", () => {
    expect(daysAgo(7)).toBe("2026-03-08");
    expect(daysAgo(30)).toBe("2026-02-13");
  });

  it("produces an earlier date for a larger offset", () => {
    expect(daysAgo(10) < daysAgo(1)).toBe(true);
  });
});
