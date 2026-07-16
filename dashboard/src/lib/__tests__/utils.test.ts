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
  shiftIso,
  reportTimeZone,
  setReportTimeZone,
  reportTodayIso,
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
    expect(profitColor(1)).toBe("text-positive");
    expect(profitColor(0.01)).toBe("text-positive");
  });

  it("returns rose for negative values", () => {
    expect(profitColor(-1)).toBe("text-negative");
    expect(profitColor(-0.01)).toBe("text-negative");
  });

  it("returns muted for exactly zero", () => {
    expect(profitColor(0)).toBe("text-ink-faint");
  });
});

describe("daysAgo", () => {
  // daysAgo is anchored to "today" in the REPORTING timezone, not the machine's
  // local zone — deriving the expectation from local Date math makes the test
  // flake for the hours of the day when the two zones disagree on the date.
  // Pin both the clock and the zone so the expected dates are literal.
  const defaultTz = reportTimeZone();
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-03-15T12:00:00Z"));
    setReportTimeZone("UTC");
  });
  afterEach(() => {
    setReportTimeZone(defaultTz);
    vi.useRealTimers();
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

describe("shiftIso", () => {
  it("shifts within a month", () => {
    expect(shiftIso("2026-07-15", 1)).toBe("2026-07-16");
    expect(shiftIso("2026-07-15", -7)).toBe("2026-07-08");
    expect(shiftIso("2026-07-15", 0)).toBe("2026-07-15");
  });

  it("crosses month boundaries", () => {
    expect(shiftIso("2026-01-31", 1)).toBe("2026-02-01");
    expect(shiftIso("2026-03-01", -1)).toBe("2026-02-28");
    // Leap year: Feb 2024 has 29 days.
    expect(shiftIso("2024-02-28", 1)).toBe("2024-02-29");
    expect(shiftIso("2024-03-01", -1)).toBe("2024-02-29");
  });

  it("crosses year boundaries", () => {
    expect(shiftIso("2025-12-31", 1)).toBe("2026-01-01");
    expect(shiftIso("2026-01-01", -1)).toBe("2025-12-31");
    expect(shiftIso("2026-01-05", -7)).toBe("2025-12-29");
  });

  it("shifts exactly one calendar day across DST transitions", () => {
    // US spring-forward (2026-03-08) and fall-back (2026-11-01) dates: local-time
    // Date.setDate math can land on the same or a doubled day in a DST browser
    // zone; UTC-anchored math must always move exactly one day.
    expect(shiftIso("2026-03-08", 1)).toBe("2026-03-09");
    expect(shiftIso("2026-03-09", -1)).toBe("2026-03-08");
    expect(shiftIso("2026-10-31", 1)).toBe("2026-11-01");
    expect(shiftIso("2026-11-01", -1)).toBe("2026-10-31");
  });
});

describe("reportTimeZone / setReportTimeZone", () => {
  // Captured before any test runs, so it is the env-configured default.
  const defaultTz = reportTimeZone();

  afterEach(() => {
    setReportTimeZone(defaultTz);
    vi.useRealTimers();
  });

  it("returns the env default when no override is set", () => {
    expect(reportTimeZone()).toBe(defaultTz);
  });

  it("prefers a valid runtime override over the env default", () => {
    setReportTimeZone("America/New_York");
    expect(reportTimeZone()).toBe("America/New_York");
  });

  it("ignores invalid IANA names, keeping the previous value", () => {
    setReportTimeZone("America/New_York");
    setReportTimeZone("Not/A_Zone");
    expect(reportTimeZone()).toBe("America/New_York");
    setReportTimeZone("");
    expect(reportTimeZone()).toBe("America/New_York");
  });

  it("reportTodayIso reflects the override", () => {
    // Freeze the clock at a UTC instant where zones on opposite sides of the
    // date line disagree on the calendar date. Etc/GMT+12 is UTC-12 and
    // Etc/GMT-12 is UTC+12 (POSIX sign convention).
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-07-15T00:00:00Z"));
    setReportTimeZone("Etc/GMT+12");
    expect(reportTodayIso()).toBe("2026-07-14");
    setReportTimeZone("Etc/GMT-12");
    expect(reportTodayIso()).toBe("2026-07-15");
  });
});
