"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { CalendarDays, ChevronLeft, ChevronRight, Info, BookOpen } from "lucide-react";
import { reportTodayIso, shiftIso } from "@/lib/utils";

/* ──────────────────────────── date helpers ──────────────────────────── */

function pad(n: number): string {
  return String(n).padStart(2, "0");
}
function toIso(d: Date): string {
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}
function fromIso(iso: string): Date {
  const [y, m, d] = iso.split("-").map(Number);
  return new Date(y, (m || 1) - 1, d || 1);
}
function daysInMonth(year: number, month: number): number {
  // month is 0-indexed; day 0 of the next month is the last day of this month.
  return new Date(year, month + 1, 0).getDate();
}
/**
 * Month navigation for the calendar chevrons and view seed: snap to the 1st
 * BEFORE shifting months so day-of-month can never overflow into the wrong
 * month (Jan 31 + 1mo must land in Feb, not spill to Mar 3). Always returns
 * the 1st of the target month.
 */
function addMonthsFirst(iso: string, n: number): string {
  const d = fromIso(iso);
  d.setDate(1);
  d.setMonth(d.getMonth() + n);
  return toIso(d);
}
/**
 * Month arithmetic that preserves the day-of-month where possible but clamps
 * to the last valid day of the target month. Used by presets that subtract
 * whole months (Last 3/6 Month, Last Year) so e.g. Jul 31 − 1mo → Jun 30.
 */
function addMonthsClamped(iso: string, n: number): string {
  const d = fromIso(iso);
  const origDay = d.getDate();
  d.setDate(1);
  d.setMonth(d.getMonth() + n);
  d.setDate(Math.min(origDay, daysInMonth(d.getFullYear(), d.getMonth())));
  return toIso(d);
}
function firstOfMonthIso(iso: string): string {
  const d = fromIso(iso || todayIso());
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-01`;
}
function todayIso(): string {
  // "Today" in the reporting timezone so presets align with the backend's day
  // boundaries (see reportTodayIso). Prevents a future-dated "Today" window.
  return reportTodayIso();
}
/** "06.10" style label used on the trigger pills. */
function fmtPill(iso: string): string {
  if (!iso) return "—";
  const d = fromIso(iso);
  return `${pad(d.getMonth() + 1)}.${pad(d.getDate())}`;
}

const MONTHS = [
  "January", "February", "March", "April", "May", "June",
  "July", "August", "September", "October", "November", "December",
];
const WEEKDAYS = ["Su", "Mo", "Tu", "We", "Th", "Fr", "Sa"];

interface Range {
  start: string;
  end: string;
}

interface Preset {
  label: string;
  range: () => Range;
}

function buildPresets(): Preset[] {
  const t = todayIso();
  const d = fromIso(t);
  const firstOfMonth = `${d.getFullYear()}-${pad(d.getMonth() + 1)}-01`;
  const firstOfYear = `${d.getFullYear()}-01-01`;
  return [
    { label: "Today", range: () => ({ start: t, end: t }) },
    { label: "Yesterday", range: () => ({ start: shiftIso(t, -1), end: shiftIso(t, -1) }) },
    { label: "This Month", range: () => ({ start: firstOfMonth, end: t }) },
    { label: "Last 7 Days", range: () => ({ start: shiftIso(t, -7), end: shiftIso(t, -1) }) },
    { label: "Last 14 Days", range: () => ({ start: shiftIso(t, -14), end: shiftIso(t, -1) }) },
    { label: "Last 30 Days", range: () => ({ start: shiftIso(t, -30), end: shiftIso(t, -1) }) },
    { label: "Last 3 Month", range: () => ({ start: addMonthsClamped(t, -3), end: t }) },
    { label: "Last 6 Month", range: () => ({ start: addMonthsClamped(t, -6), end: t }) },
    { label: "Last Year", range: () => ({ start: addMonthsClamped(t, -12), end: t }) },
    { label: "All Time", range: () => ({ start: "2015-01-01", end: t }) },
    { label: "Year to Date", range: () => ({ start: firstOfYear, end: t }) },
  ];
}

/* Days grid (6 rows x 7) for a given year/month, including adjacent-month spill. */
function monthGrid(year: number, month: number): Date[] {
  const first = new Date(year, month, 1);
  const startOffset = first.getDay();
  const gridStart = new Date(year, month, 1 - startOffset);
  return Array.from({ length: 42 }, (_, i) => {
    const d = new Date(gridStart);
    d.setDate(gridStart.getDate() + i);
    return d;
  });
}

/* ──────────────────────────── toggle ──────────────────────────── */

function Toggle({ on, onClick }: { on: boolean; onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={on}
      className={`relative inline-flex h-[22px] w-[40px] items-center rounded-full transition-colors ${
        on ? "bg-emerald-500" : "bg-white/15"
      }`}
    >
      <span
        className={`inline-block h-[16px] w-[16px] transform rounded-full bg-white shadow transition-transform ${
          on ? "translate-x-[21px]" : "translate-x-[3px]"
        }`}
      />
    </button>
  );
}

/* ──────────────────────────── component ──────────────────────────── */

interface Props {
  value: Range;
  onChange: (range: Range) => void;
  compareRange?: Range | null;
  compareEnabled?: boolean;
  onCompareEnabledChange?: (v: boolean) => void;
  autoCompare?: boolean;
  onAutoCompareChange?: (v: boolean) => void;
  showCompareControls?: boolean;
}

export default function DateRangePicker({
  value,
  onChange,
  compareRange,
  compareEnabled = false,
  onCompareEnabledChange,
  autoCompare = true,
  onAutoCompareChange,
  showCompareControls = true,
}: Props) {
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState<Range>(value);
  const [anchor, setAnchor] = useState<string | null>(null);
  const [viewIso, setViewIso] = useState(firstOfMonthIso(value.end || todayIso()));
  const ref = useRef<HTMLDivElement>(null);
  // Tracks the previous `open` value so the reset below fires only on the
  // closed→open transition, not on every re-render while the popover is open.
  const prevOpenRef = useRef(false);

  // Recompute presets when the popover opens so "Today"/"Yesterday" etc. stay
  // correct across a midnight boundary within a long-lived session.
  const presets = useMemo(() => buildPresets(), [open]);

  useEffect(() => {
    const justOpened = open && !prevOpenRef.current;
    prevOpenRef.current = open;
    if (!justOpened) return;
    // Seed the draft only on open, keyed on the PRIMITIVE start/end strings —
    // not the object identity. Consumers pass fresh {start,end} literals every
    // render (and the page re-renders every 30s / on websocket events); keying
    // on identity would wipe an in-progress selection. Re-renders while open no
    // longer reset the draft/anchor.
    setDraft({ start: value.start, end: value.end });
    setAnchor(null);
    setViewIso(firstOfMonthIso(value.end || todayIso()));
  }, [open, value.start, value.end]);

  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [open]);

  const view = fromIso(viewIso);
  const leftYear = view.getFullYear();
  const leftMonth = view.getMonth();
  const rightDate = new Date(leftYear, leftMonth + 1, 1);

  const inRange = (iso: string) => draft.start && draft.end && iso >= draft.start && iso <= draft.end;
  const isStart = (iso: string) => iso === draft.start;
  const isEnd = (iso: string) => iso === draft.end;

  const commitRange = (range: Range) => {
    const start = range.start || range.end;
    const end = range.end || range.start;
    if (!start || !end) return;
    onChange(start <= end ? { start, end } : { start: end, end: start });
    setOpen(false);
  };

  const pickDay = (iso: string) => {
    if (!anchor) {
      setDraft({ start: iso, end: iso });
      setAnchor(iso);
      return;
    }
    const next = iso < anchor ? { start: iso, end: anchor } : { start: anchor, end: iso };
    setDraft(next);
    setAnchor(null);
    commitRange(next);
  };

  const applyPreset = (p: Preset) => {
    const r = p.range();
    setDraft(r);
    setAnchor(null);
    setViewIso(firstOfMonthIso(r.end));
    commitRange(r);
  };

  const apply = () => {
    commitRange(draft);
  };

  const activePresetLabel = useMemo(() => {
    const match = presets.find((p) => {
      const r = p.range();
      return r.start === value.start && r.end === value.end;
    });
    return match?.label;
  }, [presets, value]);

  const renderMonth = (year: number, month: number) => (
    <div className="w-[244px]">
      <div className="mb-2 text-center text-[13px] font-semibold text-ink-bright">
        {MONTHS[month]} {year}
      </div>
      <div className="grid grid-cols-7 gap-y-1 text-center">
        {WEEKDAYS.map((w) => (
          <div key={w} className="text-[11px] font-medium text-ink-faint">
            {w}
          </div>
        ))}
        {monthGrid(year, month).map((d, i) => {
          const iso = toIso(d);
          const outside = d.getMonth() !== month;
          const start = isStart(iso);
          const end = isEnd(iso);
          const within = inRange(iso) && !start && !end;
          return (
            <button
              key={i}
              type="button"
              onClick={() => pickDay(iso)}
              className={[
                "mx-auto flex h-8 w-8 items-center justify-center rounded-md text-[12.5px] transition-colors",
                outside ? "text-ink-faint/60" : "text-ink",
                within ? "bg-white/10 rounded-none" : "",
                start || end ? "bg-white font-semibold !text-black" : "hover:bg-white/10",
              ].join(" ")}
            >
              {d.getDate()}
            </button>
          );
        })}
      </div>
    </div>
  );

  return (
    <div ref={ref} className="relative">
      {/* Trigger */}
      <div className="flex items-center gap-2">
        <button
          type="button"
          onClick={() => setOpen((o) => !o)}
          className="flex items-center gap-2 rounded-lg border border-[var(--card-border)] bg-[var(--surface-2)] px-3 py-1.5 text-[13px] text-ink hover:border-white/20"
        >
          <CalendarDays size={14} className="text-ink-dim" />
          <span className="tabular font-medium">
            {fmtPill(value.start)} – {fmtPill(value.end)}
          </span>
        </button>

        {showCompareControls && compareEnabled && (
          <>
            <span className="text-[12px] text-ink-dim">vs.</span>
            <div className="flex items-center gap-2 rounded-lg border border-[var(--card-border)] bg-[var(--surface)] px-3 py-1.5 text-[13px] text-ink-dim">
              <CalendarDays size={14} className="text-ink-faint" />
              <span className="tabular">
                {compareRange ? `${fmtPill(compareRange.start)} – ${fmtPill(compareRange.end)}` : "—"}
              </span>
            </div>
          </>
        )}

        {showCompareControls && (
          <>
            <div className="ml-1 flex items-center gap-2 rounded-lg border border-[var(--card-border)] bg-[var(--surface)] px-2.5 py-1.5">
              <Info size={13} className="text-ink-faint" />
              <span className="text-[12px] text-ink-dim">Auto-compare</span>
              <Toggle on={!!autoCompare} onClick={() => onAutoCompareChange?.(!autoCompare)} />
            </div>
            <div
              className={`flex items-center gap-2 rounded-lg border px-2.5 py-1.5 ${
                compareEnabled ? "border-emerald-500/40" : "border-[var(--card-border)]"
              } bg-[var(--surface)]`}
            >
              <BookOpen size={13} className={compareEnabled ? "text-emerald-400" : "text-ink-faint"} />
              <span className="text-[12px] text-ink-dim">Compare</span>
              <Toggle on={!!compareEnabled} onClick={() => onCompareEnabledChange?.(!compareEnabled)} />
            </div>
          </>
        )}
      </div>

      {/* Popover */}
      {open && (
        <div className="animate-hpop absolute right-0 z-[60] mt-2 w-[760px] max-w-[92vw] rounded-2xl border border-[var(--card-border)] bg-[#0c0c11] p-4 shadow-2xl">
          <div className="flex gap-4">
            {/* Presets */}
            <div className="w-[160px] shrink-0 space-y-0.5">
              {presets.map((p) => {
                const active = activePresetLabel === p.label;
                return (
                  <button
                    key={p.label}
                    type="button"
                    onClick={() => applyPreset(p)}
                    className={`block w-full rounded-md px-3 py-[7px] text-left text-[13px] transition-colors ${
                      active
                        ? "bg-white/10 font-medium text-ink-bright"
                        : "text-ink-dim hover:bg-white/5 hover:text-ink"
                    }`}
                  >
                    {p.label}
                  </button>
                );
              })}
            </div>

            {/* Calendars */}
            <div className="min-w-0 flex-1">
              <div className="mb-3 flex items-center justify-between">
                <button
                  type="button"
                  onClick={() => setViewIso(addMonthsFirst(viewIso, -1))}
                  className="rounded-md p-1.5 text-ink-dim hover:bg-white/10 hover:text-ink"
                >
                  <ChevronLeft size={16} />
                </button>
                <div className="flex items-center gap-2">
                  <select
                    value={leftMonth}
                    onChange={(e) => setViewIso(`${leftYear}-${pad(Number(e.target.value) + 1)}-01`)}
                    className="rounded-md border border-[var(--card-border)] bg-[var(--surface-2)] px-2 py-1 text-[12px] text-ink focus:outline-none"
                  >
                    {MONTHS.map((m, i) => (
                      <option key={m} value={i}>
                        {m}
                      </option>
                    ))}
                  </select>
                  <select
                    value={leftYear}
                    onChange={(e) => setViewIso(`${e.target.value}-${pad(leftMonth + 1)}-01`)}
                    className="rounded-md border border-[var(--card-border)] bg-[var(--surface-2)] px-2 py-1 text-[12px] text-ink focus:outline-none"
                  >
                    {Array.from({ length: 16 }, (_, i) => 2015 + i).map((y) => (
                      <option key={y} value={y}>
                        {y}
                      </option>
                    ))}
                  </select>
                </div>
                <button
                  type="button"
                  onClick={() => setViewIso(addMonthsFirst(viewIso, 1))}
                  className="rounded-md p-1.5 text-ink-dim hover:bg-white/10 hover:text-ink"
                >
                  <ChevronRight size={16} />
                </button>
              </div>

              <div className="flex justify-between gap-4">
                {renderMonth(leftYear, leftMonth)}
                {renderMonth(rightDate.getFullYear(), rightDate.getMonth())}
              </div>

              <div className="mt-4 flex items-center justify-center">
                <div className="flex items-center gap-3 rounded-lg border border-white/15 px-4 py-2 text-[13px]">
                  <span className="tabular text-ink-bright">{draft.start || "—"}</span>
                  <span className="text-ink-faint">–</span>
                  <span className="tabular text-ink-bright">{draft.end || "—"}</span>
                </div>
              </div>
            </div>
          </div>

          <div className="mt-4 flex items-center justify-between border-t border-[var(--card-border)] pt-3">
            <button
              type="button"
              onClick={() => setOpen(false)}
              className="rounded-lg border border-[var(--card-border)] px-4 py-1.5 text-[13px] text-ink-dim hover:bg-white/5 hover:text-ink"
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={apply}
              className="rounded-lg bg-white px-5 py-1.5 text-[13px] font-semibold text-black hover:bg-white/90"
            >
              Apply
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
