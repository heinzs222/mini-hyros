"use client";

import { useEffect, useRef, useState } from "react";
import { Check, ChevronDown } from "lucide-react";

export interface ModelOption {
  value: string;
  label: string;
  desc: string;
  why: string;
  when: string;
  who: string;
  badge?: string;
  color: string;
}

export const MODEL_OPTIONS: ModelOption[] = [
  {
    value: "last_click",
    label: "Last Click",
    desc: "Full credit is assigned to the last source clicked.",
    why: "To identify which source directly drove the final conversion and optimize for closing performance.",
    when: "When focusing on bottom-of-funnel campaigns or evaluating what converts users at the final step.",
    who: "Marketers optimizing conversion-focused campaigns and sales-driven funnels.",
    badge: "MOST POPULAR",
    color: "#eab308",
  },
  {
    value: "first_click",
    label: "First Click",
    desc: "Full credit is assigned to the first source clicked.",
    why: "To find which sources introduce new people to your brand and drive top-of-funnel demand.",
    when: "When evaluating awareness and prospecting campaigns that start the journey.",
    who: "Marketers scaling acquisition and measuring discovery channels.",
    color: "#60a5fa",
  },
  {
    value: "time_decay",
    label: "Time Decay",
    desc: "Assigns increasing value to touchpoints that occur closer to the conversion event.",
    why: "To balance credit while rewarding the sources nearest the sale.",
    when: "When journeys are long and recency matters to the close.",
    who: "Marketers with multi-touch funnels and longer sales cycles.",
    color: "#a78bfa",
  },
  {
    value: "linear",
    label: "Linear",
    desc: "Distributes equal credit across all touchpoints in the customer journey.",
    why: "To value every interaction that contributed to the conversion equally.",
    when: "When you want a balanced, full-journey view of channel contribution.",
    who: "Marketers running coordinated multi-channel programs.",
    color: "#34d399",
  },
  {
    value: "data_driven_proxy",
    label: "Data-Driven",
    desc: "Credit is weighted by each touchpoint's modeled contribution to the conversion.",
    why: "To allocate credit based on observed impact rather than fixed rules.",
    when: "When you have enough conversion volume for a fair statistical view.",
    who: "Data-led teams optimizing budget across many sources.",
    color: "#f472b6",
  },
];

export function modelLabel(value: string): string {
  return MODEL_OPTIONS.find((m) => m.value === value)?.label || value;
}

export default function ModelSelect({ value, onChange }: { value: string; onChange: (v: string) => void }) {
  const [open, setOpen] = useState(false);
  const [hovered, setHovered] = useState(value);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (open) setHovered(value);
  }, [open, value]);

  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [open]);

  const info = MODEL_OPTIONS.find((m) => m.value === hovered) || MODEL_OPTIONS[0];

  return (
    <div ref={ref} className="relative">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className="flex h-[34px] items-center gap-2 rounded-lg border border-[var(--card-border)] bg-[var(--surface-2)] px-3 text-[13px] text-ink hover:border-white/20"
      >
        <span className="flex flex-col gap-[2px]">
          <span className="flex items-end gap-[2px]">
            <span className="h-2 w-[3px] rounded-sm" style={{ background: info.color }} />
            <span className="h-3 w-[3px] rounded-sm" style={{ background: info.color }} />
            <span className="h-1.5 w-[3px] rounded-sm" style={{ background: info.color }} />
          </span>
        </span>
        <span className="font-medium">{modelLabel(value)}</span>
        <ChevronDown size={14} className={`text-ink-dim transition-transform ${open ? "rotate-180" : ""}`} />
      </button>

      {open && (
        <div className="animate-hpop absolute right-0 z-[60] mt-2 flex w-[680px] max-w-[92vw] rounded-2xl border border-[var(--card-border)] bg-[#0c0c11] p-2 shadow-2xl">
          {/* Model list */}
          <div className="w-1/2 border-r border-[var(--card-border)] pr-2">
            {MODEL_OPTIONS.map((m) => {
              const active = m.value === value;
              return (
                <button
                  key={m.value}
                  onMouseEnter={() => setHovered(m.value)}
                  onClick={() => {
                    onChange(m.value);
                    setOpen(false);
                  }}
                  className={`flex w-full flex-col gap-1 rounded-lg px-3 py-2.5 text-left transition-colors ${
                    m.value === hovered ? "bg-white/[0.04]" : "hover:bg-white/[0.03]"
                  }`}
                >
                  <span className="flex items-center gap-2">
                    <span className="flex items-end gap-[2px]">
                      <span className="h-2 w-[3px] rounded-sm" style={{ background: m.color }} />
                      <span className="h-3 w-[3px] rounded-sm" style={{ background: m.color }} />
                      <span className="h-1.5 w-[3px] rounded-sm" style={{ background: m.color }} />
                    </span>
                    <span className="text-[14px] font-semibold text-ink-bright">{m.label}</span>
                    {m.badge && (
                      <span className="rounded bg-white/10 px-1.5 py-0.5 text-[9px] font-semibold tracking-wide text-ink-dim">
                        {m.badge}
                      </span>
                    )}
                    {active && <Check size={14} className="ml-auto text-brand-400" />}
                  </span>
                  <span className="text-[12px] leading-snug text-ink-dim">{m.desc}</span>
                </button>
              );
            })}
          </div>

          {/* Info panel */}
          <div className="w-1/2 space-y-4 px-4 py-3">
            <div>
              <div className="text-[13px] font-semibold text-ink-bright">Why use it</div>
              <div className="mt-1 text-[12px] leading-snug text-ink-dim">{info.why}</div>
            </div>
            <div>
              <div className="text-[13px] font-semibold text-ink-bright">When to use it</div>
              <div className="mt-1 text-[12px] leading-snug text-ink-dim">{info.when}</div>
            </div>
            <div>
              <div className="text-[13px] font-semibold text-ink-bright">Who it&apos;s for</div>
              <div className="mt-1 text-[12px] leading-snug text-ink-dim">{info.who}</div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
