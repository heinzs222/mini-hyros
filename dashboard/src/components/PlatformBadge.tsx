"use client";

import React from "react";
import { Mail, Globe2, Link2, Circle } from "lucide-react";

/**
 * Canonical, Hyros-style display names for the ad platforms.
 */
export const PLATFORM_LABELS: Record<string, string> = {
  google: "Google Ads",
  meta: "Meta Ads",
  facebook: "Meta Ads",
  tiktok: "TikTok Ads",
};

export function isKnownPlatform(platform: string): boolean {
  const p = (platform || "").toLowerCase();
  return p === "google" || p === "meta" || p === "facebook" || p === "tiktok";
}

/** Title-case a raw source string for the neutral/fallback badge label. */
function titleCase(source: string): string {
  const cleaned = source.replace(/[_/|]+/g, " ").replace(/\s+/g, " ").trim();
  if (!cleaned) return "Direct";
  return cleaned
    .split(" ")
    .map((w) => (w ? w[0].toUpperCase() + w.slice(1) : w))
    .join(" ");
}

export function platformDisplayName(platform: string): string {
  const p = (platform || "").toLowerCase();
  return PLATFORM_LABELS[p] || titleCase(p);
}

/* ── Brand marks (inline SVG, no external assets) ──────────────────────── */

function GoogleGlyph({ size }: { size: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 48 48" aria-hidden="true" focusable="false">
      <path fill="#FFC107" d="M43.611 20.083H42V20H24v8h11.303c-1.649 4.657-6.08 8-11.303 8-6.627 0-12-5.373-12-12s5.373-12 12-12c3.059 0 5.842 1.154 7.961 3.039l5.657-5.657C34.046 6.053 29.268 4 24 4 12.955 4 4 12.955 4 24s8.955 20 20 20 20-8.955 20-20c0-1.341-.138-2.65-.389-3.917z" />
      <path fill="#FF3D00" d="M6.306 14.691l6.571 4.819C14.655 15.108 18.961 12 24 12c3.059 0 5.842 1.154 7.961 3.039l5.657-5.657C34.046 6.053 29.268 4 24 4 16.318 4 9.656 8.337 6.306 14.691z" />
      <path fill="#4CAF50" d="M24 44c5.166 0 9.86-1.977 13.409-5.192l-6.19-5.238C29.211 35.091 26.715 36 24 36c-5.202 0-9.619-3.317-11.283-7.946l-6.522 5.025C9.505 39.556 16.227 44 24 44z" />
      <path fill="#1976D2" d="M43.611 20.083H42V20H24v8h11.303c-.792 2.237-2.231 4.166-4.087 5.571l6.19 5.238C36.971 39.205 44 34 44 24c0-1.341-.138-2.65-.389-3.917z" />
    </svg>
  );
}

function MetaGlyph({ size }: { size: number }) {
  // Facebook "f" mark (white), sits on a Meta-blue chip.
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path
        fill="#fff"
        d="M13.5 21v-8.4h2.82l.42-3.27H13.5V7.24c0-.95.26-1.6 1.62-1.6h1.73V2.72c-.3-.04-1.33-.13-2.53-.13-2.5 0-4.22 1.53-4.22 4.34v2.4H7.28v3.27h2.82V21h3.4z"
      />
    </svg>
  );
}

const TIKTOK_NOTE =
  "M19.59 6.69a4.83 4.83 0 0 1-3.77-4.25V2h-3.45v13.67a2.89 2.89 0 0 1-5.2 1.74 2.89 2.89 0 0 1 2.31-4.64 2.93 2.93 0 0 1 .88.13V9.4a6.84 6.84 0 0 0-1-.05A6.33 6.33 0 0 0 5 20.1a6.34 6.34 0 0 0 10.86-4.43v-7a8.16 8.16 0 0 0 4.77 1.52v-3.4a4.85 4.85 0 0 1-1-.1z";

function TikTokGlyph({ size }: { size: number }) {
  // Signature chromatic offset: teal up-left, red down-right, white note on top.
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path d={TIKTOK_NOTE} fill="#25F4EE" transform="translate(-1 -1)" />
      <path d={TIKTOK_NOTE} fill="#FE2C55" transform="translate(1 1)" />
      <path d={TIKTOK_NOTE} fill="#fff" />
    </svg>
  );
}

function fallbackIcon(source: string) {
  const s = source.toLowerCase();
  if (s.includes("email") || s.includes("klaviyo") || s.includes("newsletter") || s.includes("mail")) return Mail;
  if (s.includes("organic") || s.includes("seo") || s.includes("search")) return Globe2;
  if (s.includes("direct") || s.includes("referr") || s.includes("link")) return Link2;
  // Generic fallback: a hollow grey circle (no specific source signal to show).
  return Circle;
}

interface PlatformBadgeProps {
  /** Platform key from platformFromRow(): "google" | "meta" | "facebook" | "tiktok" | "" */
  platform: string;
  /** Explicit label override (used for non-platform sources — pass the row's own name). */
  label?: string;
  /** Raw name / id used for the title + aria tooltip. */
  rawName?: string;
  /** Show the text label next to the logo. Off = logo-only (nested rows / filter pills). */
  showLabel?: boolean;
  /** Logo chip size in px. */
  size?: number;
  /** Override the label typography. */
  labelClassName?: string;
}

/**
 * Small rounded brand logo + canonical platform name, Hyros-style.
 */
export default function PlatformBadge({
  platform,
  label,
  rawName,
  showLabel = true,
  size = 18,
  labelClassName,
}: PlatformBadgeProps) {
  const key = (platform || "").toLowerCase();
  const displayLabel = label ?? platformDisplayName(key);
  const aria = rawName || displayLabel;

  let glyph: React.ReactNode;
  let bg = "var(--surface-2)";
  let border: string | undefined;

  if (key === "google") {
    glyph = <GoogleGlyph size={Math.round(size * 0.82)} />;
    bg = "#ffffff";
  } else if (key === "meta" || key === "facebook") {
    glyph = <MetaGlyph size={size} />;
    bg = "#0866FF";
  } else if (key === "tiktok") {
    glyph = <TikTokGlyph size={size} />;
    bg = "#000000";
  } else {
    const Icon = fallbackIcon(rawName || label || "");
    // The generic default (no contextual match) reads as a hollow grey circle;
    // the contextual icons (Mail/Globe2/Link2) keep the existing dim tint.
    const iconColorCls = Icon === Circle ? "text-[#6f7380]" : "text-ink-dim";
    glyph = <Icon size={Math.round(size * 0.58)} className={iconColorCls} strokeWidth={2} />;
    bg = "#17171f";
    border = "1px solid #262631";
  }

  return (
    <span className="inline-flex min-w-0 items-center gap-2" title={aria}>
      <span
        role="img"
        aria-label={aria}
        className="inline-flex shrink-0 items-center justify-center overflow-hidden"
        style={{
          width: size,
          height: size,
          borderRadius: Math.max(4, Math.round(size * 0.28)),
          background: bg,
          border,
        }}
      >
        {glyph}
      </span>
      {showLabel && (
        <span className={labelClassName ?? "truncate text-[13px] font-semibold text-ink-bright"}>
          {displayLabel}
        </span>
      )}
    </span>
  );
}
