"use client";

import { useSyncExternalStore } from "react";
import { subscribeApiActivity, getApiActivitySnapshot } from "@/lib/api";

/**
 * A slim indeterminate progress bar pinned to the very top of the viewport that
 * animates whenever ANY API request is in flight. Because every network call
 * funnels through apiFetch's activity counter, this single element gives the
 * whole app (dashboard, reports, CRM, sync, feature panels) a consistent
 * "something is loading" cue — the piece the app was missing entirely.
 */
export default function TopProgressBar() {
  const active = useSyncExternalStore(
    subscribeApiActivity,
    getApiActivitySnapshot,
    () => 0,
  );
  const busy = active > 0;

  return (
    <div
      aria-hidden={!busy}
      role="progressbar"
      aria-busy={busy}
      className={`pointer-events-none fixed inset-x-0 top-0 z-[100] h-[2px] overflow-hidden transition-opacity duration-200 ${
        busy ? "opacity-100" : "opacity-0"
      }`}
    >
      <div className="animate-progress-sweep h-full w-1/3 rounded-r-full bg-gradient-to-r from-transparent via-brand-400 to-brand-500 shadow-[0_0_8px_rgba(139,92,246,0.6)]" />
    </div>
  );
}
