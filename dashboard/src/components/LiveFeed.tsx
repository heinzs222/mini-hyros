"use client";

import { useEffect, useRef, useState } from "react";
import { Radio, ShoppingCart, MousePointerClick } from "lucide-react";

interface LiveEvent {
  type: string;
  ts: string;
  order_id?: string;
  gross?: number;
  session_id?: string;
  utm_source?: string;
  customer_key?: string;
}

interface Props {
  events: LiveEvent[];
  connected: boolean;
}

export default function LiveFeed({ events, connected }: Props) {
  const listRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (listRef.current) {
      listRef.current.scrollTop = listRef.current.scrollHeight;
    }
  }, [events]);

  return (
    <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-4">
      <div className="flex items-center gap-2 mb-3">
        <Radio size={14} className={connected ? "text-emerald-400 animate-pulse" : "text-gray-600"} />
        <h3 className="text-sm font-semibold text-gray-300">Live Events</h3>
        <span className={`ml-auto text-[10px] px-2 py-0.5 rounded-full ${connected ? "bg-emerald-500/10 text-emerald-400" : "bg-gray-700 text-gray-500"}`}>
          {connected ? "CONNECTED" : "DISCONNECTED"}
        </span>
      </div>
      <div ref={listRef} className="space-y-1.5 max-h-48 overflow-y-auto">
        {events.length === 0 && (
          <div className="text-xs text-gray-600 text-center py-6">
            Waiting for live events...
          </div>
        )}
        {events.map((ev, i) => (
          <div
            key={i}
            className="flex items-center gap-2 text-xs px-2 py-1.5 rounded-lg bg-white/[0.02] animate-[fadeIn_0.3s_ease-out]"
          >
            {ev.type === "new_order" ? (
              <ShoppingCart size={12} className="text-emerald-400 flex-shrink-0" />
            ) : (
              <MousePointerClick size={12} className="text-blue-400 flex-shrink-0" />
            )}
            <span className="text-gray-400 flex-shrink-0">
              {new Date(ev.ts).toLocaleTimeString()}
            </span>
            {ev.type === "new_order" ? (
              <span className="text-gray-300">
                Order <span className="text-emerald-400 font-medium">${ev.gross?.toFixed(2)}</span>
              </span>
            ) : (
              <span className="text-gray-300">
                Session from <span className="text-blue-400">{ev.utm_source || "direct"}</span>
              </span>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
