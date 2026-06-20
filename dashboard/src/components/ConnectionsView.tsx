"use client";

import { useEffect, useState } from "react";
import {
  CheckCircle2,
  AlertTriangle,
  XCircle,
  Circle,
  RefreshCw,
  ExternalLink,
  Plug,
  Webhook,
} from "lucide-react";
import { fetchConnections, fetchTikTokConnectUrl, refreshTikTokToken, connectGhl } from "@/lib/api";
import { useToast } from "@/components/Toast";

type ConnState = "connected" | "expired" | "invalid" | "error" | "not_configured" | "unknown";

interface Platform {
  platform: string;
  label: string;
  configured: boolean;
  fields: Record<string, boolean>;
  required_env: string[];
  state: ConnState;
  detail: string;
  checked_at: string | null;
}

const STATE_META: Record<ConnState, { label: string; cls: string; icon: React.ReactNode }> = {
  connected: { label: "Connected", cls: "text-emerald-400 bg-emerald-500/10 border-emerald-500/30", icon: <CheckCircle2 size={14} /> },
  expired: { label: "Token expired", cls: "text-amber-400 bg-amber-500/10 border-amber-500/30", icon: <AlertTriangle size={14} /> },
  invalid: { label: "Invalid / missing scope", cls: "text-rose-400 bg-rose-500/10 border-rose-500/30", icon: <XCircle size={14} /> },
  error: { label: "Could not verify", cls: "text-amber-400 bg-amber-500/10 border-amber-500/30", icon: <AlertTriangle size={14} /> },
  not_configured: { label: "Not configured", cls: "text-ink-faint bg-white/5 border-[var(--card-border)]", icon: <Circle size={14} /> },
  unknown: { label: "Unknown", cls: "text-ink-faint bg-white/5 border-[var(--card-border)]", icon: <Circle size={14} /> },
};

function fmtChecked(iso: string | null): string {
  if (!iso) return "not checked";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "not checked";
  return `checked ${d.toLocaleTimeString()}`;
}

export default function ConnectionsView() {
  const toast = useToast();
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [tiktokBusy, setTiktokBusy] = useState(false);
  const [ghl, setGhl] = useState({ token: "", location: "", busy: false });

  const load = async (notify = false) => {
    setLoading(true);
    const toastId = notify ? toast.loading("Re-checking connections…") : 0;
    try {
      const res = await fetchConnections(true);
      setData(res);
      if (notify) {
        const s = res?.summary || {};
        if (s.needs_attention > 0) {
          toast.update(toastId, {
            type: "error",
            title: `${s.needs_attention} connection${s.needs_attention === 1 ? "" : "s"} need attention`,
            description: (res.platforms || [])
              .filter((p: Platform) => ["expired", "invalid", "error"].includes(p.state))
              .map((p: Platform) => `${p.label}: ${STATE_META[p.state].label}`)
              .join("\n"),
            duration: 11000,
          });
        } else {
          toast.update(toastId, { type: "success", title: "All connections healthy", description: `${s.connected}/${s.total} platforms connected.`, duration: 4500 });
        }
      }
    } catch (e: any) {
      if (notify) toast.update(toastId, { type: "error", title: "Couldn’t check connections", description: e?.message || "Request failed." });
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void load(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const reconnectTikTok = async () => {
    setTiktokBusy(true);
    try {
      const res = await fetchTikTokConnectUrl();
      if (res?.url) {
        window.open(res.url, "_blank", "noopener");
        toast.info("Opening TikTok authorization", { description: "Approve the requested scopes, then re-check." });
      } else {
        toast.error("TikTok connect unavailable", { description: res?.error || "No authorization URL returned." });
      }
    } catch (e: any) {
      toast.error("TikTok connect failed", { description: e?.message || "Could not start TikTok OAuth." });
    } finally {
      setTiktokBusy(false);
    }
  };

  const refreshTikTok = async () => {
    setTiktokBusy(true);
    try {
      const r = await refreshTikTokToken();
      if (r?.refreshed) {
        toast.success("TikTok token refreshed");
        await load(false);
      } else {
        toast.error("TikTok refresh failed", { description: r?.error || "Token could not be refreshed." });
      }
    } catch (e: any) {
      toast.error("TikTok refresh failed", { description: e?.message || "Request failed." });
    } finally {
      setTiktokBusy(false);
    }
  };

  const connectGhlHandler = async () => {
    if (!ghl.token.trim() || !ghl.location.trim()) {
      toast.error("Missing GHL details", { description: "Enter both the API token and Location ID." });
      return;
    }
    setGhl((g) => ({ ...g, busy: true }));
    try {
      const res = await connectGhl({ api_token: ghl.token.trim(), location_id: ghl.location.trim() });
      if (res?.connected) {
        toast.success("GoHighLevel connected", { description: "Leads & opportunities will sync on the next refresh." });
        setGhl({ token: "", location: "", busy: false });
        await load(false);
      } else {
        toast.error("GHL connection failed", { description: res?.error || "Could not validate token." });
        setGhl((g) => ({ ...g, busy: false }));
      }
    } catch (e: any) {
      toast.error("GHL connection failed", { description: e?.message || "Request failed." });
      setGhl((g) => ({ ...g, busy: false }));
    }
  };

  const platforms: Platform[] = data?.platforms || [];
  const webhooks = data?.webhooks || {};
  const summary = data?.summary || { connected: 0, configured: 0, needs_attention: 0, total: 0 };

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="text-[13px] text-ink-dim">
            Live status of every ad platform, Stripe, and your webhooks. “Connected” here means the
            credentials were verified against the platform’s API — not just that an env var is set.
          </p>
        </div>
        <button
          onClick={() => void load(true)}
          disabled={loading}
          className="flex h-9 items-center gap-1.5 rounded-lg border border-[var(--card-border)] bg-[var(--surface)] px-3 text-[13px] font-medium text-ink transition-colors hover:bg-white/5 disabled:opacity-50"
        >
          <RefreshCw size={14} className={loading ? "animate-spin" : ""} /> Re-check all
        </button>
      </div>

      {/* Summary */}
      <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
        {[
          { label: "Connected", value: summary.connected, color: "text-emerald-400" },
          { label: "Configured", value: summary.configured, color: "text-ink-bright" },
          { label: "Need attention", value: summary.needs_attention, color: summary.needs_attention > 0 ? "text-amber-400" : "text-ink-bright" },
          { label: "Platforms", value: summary.total, color: "text-ink-bright" },
        ].map((c) => (
          <div key={c.label} className="hpanel p-4">
            <div className="h-label uppercase text-ink-dim">{c.label}</div>
            <div className={`h-num mt-1 text-[24px] ${c.color}`}>{c.value}</div>
          </div>
        ))}
      </div>

      {/* Platform cards */}
      <div className="hpanel p-5">
        <h3 className="mb-4 flex items-center gap-2 text-sm font-semibold text-ink-bright">
          <Plug size={15} className="text-brand-400" /> Platform connections
        </h3>

        {loading && !data ? (
          <div className="py-10 text-center text-ink-dim">
            <RefreshCw size={20} className="mx-auto animate-spin text-brand-500" />
            <div className="mt-2 text-[13px]">Verifying credentials…</div>
          </div>
        ) : (
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
            {platforms.map((p) => {
              const meta = STATE_META[p.state] || STATE_META.unknown;
              const needsAction = ["expired", "invalid", "error", "not_configured"].includes(p.state);
              return (
                <div key={p.platform} className="rounded-xl border border-[var(--card-border)] bg-white/[0.01] p-4">
                  <div className="flex items-center justify-between">
                    <span className="text-[15px] font-semibold text-ink-bright">{p.label}</span>
                    <span className={`inline-flex items-center gap-1.5 rounded-md border px-2 py-0.5 text-[11px] font-medium ${meta.cls}`}>
                      {meta.icon} {meta.label}
                    </span>
                  </div>
                  {p.detail && <div className="mt-2 text-[12px] leading-snug text-ink-dim">{p.detail}</div>}

                  {/* Credential checklist */}
                  <div className="mt-3 flex flex-wrap gap-1.5">
                    {Object.entries(p.fields).map(([label, set]) => (
                      <span
                        key={label}
                        className={`inline-flex items-center gap-1 rounded px-1.5 py-0.5 text-[10px] font-mono ${set ? "bg-emerald-500/10 text-emerald-300" : "bg-rose-500/10 text-rose-300"}`}
                        title={set ? "Set" : "Missing"}
                      >
                        {set ? "✓" : "✗"} {label}
                      </span>
                    ))}
                  </div>

                  <div className="mt-3 flex items-center justify-between">
                    <span className="text-[11px] text-ink-faint">{fmtChecked(p.checked_at)}</span>
                    {p.platform === "tiktok" && needsAction && (
                      <div className="flex items-center gap-2">
                        <button
                          onClick={refreshTikTok}
                          disabled={tiktokBusy}
                          className="rounded-md border border-[var(--card-border)] px-2.5 py-1 text-[12px] text-ink-dim transition-colors hover:text-ink disabled:opacity-50"
                        >
                          Refresh token
                        </button>
                        <button
                          onClick={reconnectTikTok}
                          disabled={tiktokBusy}
                          className="flex items-center gap-1 rounded-md bg-brand-600 px-2.5 py-1 text-[12px] font-medium text-white transition-colors hover:bg-brand-700 disabled:opacity-50"
                        >
                          Reconnect <ExternalLink size={11} />
                        </button>
                      </div>
                    )}
                  </div>

                  {needsAction && p.required_env?.length > 0 && p.platform !== "tiktok" && p.platform !== "ghl" && (
                    <div className="mt-2 rounded-lg bg-white/[0.02] p-2 text-[10px] text-ink-faint">
                      Set in environment: <span className="font-mono text-ink-dim">{p.required_env.join(", ")}</span>
                    </div>
                  )}

                  {p.platform === "ghl" && (
                    <div className="mt-3 space-y-2 rounded-lg border border-[var(--card-border)] bg-white/[0.02] p-2.5">
                      <div className="text-[10px] uppercase tracking-wide text-ink-dim">
                        Connect via Private Integration token
                      </div>
                      <input
                        type="password"
                        value={ghl.token}
                        onChange={(e) => setGhl((g) => ({ ...g, token: e.target.value }))}
                        placeholder="API token (pit-…)"
                        className="w-full rounded-md border border-[var(--card-border)] bg-[var(--surface-2)] px-2.5 py-1.5 text-[12px] text-ink placeholder:text-ink-faint focus:border-brand-500 focus:outline-none"
                      />
                      <input
                        type="text"
                        value={ghl.location}
                        onChange={(e) => setGhl((g) => ({ ...g, location: e.target.value }))}
                        placeholder="Location ID"
                        className="w-full rounded-md border border-[var(--card-border)] bg-[var(--surface-2)] px-2.5 py-1.5 text-[12px] text-ink placeholder:text-ink-faint focus:border-brand-500 focus:outline-none"
                      />
                      <button
                        onClick={connectGhlHandler}
                        disabled={ghl.busy}
                        className="flex w-full items-center justify-center gap-1.5 rounded-md bg-brand-600 px-2.5 py-1.5 text-[12px] font-medium text-white transition-colors hover:bg-brand-700 disabled:opacity-50"
                      >
                        {ghl.busy ? <RefreshCw size={12} className="animate-spin" /> : null}
                        {ghl.busy ? "Connecting…" : "Connect GoHighLevel"}
                      </button>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Webhooks */}
      <div className="hpanel p-5">
        <h3 className="mb-4 flex items-center gap-2 text-sm font-semibold text-ink-bright">
          <Webhook size={15} className="text-brand-400" /> Inbound webhooks
        </h3>
        <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
          {Object.entries(webhooks).map(([name, info]: [string, any]) => (
            <div key={name} className="rounded-xl border border-[var(--card-border)] bg-white/[0.01] p-4">
              <div className="flex items-center justify-between">
                <span className="text-[15px] font-semibold capitalize text-ink-bright">{name}</span>
                <span className={`inline-flex items-center gap-1.5 rounded-md border px-2 py-0.5 text-[11px] font-medium ${info.configured ? STATE_META.connected.cls : STATE_META.not_configured.cls}`}>
                  {info.configured ? <CheckCircle2 size={14} /> : <Circle size={14} />}
                  {info.configured ? "Secret set" : "No secret"}
                </span>
              </div>
              <div className="mt-2 font-mono text-[11px] text-ink-faint">{info.endpoint}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
