"use client";
import { useEffect, useState } from "react";
import { fetchCapiStatus, triggerCapiSync, fetchCapiLog } from "@/lib/api";
import { Send, CheckCircle, XCircle, AlertTriangle } from "lucide-react";

export default function CapiPanel() {
  const [status, setStatus] = useState<any>(null);
  const [log, setLog] = useState<any[]>([]);
  const [syncing, setSyncing] = useState(false);
  const [syncResult, setSyncResult] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  async function load() {
    setLoading(true);
    try {
      const [s, l] = await Promise.all([fetchCapiStatus(), fetchCapiLog(20)]);
      setStatus(s);
      setLog(l.rows || []);
    } catch {}
    setLoading(false);
  }

  useEffect(() => { load(); }, []);

  async function handleSync() {
    setSyncing(true);
    setSyncResult(null);
    try {
      const result = await triggerCapiSync();
      setSyncResult(result);
      await load();
    } catch (e: any) {
      setSyncResult({ error: e.message });
    }
    setSyncing(false);
  }

  if (loading) return <div className="text-center py-12 text-gray-500 text-sm">Loading CAPI status...</div>;

  const platforms = status?.platforms || {};

  return (
    <div className="space-y-6">
      {/* Sync Summary */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        {[
          { label: "Total Conversions", value: status?.total_conversions || 0 },
          { label: "Synced to Platforms", value: status?.synced || 0 },
          { label: "Unsynced", value: status?.unsynced || 0 },
          { label: "Platforms Connected", value: Object.values(platforms).filter((p: any) => p.configured).length },
        ].map((c, i) => (
          <div key={i} className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-4">
            <div className="text-[11px] text-gray-500 uppercase tracking-wider">{c.label}</div>
            <div className="text-xl font-bold text-white mt-1">{c.value}</div>
          </div>
        ))}
      </div>

      {/* Platform Status */}
      <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-6">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-sm font-semibold text-white flex items-center gap-2">
            <Send size={14} className="text-brand-400" /> Conversion API Status
          </h3>
          <button
            onClick={handleSync}
            disabled={syncing}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-brand-600 hover:bg-brand-700 text-white text-xs font-medium transition-colors disabled:opacity-50"
          >
            <Send size={12} className={syncing ? "animate-pulse" : ""} />
            {syncing ? "Syncing..." : "Sync Now"}
          </button>
        </div>

        {syncResult && (
          <div className={`mb-4 p-3 rounded-lg text-xs ${syncResult.error ? "bg-red-500/10 text-red-400" : "bg-emerald-500/10 text-emerald-400"}`}>
            {syncResult.error
              ? `Sync error: ${syncResult.error}`
              : `Synced ${syncResult.pushed || 0} conversions. Failed: ${syncResult.failed || 0}. Skipped: ${syncResult.skipped || 0}.`}
          </div>
        )}

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {Object.entries(platforms).map(([name, info]: [string, any]) => (
            <div key={name} className="rounded-lg border border-[var(--card-border)] p-4 bg-white/[0.01]">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm font-medium text-white capitalize">{name}</span>
                {info.configured ? (
                  <CheckCircle size={14} className="text-emerald-400" />
                ) : (
                  <XCircle size={14} className="text-gray-600" />
                )}
              </div>
              <div className={`text-xs ${info.configured ? "text-emerald-400" : "text-gray-500"}`}>
                {info.configured ? "Connected" : "Not configured"}
              </div>
              {!info.configured && (
                <div className="mt-2 text-[10px] text-gray-600">
                  Set: {info.env_vars?.join(", ")}
                </div>
              )}
              {info.stats && (
                <div className="mt-2 text-[10px] text-gray-500 space-y-0.5">
                  {Object.entries(info.stats).map(([k, v]: [string, any]) => (
                    <div key={k} className="flex justify-between">
                      <span>{k}</span>
                      <span className={k === "success" ? "text-emerald-400" : "text-red-400"}>{v}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          ))}
        </div>
      </div>

      {/* Push Log */}
      {log.length > 0 && (
        <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] overflow-hidden">
          <div className="p-4 border-b border-[var(--card-border)]">
            <h3 className="text-sm font-semibold text-white">Sync Log</h3>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-[var(--card-border)] text-gray-500">
                  <th className="text-left p-3 font-medium">Time</th>
                  <th className="text-left p-3 font-medium">Platform</th>
                  <th className="text-left p-3 font-medium">Event</th>
                  <th className="text-right p-3 font-medium">Value</th>
                  <th className="text-center p-3 font-medium">Status</th>
                </tr>
              </thead>
              <tbody>
                {log.map((entry: any, i: number) => (
                  <tr key={i} className="border-b border-[var(--card-border)]">
                    <td className="p-3 text-gray-500">{entry.ts?.slice(0, 16)}</td>
                    <td className="p-3 text-gray-300 capitalize">{entry.platform}</td>
                    <td className="p-3 text-gray-400">{entry.event_name}</td>
                    <td className="p-3 text-right text-gray-300">${Number(entry.value || 0).toLocaleString()}</td>
                    <td className="p-3 text-center">
                      <span className={`px-2 py-0.5 rounded text-[10px] font-medium ${
                        entry.status === "success" ? "bg-emerald-500/10 text-emerald-400" :
                        entry.status === "failed" ? "bg-red-500/10 text-red-400" :
                        "bg-yellow-500/10 text-yellow-400"
                      }`}>{entry.status}</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
