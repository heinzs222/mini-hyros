"use client";

import { fetchCampaigns, setCampaignTracking, setCampaignTrackingBatch } from "@/lib/api";
import { RefreshCw, EyeOff, Eye } from "lucide-react";
import { useCallback, useEffect, useState } from "react";
import { useToast } from "@/components/Toast";
import { formatMoney } from "@/lib/utils";

interface CampaignRow {
  platform: string;
  campaign_id: string;
  name: string;
  tracked: boolean;
  lifetime_spend: number;
  last_seen: string;
}

const rowKey = (r: { platform: string; campaign_id: string }) => `${r.platform}|${r.campaign_id}`;

function platformBadge(p: string): string {
  const colors: Record<string, string> = {
    meta: "bg-blue-500/20 text-blue-400",
    google: "bg-green-500/20 text-green-400",
    tiktok: "bg-gray-500/20 text-gray-300",
    ghl: "bg-purple-500/20 text-purple-400",
  };
  return colors[p] || "bg-gray-500/20 text-gray-400";
}

function fmtLastSeen(value: string): string {
  if (!value) return "—";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value.slice(0, 10);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}

export default function CampaignTrackingPanel({ onChange }: { onChange?: () => void }) {
  const toast = useToast();
  const [rows, setRows] = useState<CampaignRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [savingBulk, setSavingBulk] = useState(false);

  const load = useCallback(async (alive?: () => boolean) => {
    setLoading(true);
    try {
      const data = await fetchCampaigns();
      if (alive && !alive()) return;
      setRows((data?.campaigns || []) as CampaignRow[]);
    } catch (err: any) {
      if (alive && !alive()) return;
      toast.error("Couldn’t load campaigns", { description: err?.message || "The campaign list could not be loaded." });
    } finally {
      if (!alive || alive()) setLoading(false);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    let alive = true;
    void load(() => alive);
    return () => {
      alive = false;
    };
  }, [load]);

  const handleToggle = async (row: CampaignRow) => {
    const next = !row.tracked;
    // Optimistic update.
    setRows((prev) => prev.map((r) => (rowKey(r) === rowKey(row) ? { ...r, tracked: next } : r)));
    try {
      await setCampaignTracking(row.platform, row.campaign_id, next);
      onChange?.();
    } catch (err: any) {
      // Revert on failure.
      setRows((prev) => prev.map((r) => (rowKey(r) === rowKey(row) ? { ...r, tracked: row.tracked } : r)));
      toast.error("Couldn’t update tracking", { description: err?.message || "The change could not be saved." });
    }
  };

  const bulkSet = async (tracked: boolean) => {
    const targets = rows.filter((r) => selected.has(rowKey(r)));
    if (targets.length === 0) {
      toast.info("Nothing selected", { description: "Select one or more campaigns first." });
      return;
    }
    const items = targets.map((r) => ({ platform: r.platform, campaign_id: r.campaign_id, tracked }));
    const prevRows = rows;
    setSavingBulk(true);
    // Optimistic update.
    setRows((prev) => prev.map((r) => (selected.has(rowKey(r)) ? { ...r, tracked } : r)));
    try {
      await setCampaignTrackingBatch(items);
      toast.success(tracked ? "Campaigns included" : "Campaigns excluded", {
        description: `${items.length} campaign${items.length === 1 ? "" : "s"} ${tracked ? "included in" : "excluded from"} reports.`,
      });
      setSelected(new Set());
      onChange?.();
    } catch (err: any) {
      setRows(prevRows);
      toast.error("Couldn’t update campaigns", { description: err?.message || "The changes could not be saved." });
    } finally {
      setSavingBulk(false);
    }
  };

  const toggleSelect = (row: CampaignRow) => {
    setSelected((prev) => {
      const next = new Set(prev);
      const k = rowKey(row);
      if (next.has(k)) next.delete(k);
      else next.add(k);
      return next;
    });
  };

  const allSelected = rows.length > 0 && rows.every((r) => selected.has(rowKey(r)));
  const toggleSelectAll = () => {
    setSelected(() => (allSelected ? new Set() : new Set(rows.map(rowKey))));
  };

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-4">
        <div className="flex items-center justify-between mb-3">
          <div>
            <h3 className="text-sm font-semibold text-white">Campaign Tracking</h3>
            <p className="text-[11px] text-gray-500 mt-0.5">
              Untracked campaigns are excluded from spend, ROAS, and all reports (e.g. exclude Recrutement).
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => bulkSet(false)}
              disabled={savingBulk || selected.size === 0}
              className="flex items-center gap-1 px-3 py-1.5 rounded-lg bg-white/5 hover:bg-white/10 text-gray-300 text-xs font-medium transition-colors disabled:opacity-50"
            >
              <EyeOff size={12} /> Exclude selected
            </button>
            <button
              onClick={() => bulkSet(true)}
              disabled={savingBulk || selected.size === 0}
              className="flex items-center gap-1 px-3 py-1.5 rounded-lg bg-emerald-600 hover:bg-emerald-700 text-white text-xs font-medium transition-colors disabled:opacity-50"
            >
              <Eye size={12} /> Include selected
            </button>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => load()}
            className="flex items-center gap-1 px-2 py-1 text-xs text-gray-400 hover:text-white"
          >
            <RefreshCw size={11} className={loading ? "animate-spin" : ""} /> Refresh
          </button>
          {selected.size > 0 && (
            <span className="text-[11px] text-brand-300">{selected.size} selected</span>
          )}
          <span className="ml-auto text-[11px] text-gray-600">{rows.length} campaigns</span>
        </div>
      </div>

      {/* Table */}
      <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-[var(--card-border)]">
                <th className="w-10 px-4 py-2">
                  <input
                    type="checkbox"
                    className="accent-brand-500"
                    aria-label="Select all campaigns"
                    checked={allSelected}
                    onChange={toggleSelectAll}
                    disabled={rows.length === 0}
                  />
                </th>
                <th className="text-left px-3 py-2 text-gray-500 font-medium">Platform</th>
                <th className="text-left px-3 py-2 text-gray-500 font-medium min-w-[200px]">Campaign</th>
                <th className="text-left px-3 py-2 text-gray-500 font-medium">Campaign ID</th>
                <th className="text-right px-3 py-2 text-gray-500 font-medium">Lifetime Spend</th>
                <th className="text-left px-3 py-2 text-gray-500 font-medium">Last Seen</th>
                <th className="text-right px-3 py-2 text-gray-500 font-medium">Tracked</th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={7} className="px-4 py-10 text-center text-gray-600">
                    <RefreshCw size={18} className="mx-auto animate-spin text-brand-500" />
                  </td>
                </tr>
              ) : rows.length === 0 ? (
                <tr>
                  <td colSpan={7} className="px-4 py-8 text-center text-gray-600">
                    No campaigns yet. They appear here once ad spend is ingested.
                  </td>
                </tr>
              ) : (
                rows.map((row) => {
                  const k = rowKey(row);
                  return (
                    <tr
                      key={k}
                      className={`border-b border-[var(--card-border)] hover:bg-white/[0.02] ${row.tracked ? "" : "opacity-60"}`}
                    >
                      <td className="px-4 py-2">
                        <input
                          type="checkbox"
                          className="accent-brand-500"
                          aria-label={`Select ${row.name}`}
                          checked={selected.has(k)}
                          onChange={() => toggleSelect(row)}
                        />
                      </td>
                      <td className="px-3 py-2">
                        <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${platformBadge(row.platform)}`}>
                          {row.platform}
                        </span>
                      </td>
                      <td className="px-3 py-2 text-gray-200">{row.name || "—"}</td>
                      <td className="px-3 py-2 text-gray-500 font-mono text-[11px]">{row.campaign_id}</td>
                      <td className="px-3 py-2 text-right text-gray-300 tabular-nums">{formatMoney(row.lifetime_spend)}</td>
                      <td className="px-3 py-2 text-gray-500">{fmtLastSeen(row.last_seen)}</td>
                      <td className="px-3 py-2">
                        <div className="flex justify-end">
                          <button
                            type="button"
                            role="switch"
                            aria-checked={row.tracked}
                            aria-label={row.tracked ? `Stop tracking ${row.name}` : `Track ${row.name}`}
                            onClick={() => handleToggle(row)}
                            className={`relative inline-flex h-[20px] w-[36px] items-center rounded-full transition-colors ${row.tracked ? "bg-emerald-500" : "bg-white/15"}`}
                          >
                            <span className={`inline-block h-[14px] w-[14px] transform rounded-full bg-white transition-transform ${row.tracked ? "translate-x-[19px]" : "translate-x-[3px]"}`} />
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
