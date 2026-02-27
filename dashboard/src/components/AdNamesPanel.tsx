"use client";

import { fetchAdNames, upsertAdName, deleteAdName, syncAdNames } from "@/lib/api";
import { RefreshCw, Plus, Trash2, Download, Edit2, Check, X } from "lucide-react";
import { useEffect, useState, useCallback } from "react";

interface NameRow {
  platform: string;
  entity_type: string;
  entity_id: string;
  name: string;
  parent_id: string;
  source: string;
  updated_at: string;
}

const PLATFORMS = ["meta", "google", "tiktok", "ghl"];
const SYNC_PLATFORMS = ["meta", "google", "tiktok"];
const ENTITY_TYPES = ["campaign", "adset", "ad", "funnel"];

export default function AdNamesPanel() {
  const [rows, setRows] = useState<NameRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [syncing, setSyncing] = useState(false);
  const [syncResult, setSyncResult] = useState<any>(null);
  const [filterPlatform, setFilterPlatform] = useState("");
  const [filterType, setFilterType] = useState("");
  const [editingKey, setEditingKey] = useState<string | null>(null);
  const [editName, setEditName] = useState("");
  const [showAdd, setShowAdd] = useState(false);
  const [newMapping, setNewMapping] = useState({
    platform: "meta",
    entity_type: "campaign",
    entity_id: "",
    name: "",
    parent_id: "",
  });

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const data = await fetchAdNames(filterPlatform, filterType);
      setRows(data.rows || []);
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  }, [filterPlatform, filterType]);

  useEffect(() => { load(); }, [load]);

  const handleSync = async (platform = "all") => {
    setSyncing(true);
    setSyncResult(null);
    try {
      const result = await syncAdNames(platform);
      setSyncResult(result);
      await load();
    } catch (err: any) {
      setSyncResult({ errors: [err.message] });
    } finally {
      setSyncing(false);
    }
  };

  const handleAdd = async () => {
    if (!newMapping.entity_id || !newMapping.name) return;
    try {
      await upsertAdName(newMapping);
      setNewMapping({ platform: "meta", entity_type: "campaign", entity_id: "", name: "", parent_id: "" });
      setShowAdd(false);
      await load();
    } catch (err) {
      console.error(err);
    }
  };

  const handleDelete = async (row: NameRow) => {
    if (!confirm(`Delete "${row.name}" (${row.entity_id})?`)) return;
    try {
      await deleteAdName({ platform: row.platform, entity_type: row.entity_type, entity_id: row.entity_id });
      await load();
    } catch (err) {
      console.error(err);
    }
  };

  const startEdit = (row: NameRow) => {
    setEditingKey(`${row.platform}|${row.entity_type}|${row.entity_id}`);
    setEditName(row.name);
  };

  const saveEdit = async (row: NameRow) => {
    try {
      await upsertAdName({
        platform: row.platform,
        entity_type: row.entity_type,
        entity_id: row.entity_id,
        name: editName,
        parent_id: row.parent_id,
      });
      setEditingKey(null);
      await load();
    } catch (err) {
      console.error(err);
    }
  };

  const rowKey = (r: NameRow) => `${r.platform}|${r.entity_type}|${r.entity_id}`;

  const platformBadge = (p: string) => {
    const colors: Record<string, string> = {
      meta: "bg-blue-500/20 text-blue-400",
      google: "bg-green-500/20 text-green-400",
      tiktok: "bg-gray-500/20 text-gray-300",
    };
    return colors[p] || "bg-gray-500/20 text-gray-400";
  };

  const typeBadge = (t: string) => {
    const colors: Record<string, string> = {
      campaign: "bg-purple-500/20 text-purple-400",
      adset: "bg-cyan-500/20 text-cyan-400",
      ad: "bg-emerald-500/20 text-emerald-400",
      funnel: "bg-orange-500/20 text-orange-400",
    };
    return colors[t] || "bg-gray-500/20 text-gray-400";
  };

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-4">
        <div className="flex items-center justify-between mb-3">
          <div>
            <h3 className="text-sm font-semibold text-white">Ad Name Mappings</h3>
            <p className="text-[11px] text-gray-500 mt-0.5">
              Map campaign/adset/ad/funnel IDs to human-readable names. Sync from APIs or add manually.
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setShowAdd(!showAdd)}
              className="flex items-center gap-1 px-3 py-1.5 rounded-lg bg-brand-600 hover:bg-brand-700 text-white text-xs font-medium transition-colors"
            >
              <Plus size={12} /> Add
            </button>
            <button
              onClick={() => handleSync("all")}
              disabled={syncing}
              className="flex items-center gap-1 px-3 py-1.5 rounded-lg bg-emerald-600 hover:bg-emerald-700 text-white text-xs font-medium transition-colors disabled:opacity-50"
            >
              <Download size={12} className={syncing ? "animate-spin" : ""} />
              {syncing ? "Syncing..." : "Sync from APIs"}
            </button>
          </div>
        </div>

        {/* Sync result */}
        {syncResult && (
          <div className={`rounded-lg p-3 text-xs mb-3 ${syncResult.errors?.length ? "bg-yellow-500/10 text-yellow-400" : "bg-emerald-500/10 text-emerald-400"}`}>
            {syncResult.synced != null && <span>Synced {syncResult.synced} names. </span>}
            {syncResult.errors?.map((e: string, i: number) => (
              <span key={i} className="block text-yellow-500">{e}</span>
            ))}
            {Object.entries(syncResult.platforms || {}).map(([p, r]: [string, any]) => (
              <span key={p} className="block">
                {p}: {r.synced || 0} synced
                {r.campaigns != null && ` (${r.campaigns} campaigns, ${r.adsets || r.adgroups || 0} adsets, ${r.ads || 0} ads)`}
                {r.error && ` — ${r.error}`}
              </span>
            ))}
          </div>
        )}

        {/* Sync individual platforms */}
        <div className="flex gap-2 mb-3">
          {SYNC_PLATFORMS.map((p) => (
            <button
              key={p}
              onClick={() => handleSync(p)}
              disabled={syncing}
              className="px-2.5 py-1 rounded-md bg-white/5 hover:bg-white/10 text-[11px] text-gray-400 transition-colors disabled:opacity-50 capitalize"
            >
              Sync {p}
            </button>
          ))}
        </div>

        {/* Add form */}
        {showAdd && (
          <div className="rounded-lg border border-[var(--card-border)] bg-white/[0.02] p-3 mb-3">
            <p className="text-[11px] text-gray-500 mb-2">
              Tip: for funnel names, set <code className="text-gray-300">type=funnel</code> and ID as the first URL segment (e.g. <code className="text-gray-300">/book-call</code>).
            </p>
            <div className="grid grid-cols-2 md:grid-cols-5 gap-2">
              <select
                value={newMapping.platform}
                onChange={(e) => setNewMapping({ ...newMapping, platform: e.target.value })}
                className="bg-[var(--card)] border border-[var(--card-border)] rounded-md px-2 py-1.5 text-xs text-gray-300"
              >
                {PLATFORMS.map((p) => <option key={p} value={p}>{p}</option>)}
              </select>
              <select
                value={newMapping.entity_type}
                onChange={(e) => setNewMapping({ ...newMapping, entity_type: e.target.value })}
                className="bg-[var(--card)] border border-[var(--card-border)] rounded-md px-2 py-1.5 text-xs text-gray-300"
              >
                {ENTITY_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
              </select>
              <input
                type="text"
                placeholder="Entity ID (e.g. 123456)"
                value={newMapping.entity_id}
                onChange={(e) => setNewMapping({ ...newMapping, entity_id: e.target.value })}
                className="bg-[var(--card)] border border-[var(--card-border)] rounded-md px-2 py-1.5 text-xs text-gray-300 placeholder-gray-600"
              />
              <input
                type="text"
                placeholder="Display Name"
                value={newMapping.name}
                onChange={(e) => setNewMapping({ ...newMapping, name: e.target.value })}
                className="bg-[var(--card)] border border-[var(--card-border)] rounded-md px-2 py-1.5 text-xs text-gray-300 placeholder-gray-600"
              />
              <button
                onClick={handleAdd}
                disabled={!newMapping.entity_id || !newMapping.name}
                className="px-3 py-1.5 rounded-md bg-brand-600 hover:bg-brand-700 text-white text-xs font-medium disabled:opacity-50"
              >
                Save
              </button>
            </div>
            <input
              type="text"
              placeholder="Parent ID (optional — e.g. campaign ID for an adset)"
              value={newMapping.parent_id}
              onChange={(e) => setNewMapping({ ...newMapping, parent_id: e.target.value })}
              className="mt-2 w-full bg-[var(--card)] border border-[var(--card-border)] rounded-md px-2 py-1.5 text-xs text-gray-300 placeholder-gray-600"
            />
          </div>
        )}

        {/* Filters */}
        <div className="flex gap-2">
          <select
            value={filterPlatform}
            onChange={(e) => setFilterPlatform(e.target.value)}
            className="bg-[var(--card)] border border-[var(--card-border)] rounded-md px-2 py-1 text-xs text-gray-400"
          >
            <option value="">All Platforms</option>
            {PLATFORMS.map((p) => <option key={p} value={p}>{p}</option>)}
          </select>
          <select
            value={filterType}
            onChange={(e) => setFilterType(e.target.value)}
            className="bg-[var(--card)] border border-[var(--card-border)] rounded-md px-2 py-1 text-xs text-gray-400"
          >
            <option value="">All Types</option>
            {ENTITY_TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
          </select>
          <button
            onClick={load}
            className="flex items-center gap-1 px-2 py-1 text-xs text-gray-400 hover:text-white"
          >
            <RefreshCw size={11} className={loading ? "animate-spin" : ""} /> Refresh
          </button>
          <span className="ml-auto text-[11px] text-gray-600">{rows.length} mappings</span>
        </div>
      </div>

      {/* Table */}
      <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-[var(--card-border)]">
                <th className="text-left px-4 py-2 text-gray-500 font-medium">Platform</th>
                <th className="text-left px-3 py-2 text-gray-500 font-medium">Type</th>
                <th className="text-left px-3 py-2 text-gray-500 font-medium">ID</th>
                <th className="text-left px-3 py-2 text-gray-500 font-medium min-w-[200px]">Name</th>
                <th className="text-left px-3 py-2 text-gray-500 font-medium">Parent ID</th>
                <th className="text-left px-3 py-2 text-gray-500 font-medium">Source</th>
                <th className="text-right px-3 py-2 text-gray-500 font-medium">Actions</th>
              </tr>
            </thead>
            <tbody>
              {rows.length === 0 && (
                <tr>
                  <td colSpan={7} className="px-4 py-8 text-center text-gray-600">
                    No name mappings yet. Click &quot;Add&quot; to create one manually, or &quot;Sync from APIs&quot; to fetch from your ad platforms.
                  </td>
                </tr>
              )}
              {rows.map((row) => {
                const key = rowKey(row);
                const isEditing = editingKey === key;
                return (
                  <tr key={key} className="border-b border-[var(--card-border)] hover:bg-white/[0.02]">
                    <td className="px-4 py-2">
                      <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${platformBadge(row.platform)}`}>
                        {row.platform}
                      </span>
                    </td>
                    <td className="px-3 py-2">
                      <span className={`px-1.5 py-0.5 rounded text-[10px] font-medium ${typeBadge(row.entity_type)}`}>
                        {row.entity_type}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-gray-400 font-mono text-[11px]">{row.entity_id}</td>
                    <td className="px-3 py-2">
                      {isEditing ? (
                        <div className="flex items-center gap-1">
                          <input
                            type="text"
                            value={editName}
                            onChange={(e) => setEditName(e.target.value)}
                            className="bg-[var(--card)] border border-brand-500 rounded px-2 py-0.5 text-xs text-white w-full"
                            autoFocus
                            onKeyDown={(e) => { if (e.key === "Enter") saveEdit(row); if (e.key === "Escape") setEditingKey(null); }}
                          />
                          <button onClick={() => saveEdit(row)} className="text-emerald-400 hover:text-emerald-300"><Check size={12} /></button>
                          <button onClick={() => setEditingKey(null)} className="text-gray-500 hover:text-gray-300"><X size={12} /></button>
                        </div>
                      ) : (
                        <span className="text-gray-200">{row.name}</span>
                      )}
                    </td>
                    <td className="px-3 py-2 text-gray-500 font-mono text-[11px]">{row.parent_id || "—"}</td>
                    <td className="px-3 py-2">
                      <span className={`text-[10px] ${row.source === "api" ? "text-emerald-500" : "text-gray-500"}`}>
                        {row.source}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-right">
                      <div className="flex items-center justify-end gap-1">
                        <button onClick={() => startEdit(row)} className="p-1 text-gray-500 hover:text-brand-400"><Edit2 size={11} /></button>
                        <button onClick={() => handleDelete(row)} className="p-1 text-gray-500 hover:text-red-400"><Trash2 size={11} /></button>
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
