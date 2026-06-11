"use client";

import { importSpendCsv, syncSpend } from "@/lib/api";
import { AlertTriangle, CheckCircle2, Database, RefreshCw, Upload } from "lucide-react";
import { useState } from "react";

interface Props {
  startDate: string;
  endDate: string;
  onImported?: () => Promise<void> | void;
}

const PLATFORMS = [
  { value: "google", label: "Google" },
  { value: "meta", label: "Meta" },
  { value: "tiktok", label: "TikTok" },
];

function formatResult(result: any): string {
  if (!result) return "";
  if (result.errors?.length) return result.errors.join(" | ");
  if (result.inserted != null) {
    return `Imported ${result.inserted} rows, replaced ${result.deleted || 0}, skipped ${result.skipped || 0}.`;
  }
  if (result.synced != null) {
    return `Synced ${result.synced} rows for ${result.date_range?.start || ""} to ${result.date_range?.end || ""}.`;
  }
  return "Done.";
}

export default function SpendImportPanel({ startDate, endDate, onImported }: Props) {
  const [platform, setPlatform] = useState("google");
  const [accountId, setAccountId] = useState("");
  const [replaceExisting, setReplaceExisting] = useState(true);
  const [csvText, setCsvText] = useState("");
  const [busy, setBusy] = useState<"import" | "sync" | "">("");
  const [result, setResult] = useState<any>(null);
  const [error, setError] = useState("");

  const handleFile = async (file?: File | null) => {
    if (!file) return;
    setCsvText(await file.text());
    setResult(null);
    setError("");
  };

  const handleImport = async () => {
    if (!csvText.trim()) {
      setError("Paste or upload a CSV export first.");
      return;
    }

    setBusy("import");
    setResult(null);
    setError("");
    try {
      const imported = await importSpendCsv({
        platform,
        account_id: accountId.trim(),
        csv_text: csvText,
        replace: replaceExisting,
      });
      setResult(imported);
      await onImported?.();
    } catch (err: any) {
      setError(err?.message || "Spend import failed.");
    } finally {
      setBusy("");
    }
  };

  const handleSync = async () => {
    setBusy("sync");
    setResult(null);
    setError("");
    try {
      const synced = await syncSpend({ platform, start_date: startDate, end_date: endDate });
      setResult(synced);
      if (synced?.errors?.length) setError(synced.errors.join(" | "));
      await onImported?.();
    } catch (err: any) {
      setError(err?.message || "Spend sync failed.");
    } finally {
      setBusy("");
    }
  };

  const sample = "Day,Campaign ID,Campaign,Clicks,Impr.,Cost\n2026-06-11,22687198727,Campaign name,25,1000,123.45";

  return (
    <div className="space-y-4">
      <div className="rounded-xl border border-[var(--card-border)] bg-[var(--card)] p-4">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <div className="flex items-center gap-2">
              <Database size={15} className="text-brand-400" />
              <h2 className="text-sm font-semibold text-white">Ad Spend</h2>
            </div>
            <p className="mt-1 max-w-2xl text-[11px] text-gray-500">
              Import platform spend exports into the warehouse. The attribution table will use these rows for cost,
              CPC, CPM, CPA, ROAS, margin, and profit.
            </p>
          </div>
          <div className="rounded-lg border border-[var(--card-border)] bg-white/[0.02] px-3 py-2 text-[11px] text-gray-400">
            Active report window: <span className="text-gray-200">{startDate}</span> to <span className="text-gray-200">{endDate}</span>
          </div>
        </div>

        <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-4">
          <label className="block">
            <span className="mb-1 block text-[10px] uppercase tracking-wide text-gray-500">Platform</span>
            <select
              value={platform}
              onChange={(event) => setPlatform(event.target.value)}
              className="w-full rounded-lg border border-[var(--card-border)] bg-[var(--card)] px-2.5 py-2 text-xs text-gray-300 focus:border-brand-500 focus:outline-none"
            >
              {PLATFORMS.map((p) => (
                <option key={p.value} value={p.value}>{p.label}</option>
              ))}
            </select>
          </label>

          <label className="block md:col-span-2">
            <span className="mb-1 block text-[10px] uppercase tracking-wide text-gray-500">Account / customer ID</span>
            <input
              type="text"
              value={accountId}
              onChange={(event) => setAccountId(event.target.value)}
              placeholder="Optional, e.g. 5655721748"
              className="w-full rounded-lg border border-[var(--card-border)] bg-[var(--card)] px-2.5 py-2 text-xs text-gray-300 placeholder-gray-600 focus:border-brand-500 focus:outline-none"
            />
          </label>

          <div className="flex items-end">
            <button
              type="button"
              onClick={handleSync}
              disabled={!!busy}
              className="flex h-[34px] w-full items-center justify-center gap-1.5 rounded-lg bg-white/5 px-3 py-2 text-xs font-semibold text-gray-200 transition-colors hover:bg-white/10 disabled:opacity-50"
            >
              <RefreshCw size={12} className={busy === "sync" ? "animate-spin" : ""} />
              Sync APIs
            </button>
          </div>
        </div>

        <div className="mt-4 rounded-lg border border-dashed border-[var(--card-border)] bg-white/[0.015] p-3">
          <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
            <div>
              <div className="text-xs font-medium text-gray-300">CSV import fallback</div>
              <div className="mt-0.5 text-[11px] text-gray-500">
                Accepts headers like Day, Campaign ID, Campaign, Clicks, Impr., Cost.
              </div>
            </div>
            <label className="inline-flex cursor-pointer items-center justify-center gap-1.5 rounded-lg bg-white/5 px-3 py-2 text-xs font-medium text-gray-300 transition-colors hover:bg-white/10">
              <Upload size={12} />
              Upload CSV
              <input
                type="file"
                accept=".csv,text/csv"
                className="hidden"
                onChange={(event) => void handleFile(event.target.files?.[0])}
              />
            </label>
          </div>

          <textarea
            value={csvText}
            onChange={(event) => setCsvText(event.target.value)}
            placeholder={sample}
            className="mt-3 min-h-[180px] w-full resize-y rounded-lg border border-[var(--card-border)] bg-[#09090d] p-3 font-mono text-[11px] leading-5 text-gray-300 placeholder-gray-700 focus:border-brand-500 focus:outline-none"
            spellCheck={false}
          />

          <div className="mt-3 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <label className="flex items-center gap-2 text-[11px] text-gray-400">
              <input
                type="checkbox"
                checked={replaceExisting}
                onChange={(event) => setReplaceExisting(event.target.checked)}
                className="h-4 w-4 rounded border-[var(--card-border)] bg-[var(--card)]"
              />
              Replace existing rows for the imported platform/date range
            </label>

            <button
              type="button"
              onClick={handleImport}
              disabled={!!busy}
              className="flex h-[34px] items-center justify-center gap-1.5 rounded-lg bg-brand-600 px-4 py-2 text-xs font-semibold text-white transition-colors hover:bg-brand-700 disabled:opacity-50"
            >
              <Upload size={12} />
              {busy === "import" ? "Importing..." : "Import CSV"}
            </button>
          </div>
        </div>
      </div>

      {(result || error) && (
        <div className={`rounded-xl border p-4 text-xs ${
          error
            ? "border-yellow-500/30 bg-yellow-500/10 text-yellow-300"
            : "border-emerald-500/30 bg-emerald-500/10 text-emerald-300"
        }`}>
          <div className="flex items-start gap-2">
            {error ? <AlertTriangle size={14} /> : <CheckCircle2 size={14} />}
            <div>
              <div className="font-semibold">{error ? "Needs attention" : "Spend updated"}</div>
              <div className="mt-1 text-[11px]">{error || formatResult(result)}</div>
              {result?.warnings?.length > 0 && (
                <div className="mt-2 space-y-1 text-yellow-300">
                  {result.warnings.map((warning: string, index: number) => (
                    <div key={index}>{warning}</div>
                  ))}
                </div>
              )}
              {result?.platforms && (
                <div className="mt-2 grid gap-1 text-[11px] text-gray-300">
                  {Object.entries(result.platforms).map(([name, value]: [string, any]) => (
                    <div key={name}>
                      {name}: {value?.synced || 0} synced
                      {value?.error ? ` - ${value.error}` : ""}
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
