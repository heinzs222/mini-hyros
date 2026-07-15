import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const dashboardRoot = path.resolve(scriptDir, "..");

function replaceExact(text, oldText, newText, label) {
  if (text.includes(newText)) return text;
  if (!text.includes(oldText)) {
    throw new Error(`Cannot apply ${label}: expected source text was not found.`);
  }
  return text.replace(oldText, newText);
}

function replacePattern(text, pattern, replacement, doneNeedle, label) {
  if (text.includes(doneNeedle)) return text;
  if (!pattern.test(text)) {
    throw new Error(`Cannot apply ${label}: expected source pattern was not found.`);
  }
  return text.replace(pattern, replacement);
}

function patchFile(relativePath, patch) {
  const filePath = path.join(dashboardRoot, relativePath);
  const original = fs.readFileSync(filePath, "utf8");
  const updated = patch(original);
  if (updated !== original) {
    fs.writeFileSync(filePath, updated, "utf8");
    console.log(`Patched ${relativePath}`);
  } else {
    console.log(`Already patched ${relativePath}`);
  }
}

patchFile("src/app/page.tsx", (input) => {
  let text = input;

  text = replaceExact(
    text,
    'import { fetchReport, createWebSocket, fetchAuthMe, logout as logoutApi, syncSpend, syncAdNames, syncStripe, syncGhl, type ManagedWebSocket } from "@/lib/api";',
    'import { fetchReport, createWebSocket, fetchAuthMe, logout as logoutApi, syncSpend, syncStripe, syncGhl, type ManagedWebSocket } from "@/lib/api";',
    "dashboard API import",
  );

  text = replaceExact(
    text,
    'toastRef.current.loading("Syncing all platforms…", { description: "Pulling ad spend, ad names and Stripe orders." })',
    'toastRef.current.loading("Syncing report data…", { description: "Pulling ad spend, Stripe orders and GHL attribution." })',
    "sync loading message",
  );

  text = replacePattern(
    text,
    /      const \[spendResult, namesResult, stripeResult, ghlResult\] = await Promise\.allSettled\(\[\r?\n[\s\S]*?\r?\n      \]\);/,
    `      const [spendResult, stripeResult, ghlResult] = await Promise.allSettled([
        // Spend can legitimately take longer than the old 60-second browser deadline.
        // The API's own timeout still caps the request.
        syncSpend({ platform: "all", start_date: syncStart, end_date: syncEnd }),
        withSyncDeadline("Stripe sync", 180_000, (signal) => syncStripe({ start_date: syncStart, end_date: syncEnd }, signal)),
        withSyncDeadline("GHL attribution sync", 120_000, (signal) => syncGhl({
          start_date: syncStart,
          end_date: syncEnd,
          include_forms: false,
          include_opportunities: false,
        }, signal)),
      ]);`,
    "const [spendResult, stripeResult, ghlResult]",
    "blocking sync operations",
  );

  text = replaceExact(text, '      addSyncErrors("Names", namesResult);\n', "", "ad-name error removal");
  text = replaceExact(
    text,
    '      addSyncErrors("Leads", ghlResult);',
    '      addSyncErrors("GHL attribution", ghlResult);',
    "GHL sync label",
  );
  text = replaceExact(
    text,
    'description: "Ad spend, ad names and Stripe orders are up to date.",',
    'description: "Ad spend, Stripe orders and GHL attribution are up to date.",',
    "sync success message",
  );

  text = replacePattern(
    text,
    /  const handleManualSync = useCallback\(async \(\) => \{\r?\n[\s\S]*?\r?\n  \}, \[loadReport, syncSpendData, windowEnd, windowStart\]\);/,
    `  const handleManualSync = useCallback(async () => {
    await syncSpendData({ start_date: windowStart, end_date: windowEnd }, { notify: true });
    // One force-fresh report build after every sync operation has settled.
    await loadReport({ fresh: true });
  }, [loadReport, syncSpendData, windowEnd, windowStart]);`,
    "One force-fresh report build after every sync operation has settled.",
    "manual sync refresh order",
  );

  return text;
});

patchFile("src/lib/api.ts", (input) =>
  replaceExact(
    input,
    `export async function syncGhl(params: {
  start_date?: string;
  end_date?: string;
} = {}, signal?: AbortSignal) {
  const sp = new URLSearchParams();
  if (params.start_date) sp.set("start_date", params.start_date);
  if (params.end_date) sp.set("end_date", params.end_date);`,
    `export async function syncGhl(params: {
  start_date?: string;
  end_date?: string;
  include_forms?: boolean;
  include_opportunities?: boolean;
} = {}, signal?: AbortSignal) {
  const sp = new URLSearchParams();
  if (params.start_date) sp.set("start_date", params.start_date);
  if (params.end_date) sp.set("end_date", params.end_date);
  if (typeof params.include_forms === "boolean") sp.set("include_forms", String(params.include_forms));
  if (typeof params.include_opportunities === "boolean") sp.set("include_opportunities", String(params.include_opportunities));`,
    "lightweight GHL query parameters",
  ),
);

patchFile("src/components/ReportsView.tsx", (input) => {
  let text = replaceExact(
    input,
    "                const unattrRev = Number(s.unattributed_revenue ?? 0);",
    "                const unattrRev = Number(s.unattributed_revenue ?? 0);\n                const campaignAttributed = Number(report?.table?.totals_row?.orders ?? 0);",
    "campaign coverage calculation",
  );

  text = replaceExact(
    text,
    `                      <span className="font-semibold">
                        Reporting gap — {pct}% of orders matched to a source.
                      </span>{" "}
                      {unattrOrders} of {total} order{total === 1 ? "" : "s"}
                      {unattrRev > 0 ? \` ($\{money(unattrRev)\})\` : ""} in this range aren’t
                      attributed to any ad source. That usually means the tracking pixel didn’t
                      capture a click for those sales, or a platform’s spend/clicks haven’t synced
                      yet — press <span className="font-semibold">Sync</span>, and confirm the
                      tracking script is firing on both your funnel and checkout pages.`,
    `                      <span className="font-semibold">
                        Source coverage — {pct}% of orders matched to a source touchpoint.
                      </span>{" "}
                      {unattrOrders} of {total} order{total === 1 ? "" : "s"}
                      {unattrRev > 0 ? \` ($\{money(unattrRev)\})\` : ""} in this range have no
                      qualifying source touchpoint inside the attribution window. This is not
                      automatically a sync failure: direct, recurring, offline, and identity-unmatched
                      sales can remain unattributed.
                      {activeTab === "campaign" && Number.isFinite(campaignAttributed) && (
                        <>
                          {" "}Campaign coverage in this view: <span className="font-semibold">
                            {campaignAttributed.toLocaleString(undefined, { maximumFractionDigits: 2 })} of {total} orders
                          </span>. A source touchpoint can exist without a campaign ID.
                        </>
                      )}`,
    "source coverage explanation",
  );

  return text;
});
