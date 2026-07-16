"use client";

import { useEffect } from "react";
import { fetchHealth } from "@/lib/api";
import { setReportTimeZone } from "@/lib/utils";

/**
 * Aligns the dashboard with the backend's live reporting timezone (exposed via
 * /api/health) so "Today"/preset windows track an ops-side REPORT_TIMEZONE
 * change without a frontend rebuild. Renders nothing; failures are ignored —
 * the build-time NEXT_PUBLIC_REPORT_TIMEZONE default still applies.
 */
export default function ReportTimezoneSync() {
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const body = await fetchHealth();
        if (!cancelled && body?.timezone) {
          setReportTimeZone(String(body.timezone));
        }
      } catch {}
    })();
    return () => {
      cancelled = true;
    };
  }, []);
  return null;
}
