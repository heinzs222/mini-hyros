import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { syncGhl } from "@/lib/api";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

function makeResponse(body: unknown) {
  return {
    ok: true,
    status: 200,
    json: async () => body,
  } as unknown as Response;
}

describe("syncGhl", () => {
  beforeEach(() => {
    window.localStorage.clear();
    vi.restoreAllMocks();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("can request contact attribution without forms or opportunities", async () => {
    const fetchMock = vi.fn(async () => makeResponse({ synced: 1 }));
    vi.stubGlobal("fetch", fetchMock);

    await syncGhl({
      start_date: "2026-07-04",
      end_date: "2026-07-11",
      include_forms: false,
      include_opportunities: false,
    });

    const [url, init] = fetchMock.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe(
      `${API_BASE}/api/ghl/sync?start_date=2026-07-04&end_date=2026-07-11&include_forms=false&include_opportunities=false`,
    );
    expect(init.method).toBe("POST");
  });
});
