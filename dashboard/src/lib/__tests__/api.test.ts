import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import {
  apiFetch,
  setAuthToken,
  clearAuthToken,
  fetchReport,
  fetchChildren,
  syncSpend,
  fetchLtvBySource,
} from "@/lib/api";

const AUTH_TOKEN_KEY = "hyros_auth_token";
// API_BASE defaults to this when NEXT_PUBLIC_API_URL is unset.
const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

/** Build a Response-like object that satisfies the parts api.ts uses. */
function makeResponse(body: unknown, ok = true, status = 200) {
  return {
    ok,
    status,
    json: async () => body,
  } as unknown as Response;
}

/** Grab the (url, init) the last fetch call was invoked with. */
function lastFetchCall() {
  const mock = global.fetch as unknown as ReturnType<typeof vi.fn>;
  return mock.mock.calls[mock.mock.calls.length - 1] as [string, RequestInit];
}

beforeEach(() => {
  window.localStorage.clear();
  vi.restoreAllMocks();
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("auth token storage", () => {
  it("setAuthToken trims and persists the token", () => {
    setAuthToken("  abc123  ");
    expect(window.localStorage.getItem(AUTH_TOKEN_KEY)).toBe("abc123");
  });

  it("setAuthToken removes the token when given an empty/whitespace value", () => {
    window.localStorage.setItem(AUTH_TOKEN_KEY, "existing");
    setAuthToken("   ");
    expect(window.localStorage.getItem(AUTH_TOKEN_KEY)).toBeNull();

    window.localStorage.setItem(AUTH_TOKEN_KEY, "existing");
    setAuthToken("");
    expect(window.localStorage.getItem(AUTH_TOKEN_KEY)).toBeNull();
  });

  it("clearAuthToken removes any stored token", () => {
    window.localStorage.setItem(AUTH_TOKEN_KEY, "tok");
    clearAuthToken();
    expect(window.localStorage.getItem(AUTH_TOKEN_KEY)).toBeNull();
  });
});

describe("apiFetch", () => {
  it("adds an Authorization: Bearer <token> header when a token is stored", async () => {
    window.localStorage.setItem(AUTH_TOKEN_KEY, "secret-token");
    const fetchMock = vi.fn(async () => makeResponse({ ok: true }));
    vi.stubGlobal("fetch", fetchMock);

    await apiFetch("http://localhost:8000/api/thing");

    const [url, init] = lastFetchCall();
    expect(url).toBe("http://localhost:8000/api/thing");
    const headers = new Headers(init.headers);
    expect(headers.get("Authorization")).toBe("Bearer secret-token");
  });

  it("omits the Authorization header when no token is stored", async () => {
    const fetchMock = vi.fn(async () => makeResponse({ ok: true }));
    vi.stubGlobal("fetch", fetchMock);

    await apiFetch("http://localhost:8000/api/thing");

    const [, init] = lastFetchCall();
    const headers = new Headers(init.headers);
    expect(headers.get("Authorization")).toBeNull();
  });

  it("preserves caller-supplied headers and init fields", async () => {
    window.localStorage.setItem(AUTH_TOKEN_KEY, "tok");
    const fetchMock = vi.fn(async () => makeResponse({ ok: true }));
    vi.stubGlobal("fetch", fetchMock);

    await apiFetch("http://localhost:8000/api/thing", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
    });

    const [, init] = lastFetchCall();
    expect(init.method).toBe("POST");
    const headers = new Headers(init.headers);
    expect(headers.get("Content-Type")).toBe("application/json");
    expect(headers.get("Authorization")).toBe("Bearer tok");
  });
});

describe("fetchReport", () => {
  it("builds the report URL with only the provided query params, in order", async () => {
    const fetchMock = vi.fn(async () => makeResponse({ rows: [] }));
    vi.stubGlobal("fetch", fetchMock);

    const data = await fetchReport({
      start_date: "2026-01-01",
      end_date: "2026-01-31",
      model: "last_touch",
      lookback_days: 30,
      active_tab: "campaigns",
      conversion_type: "purchase",
      use_click_date: true,
    });

    expect(data).toEqual({ rows: [] });
    const [url] = lastFetchCall();
    expect(url).toBe(
      `${API_BASE}/api/report?start_date=2026-01-01&end_date=2026-01-31&model=last_touch&lookback_days=30&active_tab=campaigns&conversion_type=purchase&use_click_date=true`,
    );
  });

  it("omits falsy params and never sets use_click_date when false", async () => {
    const fetchMock = vi.fn(async () => makeResponse({ rows: [] }));
    vi.stubGlobal("fetch", fetchMock);

    await fetchReport({ start_date: "2026-01-01", use_click_date: false });

    const [url] = lastFetchCall();
    expect(url).toBe(`${API_BASE}/api/report?start_date=2026-01-01`);
    expect(url).not.toContain("use_click_date");
    expect(url).not.toContain("end_date");
  });

  it("throws when the response is not ok", async () => {
    const fetchMock = vi.fn(async () => makeResponse(null, false, 500));
    vi.stubGlobal("fetch", fetchMock);

    await expect(fetchReport({})).rejects.toThrow("Report fetch failed: 500");
  });
});

describe("fetchChildren", () => {
  it("always sets parent_tab and parent_id, plus optional params", async () => {
    const fetchMock = vi.fn(async () => makeResponse({ children: [] }));
    vi.stubGlobal("fetch", fetchMock);

    await fetchChildren({ parent_tab: "campaigns", parent_id: "abc", model: "first_touch" });

    const [url] = lastFetchCall();
    expect(url).toBe(`${API_BASE}/api/report/children?parent_tab=campaigns&parent_id=abc&model=first_touch`);
  });

  it("throws on a non-ok response", async () => {
    const fetchMock = vi.fn(async () => makeResponse(null, false, 404));
    vi.stubGlobal("fetch", fetchMock);

    await expect(fetchChildren({ parent_tab: "x", parent_id: "y" })).rejects.toThrow(
      "Children fetch failed: 404",
    );
  });
});

describe("syncSpend", () => {
  it("defaults the platform to all and posts", async () => {
    const fetchMock = vi.fn(async () => makeResponse({ synced: true }));
    vi.stubGlobal("fetch", fetchMock);

    await syncSpend();

    const [url, init] = lastFetchCall();
    expect(url).toBe(`${API_BASE}/api/spend/sync?platform=all`);
    expect(init.method).toBe("POST");
  });

  it("includes the date range when provided", async () => {
    const fetchMock = vi.fn(async () => makeResponse({ synced: true }));
    vi.stubGlobal("fetch", fetchMock);

    await syncSpend({ platform: "google", start_date: "2026-01-01", end_date: "2026-01-31" });

    const [url] = lastFetchCall();
    expect(url).toBe(
      `${API_BASE}/api/spend/sync?platform=google&start_date=2026-01-01&end_date=2026-01-31`,
    );
  });
});

describe("fetchLtvBySource", () => {
  it("uses default breakdown and windows query params", async () => {
    const fetchMock = vi.fn(async () => makeResponse({ ltv: [] }));
    vi.stubGlobal("fetch", fetchMock);

    await fetchLtvBySource();

    const [url] = lastFetchCall();
    expect(url).toBe(`${API_BASE}/api/ltv/by-source?breakdown=platform&windows=30,60,90,365`);
  });

  it("throws on a non-ok response", async () => {
    const fetchMock = vi.fn(async () => makeResponse(null, false, 502));
    vi.stubGlobal("fetch", fetchMock);

    await expect(fetchLtvBySource()).rejects.toThrow("LTV fetch failed: 502");
  });
});
