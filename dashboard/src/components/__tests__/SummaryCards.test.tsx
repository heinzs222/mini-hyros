import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import SummaryCards from "@/components/SummaryCards";

type Totals = Parameters<typeof SummaryCards>[0]["totals"];

function makeTotals(overrides: Partial<Totals> = {}): Totals {
  return {
    clicks: 1_000,
    impressions: 50_000,
    orders: 80,
    cost: 5_000,
    cpc: 5,
    cpm: 100,
    ctr: 2,
    cpa: 62.5,
    cvr: 8,
    revenue: 20_000,
    total_revenue: 20_000,
    aov: 250,
    rpc: 20,
    profit: 15_000,
    margin_pct: 75,
    net_profit: 14_000,
    roas: 4,
    mer: 4,
    cac: 62.5,
    reported: 19_000,
    reported_delta: 1_000,
    tracked_orders: 100,
    tracked_revenue: 25_000,
    attributed_orders: 80,
    attributed_revenue: 20_000,
    unattributed_orders: 20,
    unattributed_revenue: 5_000,
    attribution_rate: 80,
    ...overrides,
  };
}

describe("SummaryCards", () => {
  it("renders the headline card labels", () => {
    render(<SummaryCards totals={makeTotals()} />);
    expect(screen.getByText("Ad Spend")).toBeInTheDocument();
    expect(screen.getByText("Tracked Revenue")).toBeInTheDocument();
    expect(screen.getByText("Attr. Revenue")).toBeInTheDocument();
    expect(screen.getByText("Clicks")).toBeInTheDocument();
    expect(screen.getByText("Orders")).toBeInTheDocument();
    expect(screen.getByText("Attribution Rate")).toBeInTheDocument();
  });

  it("formats money, ratios, and percentages from the totals", () => {
    render(<SummaryCards totals={makeTotals()} />);

    // Ad spend $5,000 -> abbreviated K.
    expect(screen.getByText("$5.0K")).toBeInTheDocument();
    // Tracked revenue $25,000 -> $25.0K.
    expect(screen.getByText("$25.0K")).toBeInTheDocument();
    // Attributed revenue $20,000 -> $20.0K (also used as the ROAS/CVR baseline).
    expect(screen.getAllByText("$20.0K").length).toBeGreaterThan(0);
    // ROAS ratio with the x suffix.
    expect(screen.getAllByText("4.00x").length).toBeGreaterThan(0);
    // Attribution rate percentage.
    expect(screen.getByText("80.0%")).toBeInTheDocument();
  });

  it("uses the attribution-aware labels when attribution data is present", () => {
    render(<SummaryCards totals={makeTotals()} />);
    expect(screen.getByText("Profit (Attr.)")).toBeInTheDocument();
    expect(screen.getByText("ROAS (Attr.)")).toBeInTheDocument();
    expect(screen.getByText("CVR (Attr.)")).toBeInTheDocument();
  });

  it("falls back to tracked labels when there is no attribution", () => {
    const totals = makeTotals({
      attributed_orders: 0,
      attributed_revenue: 0,
      orders: 0,
      revenue: 0,
    });
    render(<SummaryCards totals={totals} />);
    expect(screen.getByText("Profit (Tracked)")).toBeInTheDocument();
    expect(screen.getByText("ROAS (Tracked)")).toBeInTheDocument();
    expect(screen.getByText("CVR (Tracked)")).toBeInTheDocument();
  });

  it("renders zero/placeholder states without crashing", () => {
    const totals = makeTotals({
      clicks: 0,
      impressions: 0,
      orders: 0,
      cost: 0,
      cpc: null,
      cpa: null,
      cvr: null,
      revenue: 0,
      total_revenue: 0,
      aov: null,
      rpc: null,
      profit: 0,
      margin_pct: null,
      net_profit: 0,
      roas: null,
      mer: null,
      cac: null,
      reported: null,
      reported_delta: null,
      tracked_orders: 0,
      tracked_revenue: 0,
      attributed_orders: 0,
      attributed_revenue: 0,
      unattributed_orders: 0,
      unattributed_revenue: 0,
      attribution_rate: null,
    });
    render(<SummaryCards totals={totals} />);

    // Ad Spend of 0 formats to $0.00.
    expect(screen.getAllByText("$0.00").length).toBeGreaterThan(0);
    // attribution_rate null -> em dash placeholder.
    expect(screen.getAllByText("—").length).toBeGreaterThan(0);
    // Still renders all ten card labels.
    expect(screen.getByText("Sync Delta")).toBeInTheDocument();
  });

  it("shows the comparison baseline banner when compareTotals is supplied", () => {
    render(
      <SummaryCards
        totals={makeTotals()}
        compareTotals={makeTotals({ cost: 4_000 })}
        compareLabel="previous period"
      />,
    );
    expect(screen.getByText(/Comparison baseline: previous period/i)).toBeInTheDocument();
  });
});
