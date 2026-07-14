import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import FunnelSnapshotTable from "@/components/FunnelSnapshotTable";

const rows = [
  {
    funnel_id: "f-1",
    funnel_name: "Webinar Funnel",
    visits: 12_345,
    leads: 2_000,
    purchases: 150,
    lead_rate: 16.2,
    purchase_rate: 7.5,
    revenue: 45_000,
    aov: 300,
  },
  {
    funnel_id: "f-2",
    funnel_name: "Tripwire Funnel",
    visits: 800,
    leads: 120,
    purchases: 9,
    lead_rate: 15,
    purchase_rate: 7.5,
    revenue: 1_800,
    aov: null,
  },
];

describe("FunnelSnapshotTable", () => {
  it("renders the empty state when there are no rows", () => {
    render(<FunnelSnapshotTable rows={[]} />);
    expect(
      screen.getByText(/No funnel traffic yet/i),
    ).toBeInTheDocument();
    // No table is rendered in the empty state.
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });

  it("renders a row per funnel with formatted metrics", () => {
    render(<FunnelSnapshotTable rows={rows} />);

    expect(screen.getByRole("table")).toBeInTheDocument();
    expect(screen.getByText("Webinar Funnel")).toBeInTheDocument();
    expect(screen.getByText("Tripwire Funnel")).toBeInTheDocument();

    // Funnel ids are rendered.
    expect(screen.getByText("f-1")).toBeInTheDocument();
    expect(screen.getByText("f-2")).toBeInTheDocument();

    // Visits use formatNumber's locale separators.
    expect(screen.getByText((12_345).toLocaleString())).toBeInTheDocument();

    // Revenue uses exact grouped currency values in data tables.
    expect(screen.getByText("$45,000.00")).toBeInTheDocument();
    expect(screen.getByText("$1,800.00")).toBeInTheDocument();

    // Percentages use formatPercentValue with 2 digits.
    expect(screen.getByText("16.20%")).toBeInTheDocument();

    // AOV line is shown only when aov is non-null.
    expect(screen.getByText(/AOV \$300\.00/)).toBeInTheDocument();
  });

  it("shows the comparison badge and delta values when compareRows are supplied", () => {
    const compareRows = [
      { ...rows[0], visits: 10_000, purchases: 100, revenue: 40_000 },
    ];
    render(
      <FunnelSnapshotTable rows={rows} compareRows={compareRows} compareLabel="last week" />,
    );

    expect(screen.getByText(/Delta vs last week/i)).toBeInTheDocument();
    // visits delta = 12345 - 10000 = 2345 -> "+2,345"
    expect(screen.getByText(`+${(2_345).toLocaleString()}`)).toBeInTheDocument();
  });
});
