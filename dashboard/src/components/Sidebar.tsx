"use client";

import { LayoutGrid, FileBarChart2, Users, Plug, LogOut, ChevronRight, PanelLeftClose, PanelLeftOpen } from "lucide-react";

export type Section = "dashboard" | "reports" | "leads" | "settings";

interface NavItem {
  key: Section;
  label: string;
  icon: React.ReactNode;
}

const NAV: NavItem[] = [
  { key: "dashboard", label: "Dashboard", icon: <LayoutGrid size={18} /> },
  { key: "reports", label: "Reports", icon: <FileBarChart2 size={18} /> },
  { key: "leads", label: "Leads", icon: <Users size={18} /> },
  { key: "settings", label: "Settings", icon: <Plug size={18} /> },
];

/** Hyros candlestick-style logo mark. */
function LogoMark() {
  return (
    <svg width="26" height="26" viewBox="0 0 26 26" fill="none" aria-hidden>
      <rect x="2" y="9" width="3" height="11" rx="1" fill="#e9eaf0" />
      <rect x="2" y="6" width="3" height="3" rx="1" fill="#8b5cf6" />
      <rect x="7.5" y="5" width="3" height="15" rx="1" fill="#e9eaf0" />
      <rect x="7.5" y="14" width="3" height="3" rx="1" fill="#8b5cf6" />
      <rect x="13" y="11" width="3" height="9" rx="1" fill="#e9eaf0" />
      <rect x="13" y="8" width="3" height="3" rx="1" fill="#8b5cf6" />
      <rect x="18.5" y="3" width="3" height="17" rx="1" fill="#e9eaf0" />
      <rect x="18.5" y="16" width="3" height="3" rx="1" fill="#8b5cf6" />
    </svg>
  );
}

interface Props {
  section: Section;
  onSectionChange: (s: Section) => void;
  collapsed: boolean;
  onToggleCollapse: () => void;
  userName?: string;
  authEnabled?: boolean;
  onLogout?: () => void;
}

export default function Sidebar({
  section,
  onSectionChange,
  collapsed,
  onToggleCollapse,
  userName,
  authEnabled,
  onLogout,
}: Props) {
  const initials = (userName || "Account")
    .split(/[\s@._-]+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((p) => p[0]?.toUpperCase())
    .join("") || "MH";

  return (
    <aside
      className={`relative flex h-screen shrink-0 flex-col border-r border-[var(--card-border)] bg-[#0a0a0e] transition-[width] duration-200 ${
        collapsed ? "w-[68px]" : "w-[232px]"
      }`}
    >
      {/* Logo */}
      <div className={`flex items-center gap-2.5 px-4 py-5 ${collapsed ? "justify-center px-0" : ""}`}>
        <LogoMark />
        {!collapsed && (
          <span className="text-[17px] font-bold tracking-tight text-ink-bright">
            MINI HYROS
          </span>
        )}
      </div>

      {/* Nav */}
      <nav className="mt-2 flex-1 space-y-1 px-3">
        {NAV.map((item) => {
          const active = section === item.key;
          return (
            <button
              key={item.key}
              onClick={() => onSectionChange(item.key)}
              title={collapsed ? item.label : undefined}
              className={`flex w-full items-center gap-3 rounded-lg px-3 py-2.5 text-[14px] transition-colors ${
                collapsed ? "justify-center px-0" : ""
              } ${
                active
                  ? "border border-white/10 bg-white/[0.05] font-medium text-ink-bright"
                  : "border border-transparent text-ink-dim hover:bg-white/[0.03] hover:text-ink"
              }`}
            >
              <span className={active ? "text-brand-400" : ""}>{item.icon}</span>
              {!collapsed && <span>{item.label}</span>}
            </button>
          );
        })}
      </nav>

      {/* Collapse toggle — sits on the divider edge */}
      <button
        onClick={onToggleCollapse}
        className="absolute -right-3 top-[88px] z-10 flex h-6 w-6 items-center justify-center rounded-md border border-[var(--card-border)] bg-[var(--surface-2)] text-ink-dim hover:text-ink"
        title={collapsed ? "Expand" : "Collapse"}
      >
        {collapsed ? <PanelLeftOpen size={14} /> : <PanelLeftClose size={14} />}
      </button>

      {/* Account card */}
      <div className="border-t border-[var(--card-border)] p-3">
        <div
          className={`flex items-center gap-3 rounded-xl border border-[var(--card-border)] bg-[var(--surface)] p-2.5 ${
            collapsed ? "justify-center" : ""
          }`}
        >
          <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-gradient-to-br from-brand-500 via-fuchsia-500 to-amber-400 text-[12px] font-bold text-white">
            {initials}
          </div>
          {!collapsed && (
            <div className="min-w-0 flex-1">
              <div className="truncate text-[13px] font-medium text-ink-bright">
                {userName || "Account"}
              </div>
              <div className="flex items-center gap-1 text-[11px] text-ink-dim">
                Account <ChevronRight size={11} />
              </div>
            </div>
          )}
          {!collapsed && authEnabled && (
            <button
              onClick={onLogout}
              title="Log out"
              className="rounded-md p-1.5 text-ink-dim hover:bg-white/10 hover:text-ink"
            >
              <LogOut size={14} />
            </button>
          )}
        </div>
      </div>
    </aside>
  );
}
