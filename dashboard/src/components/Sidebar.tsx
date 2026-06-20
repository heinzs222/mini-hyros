"use client";

import { useEffect, useRef, useState } from "react";
import { LayoutGrid, FileBarChart2, Users, Plug, LogOut, ChevronRight, PanelLeftClose, PanelLeftOpen, Settings as SettingsIcon } from "lucide-react";

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

/** VIGIL logo mark: a ring with a single dot inside (a watchful eye / orbit). */
export function LogoMark({ size = 26 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 26 26" fill="none" aria-hidden>
      <circle cx="13" cy="13" r="9.5" stroke="#e9eaf0" strokeWidth="1.9" />
      <circle cx="16.7" cy="10.6" r="2.3" fill="#e9eaf0" />
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
    .join("") || "V";

  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!menuOpen) return;
    const onDown = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) setMenuOpen(false);
    };
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [menuOpen]);

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
          <span className="text-[19px] font-bold tracking-[0.22em] text-ink-bright">
            VIGIL
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
      <div className="relative border-t border-[var(--card-border)] p-3" ref={menuRef}>
        {menuOpen && (
          <div className="animate-hpop absolute bottom-[68px] left-3 right-3 z-50 overflow-hidden rounded-xl border border-[var(--card-border)] bg-[#0c0c11] p-1 shadow-2xl">
            {!collapsed && (
              <div className="border-b border-[var(--card-border)] px-3 py-2">
                <div className="truncate text-[13px] font-medium text-ink-bright">{userName || "Account"}</div>
                <div className="text-[11px] text-ink-dim">Signed in</div>
              </div>
            )}
            <button
              onClick={() => { onSectionChange("settings"); setMenuOpen(false); }}
              className="flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-[13px] text-ink hover:bg-white/5"
            >
              <SettingsIcon size={14} className="text-ink-dim" /> Settings
            </button>
            <button
              onClick={() => { setMenuOpen(false); onLogout?.(); }}
              className="flex w-full items-center gap-2 rounded-lg px-3 py-2 text-left text-[13px] text-rose-300 hover:bg-rose-500/10"
            >
              <LogOut size={14} /> Log out
            </button>
          </div>
        )}
        <button
          onClick={() => setMenuOpen((o) => !o)}
          title="Account menu"
          aria-haspopup="menu"
          aria-expanded={menuOpen}
          className={`flex w-full items-center gap-3 rounded-xl border bg-[var(--surface)] p-2.5 text-left transition-colors hover:border-white/15 hover:bg-white/[0.04] ${
            menuOpen ? "border-white/15" : "border-[var(--card-border)]"
          } ${collapsed ? "justify-center" : ""}`}
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
                Account <ChevronRight size={11} className={`transition-transform ${menuOpen ? "rotate-90" : ""}`} />
              </div>
            </div>
          )}
        </button>
      </div>
    </aside>
  );
}
