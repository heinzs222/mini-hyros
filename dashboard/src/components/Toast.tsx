"use client";

import { createContext, useCallback, useContext, useEffect, useRef, useState } from "react";
import { CheckCircle2, AlertTriangle, Info, Loader2, X } from "lucide-react";

type ToastType = "success" | "error" | "info" | "loading";

interface ToastItem {
  id: number;
  type: ToastType;
  title: string;
  description?: string;
  duration: number; // ms; 0 = sticky
}

interface ToastOptions {
  description?: string;
  duration?: number;
}

interface ToastApi {
  show: (type: ToastType, title: string, opts?: ToastOptions) => number;
  success: (title: string, opts?: ToastOptions) => number;
  error: (title: string, opts?: ToastOptions) => number;
  info: (title: string, opts?: ToastOptions) => number;
  loading: (title: string, opts?: ToastOptions) => number;
  update: (id: number, patch: Partial<Omit<ToastItem, "id">>) => void;
  dismiss: (id: number) => void;
}

const ToastContext = createContext<ToastApi | null>(null);

const DEFAULT_DURATION: Record<ToastType, number> = {
  success: 4500,
  info: 5000,
  error: 9000,
  loading: 0,
};

const TYPE_META: Record<ToastType, { icon: React.ReactNode; ring: string; accent: string }> = {
  success: { icon: <CheckCircle2 size={16} className="text-emerald-400" />, ring: "border-emerald-500/30", accent: "bg-emerald-500" },
  error: { icon: <AlertTriangle size={16} className="text-rose-400" />, ring: "border-rose-500/30", accent: "bg-rose-500" },
  info: { icon: <Info size={16} className="text-blue-400" />, ring: "border-blue-500/30", accent: "bg-blue-500" },
  loading: { icon: <Loader2 size={16} className="animate-spin text-brand-400" />, ring: "border-brand-500/30", accent: "bg-brand-500" },
};

export function ToastProvider({ children }: { children: React.ReactNode }) {
  const [toasts, setToasts] = useState<ToastItem[]>([]);
  const idRef = useRef(0);

  const dismiss = useCallback((id: number) => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
  }, []);

  const show = useCallback((type: ToastType, title: string, opts?: ToastOptions) => {
    const id = ++idRef.current;
    const duration = opts?.duration ?? DEFAULT_DURATION[type];
    setToasts((prev) => [...prev, { id, type, title, description: opts?.description, duration }].slice(-5));
    return id;
  }, []);

  const update = useCallback((id: number, patch: Partial<Omit<ToastItem, "id">>) => {
    setToasts((prev) => prev.map((t) => (t.id === id ? { ...t, ...patch } : t)));
  }, []);

  const api: ToastApi = {
    show,
    success: (title, opts) => show("success", title, opts),
    error: (title, opts) => show("error", title, opts),
    info: (title, opts) => show("info", title, opts),
    loading: (title, opts) => show("loading", title, opts),
    update,
    dismiss,
  };

  return (
    <ToastContext.Provider value={api}>
      {children}
      <Toaster toasts={toasts} onDismiss={dismiss} />
    </ToastContext.Provider>
  );
}

export function useToast(): ToastApi {
  const ctx = useContext(ToastContext);
  if (!ctx) {
    // No-op fallback so components never crash if used outside a provider.
    const noop = () => 0;
    return { show: noop, success: noop, error: noop, info: noop, loading: noop, update: () => {}, dismiss: () => {} };
  }
  return ctx;
}

function Toaster({ toasts, onDismiss }: { toasts: ToastItem[]; onDismiss: (id: number) => void }) {
  return (
    <div className="pointer-events-none fixed bottom-4 right-4 z-[100] flex w-[360px] max-w-[calc(100vw-2rem)] flex-col gap-2">
      {toasts.map((t) => (
        <ToastCard key={t.id} toast={t} onDismiss={onDismiss} />
      ))}
    </div>
  );
}

function ToastCard({ toast, onDismiss }: { toast: ToastItem; onDismiss: (id: number) => void }) {
  const meta = TYPE_META[toast.type];

  useEffect(() => {
    if (!toast.duration) return;
    const timer = setTimeout(() => onDismiss(toast.id), toast.duration);
    return () => clearTimeout(timer);
  }, [toast.id, toast.duration, toast.type, onDismiss]);

  return (
    <div
      role="status"
      className={`animate-hpop pointer-events-auto relative flex gap-3 overflow-hidden rounded-xl border ${meta.ring} bg-[#0c0c11] p-3 pl-4 shadow-2xl`}
    >
      <span className={`absolute left-0 top-0 h-full w-[3px] ${meta.accent}`} />
      <span className="mt-0.5 shrink-0">{meta.icon}</span>
      <div className="min-w-0 flex-1">
        <div className="text-[13px] font-semibold text-ink-bright">{toast.title}</div>
        {toast.description && (
          <div className="mt-0.5 whitespace-pre-line break-words text-[12px] leading-snug text-ink-dim">
            {toast.description}
          </div>
        )}
      </div>
      <button
        onClick={() => onDismiss(toast.id)}
        title="Dismiss"
        className="mt-0.5 shrink-0 text-ink-faint transition-colors hover:text-ink"
      >
        <X size={14} />
      </button>
    </div>
  );
}
