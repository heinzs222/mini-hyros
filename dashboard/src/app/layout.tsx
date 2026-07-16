import type { Metadata } from "next";
import "./globals.css";
import { ToastProvider } from "@/components/Toast";
import TopProgressBar from "@/components/TopProgressBar";
import ReportTimezoneSync from "@/components/ReportTimezoneSync";

export const metadata: Metadata = {
  title: "VIGIL – Attribution Dashboard",
  description: "Self-hosted ad attribution tracking for Meta, Google & TikTok",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="antialiased">
        <TopProgressBar />
        <ReportTimezoneSync />
        <ToastProvider>{children}</ToastProvider>
      </body>
    </html>
  );
}
