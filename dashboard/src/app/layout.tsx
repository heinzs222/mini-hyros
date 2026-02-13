import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Mini Hyros – Attribution Dashboard",
  description: "Self-hosted ad attribution tracking for Meta, Google & TikTok",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className="antialiased">{children}</body>
    </html>
  );
}
