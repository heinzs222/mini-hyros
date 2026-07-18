/** @type {import('next').NextConfig} */
const nextConfig = {
  experimental: {
    optimizePackageImports: ["lucide-react", "recharts"],
  },
  // Only proxy in local dev (when no NEXT_PUBLIC_API_URL is set, frontend
  // calls localhost:3000/api/* which gets rewritten to the backend).
  // In production on Vercel, NEXT_PUBLIC_API_URL points directly at the
  // deployed backend, so no rewrites are needed. (The live feed now polls
  // /api/live/recent, so there is no /ws WebSocket rewrite.)
  async rewrites() {
    if (process.env.NEXT_PUBLIC_API_URL) return [];
    return [
      {
        source: "/api/:path*",
        destination: "http://localhost:8000/api/:path*",
      },
    ];
  },
};

module.exports = nextConfig;
