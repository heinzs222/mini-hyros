/** @type {import('next').NextConfig} */
const nextConfig = {
  // Only proxy in local dev (when no NEXT_PUBLIC_API_URL is set, frontend
  // calls localhost:3000/api/* which gets rewritten to the backend).
  // In production on Vercel, NEXT_PUBLIC_API_URL points directly at the
  // Render backend so no rewrites are needed.
  async rewrites() {
    if (process.env.NEXT_PUBLIC_API_URL) return [];
    return [
      {
        source: "/api/:path*",
        destination: "http://localhost:8000/api/:path*",
      },
      {
        source: "/ws",
        destination: "http://localhost:8000/ws",
      },
    ];
  },
};

module.exports = nextConfig;
