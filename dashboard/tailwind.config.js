/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./src/**/*.{js,ts,jsx,tsx,mdx}"],
  theme: {
    extend: {
      fontFamily: {
        sans: ["Hanken Grotesk", "Inter", "system-ui", "-apple-system", "Segoe UI", "sans-serif"],
        mono: ["JetBrains Mono", "ui-monospace", "SFMono-Regular", "Menlo", "Consolas", "monospace"],
      },
      colors: {
        brand: {
          50: "#f5f3ff",
          100: "#ede9fe",
          200: "#ddd6fe",
          300: "#c4b5fd",
          400: "#a78bfa",
          500: "#8b5cf6",
          600: "#7c3aed",
          700: "#6d28d9",
          900: "#4c1d95",
        },
        ink: {
          DEFAULT: "#b9bcc8",
          dim: "#7e828f",
          faint: "#565a67",
          bright: "#eceef4",
        },
        surface: {
          DEFAULT: "#0e0e13",
          2: "#15151c",
          3: "#1b1b23",
        },
        mint: {
          DEFAULT: "#3ee0a1",
        },
        coral: {
          DEFAULT: "#ff6b7a",
        },
      },
    },
  },
  plugins: [],
};
