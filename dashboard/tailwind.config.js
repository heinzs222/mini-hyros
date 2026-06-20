/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./src/**/*.{js,ts,jsx,tsx,mdx}"],
  theme: {
    extend: {
      fontFamily: {
        sans: ["Inter", "system-ui", "-apple-system", "Segoe UI", "sans-serif"],
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
          DEFAULT: "#ced0dd",
          dim: "#80838f",
          faint: "#595c68",
          bright: "#e9eaf0",
        },
        surface: {
          DEFAULT: "#0e0e13",
          2: "#15151c",
          3: "#1b1b23",
        },
      },
    },
  },
  plugins: [],
};
