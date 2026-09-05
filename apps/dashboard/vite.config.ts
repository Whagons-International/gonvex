import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  // The dashboard consumes @gonvex/react through a workspace symlink. Resolve
  // its peer imports through the dashboard's React installation so hooks and
  // the renderer always share one dispatcher in production bundles.
  resolve: {
    dedupe: ["react", "react-dom"],
  },
  // Keep unit tests deterministic even when a developer's ignored .env.local
  // points the dashboard at a real project.
  define: process.env.VITEST
    ? {
        "import.meta.env.VITE_GONVEX_PROJECT_ID": JSON.stringify("app"),
        "import.meta.env.VITE_GONVEX_PROJECTS": JSON.stringify(""),
        "import.meta.env.VITE_GONVEX_DATABASE": JSON.stringify("gonvex_dev"),
        "import.meta.env.VITE_GONVEX_DASHBOARD_AUTH_PROJECT_ID": JSON.stringify(""),
        "import.meta.env.VITE_GONVEX_GOOGLE_LOGIN_ENABLED": JSON.stringify("false"),
      }
    : undefined,
  server: {
    host: "127.0.0.1",
    port: 5174,
    strictPort: true,
    proxy: {
      "/dev": { target: "http://127.0.0.1:8080", ws: true },
      "/ws": {
        target: "ws://127.0.0.1:8080",
        ws: true,
      },
    },
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: "./src/test/setup.ts",
  },
});
